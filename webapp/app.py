# -*- coding: utf-8 -*-
from fastapi import FastAPI, Request, Query
from fastapi.responses import HTMLResponse, Response
from fastapi.templating import Jinja2Templates
import happybase
import subprocess
import time
import math
import uvicorn
import os

app = FastAPI(title="USTC Search Engine")

# Jinja2模板引擎配置(使用脚本所在目录的绝对路径,避免CWD依赖)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
templates = Jinja2Templates(directory=os.path.join(BASE_DIR, "templates"))

# Configuration
# HBase集群连接参数
HBASE_HOST = '172.20.194.143'  # HBase主机地址
TABLE_NAME = 'ustc_search_engine'  # 元数据表名
# HDFS文件读取命令(通过WSL调用)
HDFS_BIN = "/opt/module/hadoop-3.3.6/bin/hdfs"

def get_db():
    """获取HBase连接(单次请求级别连接)

    返回:
        HBase连接对象或None(连接失败)
    说明:
        - 超时设置5000ms防止长期阻塞
        - 异常时返回None(由调用者检查)
    """
    try:
        conn = happybase.Connection(HBASE_HOST, port=9090, timeout=5000)
        conn.open()
        return conn
    except Exception as e:
        print(f"HBase connection failed: {e}")
        return None

@app.get('/', response_class=HTMLResponse)
async def index(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})

@app.get('/search', response_class=HTMLResponse)
async def search(request: Request, q: str = "", page: int = Query(default=1, ge=1)):
    # 查询入口: 接收搜索词和分页参数
    query = q.strip()  # 搜索关键词(需要去空格)
    per_page = 10  # 每页结果数(固定值)

    # 空查询快速返回(避免无谓扫表)
    if not query:
        return templates.TemplateResponse("index.html", {"request": request})

    conn = get_db()
    if not conn:
        return HTMLResponse("<h3>Database connection failed</h3>", status_code=500)

    table = conn.table(TABLE_NAME)

    # 内存缓存所有候选结果,用于排序和分页
    scored_results = []
    start_time = time.time()  # 记录查询耗时(用于计算返回)

    # 全表扫描+过滤(生产环境应使用倒排索引)
    try:
        for key, data in table.scan():
            # HBase行数据解码: 所有值为字节类型,需转码为字符串
            title = data.get(b'meta:title', b'').decode('utf-8', 'ignore')
            content = data.get(b'data:content', b'').decode('utf-8', 'ignore')
            url = data.get(b'meta:url', b'').decode('utf-8', 'ignore')
            date = data.get(b'meta:date', b'').decode('utf-8', 'ignore')

            # 离线关键词索引(由爬虫Jieba生成,逗号分隔)
            kw_str = data.get(b'index:keywords', b'').decode('utf-8', 'ignore')
            keywords_list = kw_str.split(',') if kw_str else []

            # 三维相关度计分模型
            score = 0

            # 基础过滤: 搜索词都未命中则排除(减少无关结果)
            if query not in title and query not in content:
                continue

            # 维度一: 标题命中(权重100=最重要)
            if query in title:
                score += 100

            # 维度二: 离线关键词命中(权重50=中等重要,反映语义相关性)
            if query in keywords_list:
                score += 50

            # 维度三: 正文词频(权重1,上限20=避免被堆砌词汇影响)
            term_count = content.count(query)
            score += min(term_count, 20)

            # 摘要生成: 优先截取关键词周边文本(上下文各30/100字)
            idx = content.find(query)
            if idx != -1:
                start = max(0, idx - 30)
                end = min(len(content), idx + 100)
                # 关键词高亮(<em>标签)
                snippet = content[start:end].replace(query, f"<em>{query}</em>") + "..."
            else:
                # 关键词未命中时使用文本头部作为摘要
                snippet = content[:120] + "..."

            # 打包结果记录
            scored_results.append({
                'score': score,
                'row_key': key.decode(),
                'title': title,
                'url': url,
                'date': date,
                'snippet': snippet,
                'keywords': keywords_list
            })

            # 限制候选集大小为200(平衡召回与响应速度)
            if len(scored_results) >= 200:
                break

    except Exception as e:
        # 表扫描异常记录日志,返回已扫描结果
        print(f"Search error: {e}")
    finally:
        # 必须关闭连接释放资源
        conn.close()

    # 按相关度分数从高到低排序
    scored_results.sort(key=lambda x: x['score'], reverse=True)
    # 内存分页(结果集较小,直接切片)
    total_results = len(scored_results)  # 实际命中总数
    total_pages = math.ceil(total_results / per_page)  # 向上取整

    # 页码范围检查(防止越界访问)
    if page < 1: page = 1
    if page > total_pages and total_pages > 0: page = total_pages

    # 计算切片位置(0-based索引)
    start_idx = (page - 1) * per_page
    end_idx = start_idx + per_page
    current_page_results = scored_results[start_idx:end_idx]

    # 查询总耗时(毫秒精度)
    cost_time = round(time.time() - start_time, 3)

    return templates.TemplateResponse(
        'result.html',
        {
            "request": request,
            "query": query,
            "results": current_page_results,
            "count": total_results,
            "time": cost_time,
            "page": page,
            "total_pages": total_pages,
        }
    )

@app.get('/download/{row_key:path}')
async def download(row_key: str):
    """从HDFS下载文件

    参数:
        row_key: HBase行键(URL的MD5哈希)
    流程:
        1. 查询HBase获取HDFS路径和元数据
        2. 通过WSL调用HDFS读取文件二进制数据
        3. 流式传输到浏览器(触发下载)
    """
    conn = get_db()
    if not conn:
        return HTMLResponse("Database connection failed", status_code=500)
    try:
        table = conn.table(TABLE_NAME)
        # HBase Get操作: 按行键查询单行数据
        data = table.row(row_key.encode())
    finally:
        conn.close()  # 及时释放连接

    # 元数据缺失返回404(文件可能已被删除)
    if not data:
        return HTMLResponse("文件未找到或元数据丢失", status_code=404)

    # 从HBase读取HDFS存储路径和原始文件名
    hdfs_path = data.get(b'data:hdfs_path', b'').decode()
    file_name = data.get(b'meta:title', b'download.file').decode('utf-8', 'ignore')
    # WSL子进程调用hdfs命令读取文件内容(避免本地临时文件)
    cmd = f'wsl {HDFS_BIN} dfs -cat "{hdfs_path}"'

    try:
        # 启动子进程执行HDFS读取命令
        process = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        # 同步等待进程完成,获取标准输出和标准错误
        file_data, stderr = process.communicate()

        # 进程执行失败或文件为空
        if process.returncode != 0 or len(file_data) == 0:
            return HTMLResponse(f"HDFS read failed: {stderr.decode()}", status_code=500)

        # 将二进制数据流发送给客户端(触发下载)
        from urllib.parse import quote
        encoded_filename = quote(file_name)
        return Response(
            content=file_data,
            media_type="application/octet-stream",
            headers={
                "Content-Disposition": f"attachment; filename*=UTF-8''{encoded_filename}"
            }
        )
    except Exception as e:
        # 系统异常(如权限问题、进程异常)
        return HTMLResponse(f"System error: {e}", status_code=500)

if __name__ == '__main__':
    # Uvicorn ASGI服务器启动(监听0.0.0.0的5000端口)
    print("Search engine starting... http://localhost:5000")
    uvicorn.run(app, host='0.0.0.0', port=5000)
