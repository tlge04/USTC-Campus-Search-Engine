# -*- coding: utf-8 -*-
import logging
from bs4 import BeautifulSoup
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import happybase
import hashlib
import time
import jieba.analyse
from collections import deque
from urllib.parse import urljoin, urlparse
import random
import io
import pdfplumber
from docx import Document
import subprocess
import sys

# Configuration
class Config:
    # 爬虫控制参数
    MAX_PAGES = 10000              # 目标页面数量上限
    DOMAIN_LIMIT = "ustc.edu.cn"   # 域名过滤器
    DELAY_RANGE = (3.0, 6.0)        # 请求间隔随机范围(秒)
    
    # HBase存储连接参数
    HBASE_HOST = '172.20.194.143'
    HBASE_PORT = 9090
    TABLE_NAME = 'ustc_search_engine'
    
    # HDFS文件存储配置
    HDFS_BIN = "/opt/module/hadoop-3.3.6/bin/hdfs"
    HDFS_ROOT = "/search_engine/raw_data"
    
    # 伪装User-Agent(防检测)
    USER_AGENTS = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/115.0"
    ]

# Logger initialization
handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(message)s', datefmt='%H:%M:%S'))
logger = logging.getLogger("USTC_Crawler")
logger.setLevel(logging.INFO)
logger.addHandler(handler)

# Storage manager
# 职责: 管理文件和元数据的持久化(HDFS + HBase)
class StorageManager:
    def __init__(self):
        self._init_hbase()
        
    def _init_hbase(self):
        # 连接HBase并获取表引用
        # 异常时直接退出(避免后续操作失败)
        try:
            self.conn = happybase.Connection(Config.HBASE_HOST, port=Config.HBASE_PORT, timeout=30000)
            self.conn.open()
            self.table = self.conn.table(Config.TABLE_NAME)
            logger.info(f"HBase connected at {Config.HBASE_HOST}")
        except Exception as e:
            logger.critical(f"HBase connection failed: {e}")
            sys.exit(1)

    def save_file_to_hdfs(self, content, ext):
        """保存文件到HDFS
        
        参数:
            content: 文件二进制内容
            ext: 文件扩展名(含.)
        返回:
            HDFS路径或None(上传失败/已存在)
        说明:
            - 文件名使用MD5(content)保证去重
            - 通过WSL+管道方式完成跨系统传输
            - 重复文件直接返回现有路径
        """
        file_hash = hashlib.md5(content).hexdigest()
        filename = f"{file_hash}{ext}"
        hdfs_path = f"{Config.HDFS_ROOT}/{filename}"
        # 通过标准输入管道避免临时文件
        cmd_str = f'wsl {Config.HDFS_BIN} dfs -put -f - "{hdfs_path}"'

        try:
            process = subprocess.Popen(
                cmd_str,
                shell=True,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )
            stdout, stderr = process.communicate(input=content)
            
            if process.returncode == 0:
                return hdfs_path
            
            err_msg = stderr.decode('utf-8', errors='ignore').strip()
            # 文件已存在时仍返回路径(满足去重需求)
            if "File exists" in err_msg:
                return hdfs_path
            return None
        except Exception:
            return None

    def save_metadata(self, url_hash, data_dict):
        # HBase Put操作(行键为URL的MD5哈希)
        # 异常时尝试重连并重试
        try:
            self.table.put(url_hash, data_dict)
        except Exception:
            try:
                self.conn.open()
                self.table.put(url_hash, data_dict)
            except: pass

    def close(self):
        self.conn.close()

# Content parser
# 职责: 文档内容提取和关键词索引生成
class ContentParser:
    @staticmethod
    def parse_text(content, ext):
        # 文档内容抽取
        # PDF: 限制前6页(避免文本过长); DOCX: 逐段提取
        text = ""
        try:
            f = io.BytesIO(content)
            if ext == '.pdf':
                with pdfplumber.open(f) as pdf:
                    for i, page in enumerate(pdf.pages):
                        if i > 5: break
                        t = page.extract_text()
                        if t: text += t + "\n"
            elif ext == '.docx':
                doc = Document(f)
                for p in doc.paragraphs:
                    text += p.text + "\n"
        except Exception:
            pass 
        return text.strip()

    @staticmethod
    def extract_keywords(text):
        # 基于Jieba TF-IDF的关键词抽取
        # 返回前5个关键词,用','分隔
        # 文本长度<10字符时返回空(防止无效分析)
        if len(text) < 10: return ""
        tags = jieba.analyse.extract_tags(text, topK=5)
        return ",".join(tags)

# Web crawler
# 职责: BFS广度优先搜索爬虫,实现链接发现和文档采集
# 架构: 种子URL -> 队列处理 -> HTML解析+文件下载 -> 去重+存储
class USTCCrawler:
    def __init__(self, seeds):
        # 初始化爬虫状态
        # queue: BFS队列; visited: 已访问集合(URL去重); file_count: 累计下载文件数
        self.queue = deque(seeds)
        self.visited = set()
        self.storage = StorageManager()
        self.session = self._init_session()
        self.file_count = 0

    def _init_session(self):
        # 配置HTTP连接池和自动重试策略
        # 对于5xx错误使用指数退避重试(总3次,基数2s)
        session = requests.Session()
        retries = Retry(total=3, backoff_factor=2, status_forcelist=[500, 502, 503, 504])
        adapter = HTTPAdapter(max_retries=retries)
        session.mount('http://', adapter)
        session.mount('https://', adapter)
        return session

    def _get_headers(self):
        # 构造HTTP请求头(随机UA+关闭持久连接)
        return {
            'User-Agent': random.choice(Config.USER_AGENTS),
            'Connection': 'close' 
        }

    def run(self):
        # BFS主循环: 队列处理直到达到MAX_PAGES或队列为空
        # 去重: 使用URL MD5哈希跟踪已访问
        logger.info(f"Crawler started. Seeds: {len(self.queue)}")
        count = 0
        
        while self.queue and count < Config.MAX_PAGES:
            url = self.queue.popleft()
            # 用MD5哈希作为去重键(固定长度、高效)
            url_hash = hashlib.md5(url.encode()).hexdigest()
            
            if url_hash in self.visited: continue
            
            if count % 10 == 0:
                logger.info(f"Status: {count} scanned | {self.file_count} files saved. Current: {url[:50]}...")

            try:
                # 流式下载(避免大文件内存溢出)
                resp = self.session.get(
                    url, 
                    headers=self._get_headers(), 
                    timeout=30, 
                    stream=True
                )
                
                if resp.status_code == 200:
                    self._process_response(resp, url, url_hash)
                elif resp.status_code == 404:
                    pass
            
            except Exception as e:
                # 错误10053: 服务器强制关闭连接(被检测到爬虫行为)
                # 此时长时间休息再继续(避免短期IP封禁)
                if "10053" in str(e) or "Connection aborted" in str(e):
                    logger.warning("Connection aborted by server. Sleeping 60s...")
                    time.sleep(60)
                pass

            self.visited.add(url_hash)
            count += 1
            # 随机延时防封(模拟人类浏览行为)
            time.sleep(random.uniform(*Config.DELAY_RANGE))

        logger.info(f"Task finished. Total files: {self.file_count}")
        self.storage.close()

    def _process_response(self, resp, url, url_hash):
        # 响应分流处理: 区分文件下载(PDF/DOC)和HTML链接解析
        # 判断依据: URL路径后缀 > Content-Type头
        content_type = resp.headers.get('Content-Type', '').lower()
        url_lower = url.lower()
        
        file_ext = self._detect_file_type(url_lower, content_type)
        
        if file_ext:
            self._handle_file(resp, url, url_hash, file_ext)
        elif 'text/html' in content_type:
            self._handle_html(resp, url)

    def _detect_file_type(self, url, c_type):
        # 文件类型检测: 优先匹配URL路径,其次判断Content-Type
        # 目标格式: PDF/DOC/DOCX/XLS/XLSX(学术文档)
        target_exts = ['.pdf', '.doc', '.docx', '.xls', '.xlsx']
        for ext in target_exts:
            if ext in url: return ext
        if 'application/pdf' in c_type: return '.pdf'
        if 'word' in c_type: return '.docx'
        return None

    def _handle_file(self, resp, url, url_hash, ext):
        # 文件下载流程: 流式读取(限制15MB) -> 内容提取 -> 存储(HDFS+HBase)
        # 过滤: 小于100字节的文件丢弃(垃圾文件)
        try:
            content = b""
            downloaded = 0
            # 分块读取(8KB块大小,防止内存溢出)
            for chunk in resp.iter_content(chunk_size=8192):
                if chunk:
                    content += chunk
                    downloaded += len(chunk)
                    # 单文件15MB上限(大文件通常非学术资源)
                    if downloaded > 15 * 1024 * 1024: break
            
            if len(content) < 100: return

            hdfs_path = self.storage.save_file_to_hdfs(content, ext)
            
            if hdfs_path:
                self.file_count += 1
                # 内容提取和关键词计算(用于后续全文检索)
                text = ContentParser.parse_text(content, ext)
                keywords = ContentParser.extract_keywords(text)
                
                # 获取原始文件名(URL解码避免乱码)
                fname = url.split('/')[-1][:100]
                try: fname = requests.utils.unquote(fname)
                except: pass

                # HBase行键结构: 列族为元数据/数据/索引
                data = {
                    b'meta:url': url.encode(),
                    b'meta:title': fname.encode('utf-8', 'ignore'),
                    b'meta:type': b'file',
                    b'meta:date': time.strftime("%Y-%m-%d").encode(),
                    b'data:hdfs_path': hdfs_path.encode(),           # HDFS物理路径
                    b'data:content': text[:5000].encode('utf-8', 'ignore'),  # 摘要文本(5000字)
                    b'index:keywords': keywords.encode('utf-8', 'ignore')    # 全文检索索引
                }
                self.storage.save_metadata(url_hash, data)
                logger.info(f"[SAVED] {fname}")

        except Exception:
            pass

    def _handle_html(self, resp, base_url):
        # HTML链接提取和入队
        # 策略: 文档链接加入队头(优先级高),HTML链接加入队尾(BFS)
        # 过滤: URL去重+域名限制
        try:
            text = resp.content.decode(resp.encoding or 'utf-8', errors='ignore')
            soup = BeautifulSoup(text, 'html.parser')
            
            for a in soup.find_all('a', href=True):
                # 绝对路径转换+片段符号移除(#部分)
                full = urljoin(base_url, a['href']).split('#')[0]
                
                # 域名过滤+去重检查
                if Config.DOMAIN_LIMIT in full and hashlib.md5(full.encode()).hexdigest() not in self.visited:
                    # 文件链接优先级高(appendleft),HTML链接用append
                    if any(ext in full.lower() for ext in ['.pdf', '.doc', '.docx']):
                        self.queue.appendleft(full)
                    else:
                        self.queue.append(full)
        except: pass

# Entry point
if __name__ == '__main__':
    # 种子URL集合: 涵盖USTC教务、研究生、财务、学工等主要部门
    # 策略: 直接指定下载中心URL(避免过深爬取非文档页面)
    SEEDS = [
        # Academic Affairs Office
        "https://www.teach.ustc.edu.cn/download/all",
        "https://www.teach.ustc.edu.cn/download/all/page/2",
        "https://www.teach.ustc.edu.cn/download/all/page/3",
        "https://www.teach.ustc.edu.cn/download/all/page/4",
        "https://www.teach.ustc.edu.cn/download/all/page/5",
        "https://www.teach.ustc.edu.cn/download/all/page/6",
        "https://www.teach.ustc.edu.cn/download/all/page/7",
        "https://www.teach.ustc.edu.cn/download/all/page/8",
        # Graduate School
        "https://gradschool.ustc.edu.cn/column/62",
        "https://gradschool.ustc.edu.cn/column/11",
        "https://gradschool.ustc.edu.cn/column/43",
        "https://gradschool.ustc.edu.cn/column/178",
        "https://gradschool.ustc.edu.cn/column/179",
        "https://gradschool.ustc.edu.cn/column/181",
        # Finance Office
        "https://finance.ustc.edu.cn/xzzx/list.psp",
        "https://finance.ustc.edu.cn/wdxz/list.psp",
        # Student Affairs
        "https://stuhome.ustc.edu.cn/2310/list.htm",
        "https://stuhome.ustc.edu.cn/2308/list.htm",
        # Youth League
        "https://young.ustc.edu.cn/15056/list.psp",
        # Schools
        "https://cs.ustc.edu.cn/3039/list.htm",
        "https://sist.ustc.edu.cn/5085/list.htm",
        "https://cybersec.ustc.edu.cn/zlxz_34997/list.htm",
        "https://saids.ustc.edu.cn/15443/list.htm",
        # Administrative departments
        "https://bwc.ustc.edu.cn/5655/list1.htm",
        "https://lib.ustc.edu.cn/",
        "https://www.ustc.edu.cn/"  # 备选种子
    ]
    
    # 启动爬虫主程序
    crawler = USTCCrawler(SEEDS)
    crawler.run()