"""
A股历史分时数据下载器 - 优化版
作者: Antigravity
功能: 高效下载全市场股票历史分时成交数据
"""

import pandas as pd
import os
from pytdx.hq import TdxHq_API
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from tqdm import tqdm
import logging

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('download.log'),
        logging.StreamHandler()
    ]
)

class StockDataDownloader:
    """股票历史分时数据下载器"""
    
    # 通达信服务器列表(支持主备切换)
    TDX_SERVERS = [
        ('121.37.207.165', 7709),
        ('202.108.253.131', 7709),
        ('218.108.47.69', 7709),
    ]
    
    def __init__(self, stock_csv_path='data/eastmoney_all_stocks.csv'):
        """初始化下载器"""
        self.stock_csv_path = stock_csv_path
        self.stocks_df = None
        self.load_stock_list()
        
    def load_stock_list(self):
        """加载股票列表"""
        try:
            self.stocks_df = pd.read_csv(self.stock_csv_path)
            logging.info(f"成功加载 {len(self.stocks_df)} 只股票信息")
            
            # 数据清洗：确保股票代码格式统一
            self.stocks_df['stock_code'] = self.stocks_df['stock_code'].astype(str).str.zfill(6)
            
            # 添加市场代码列(tdx格式: 上海=1, 深圳=0, 北交所=2)
            # 优先使用 market_type 列,如果不存在则使用 exchange 列
            if 'market_type' in self.stocks_df.columns:
                self.stocks_df['tdx_market'] = self.stocks_df['market_type']
            else:
                exchange_map = {'上海': 1, '深圳': 0, '北交所': 2}
                self.stocks_df['tdx_market'] = self.stocks_df['exchange'].map(exchange_map)
            
            # 统计各市场股票数量
            market_counts = self.stocks_df.groupby('exchange').size()
            for market, count in market_counts.items():
                logging.info(f"  {market}: {count} 只")
            
        except Exception as e:
            logging.error(f"加载股票列表失败: {e}")
            raise
    
    def test_connectivity(self):
        """测试服务器连通性"""
        logging.info("正在测试通达信服务器连通性...")
        available_servers = []
        
        for ip, port in self.TDX_SERVERS:
            api = TdxHq_API(heartbeat=True)
            try:
                with api.connect(ip, port, time_out=2):
                    # 尝试获取一个简单数据来验证
                    if api.get_security_count(0) is not None:
                        logging.info(f"✅ 服务器 {ip}:{port} 连接正常")
                        available_servers.append((ip, port))
                    else:
                        logging.warning(f"❌ 服务器 {ip}:{port} 连接成功但无响应")
            except Exception as e:
                logging.error(f"❌ 服务器 {ip}:{port} 连接失败: {e}")
        
        if not available_servers:
            logging.error("⚠️ 警告: 所有配置的通达信服务器均无法连接！下载任务极可能全部失败。")
        else:
            logging.info(f"可用服务器数量: {len(available_servers)}")
            
        return available_servers

    def fetch_single_stock(self, stock_row, date_int, save_dir, retry_count=3):
        """
        下载单只股票的分时数据
        """
        stock_code = stock_row['stock_code']
        market = stock_row['tdx_market']
        stock_name = stock_row['stock_name']
        
        # 0. 快速检查：如果文件已存在，直接跳过
        file_path = os.path.join(save_dir, f"{stock_code}.parquet")
        if os.path.exists(file_path):
            return (stock_code, True, "文件已存在，跳过")
        
        last_error = None
        
        # 尝试多个服务器
        for server in self.TDX_SERVERS[:retry_count]:
            api = TdxHq_API()
            try:
                # 增加超时设置
                if not api.connect(server[0], server[1], time_out=5):
                    last_error = f"无法连接服务器 {server[0]}"
                    continue
                
                # 分批抓取数据
                all_data = []
                start = 0
                batch_size = 2000
                
                while True:
                    data = api.get_history_transaction_data(
                        market, stock_code, start, batch_size, date_int
                    )
                    
                    if data is None:
                        # 可能是网络中断
                        last_error = "获取数据返回None(可能是网络中断)"
                        break
                        
                    if len(data) == 0:
                        break
                        
                    all_data.extend(data)
                    
                    # 如果返回数据不足一批,说明已经全部获取
                    if len(data) < batch_size:
                        break
                        
                    start += batch_size
                
                api.disconnect()
                
                # 如果中途出错导致 data is None，则视作这次尝试失败
                if data is None:
                    continue

                # 保存数据
                if all_data:
                    df = pd.DataFrame(all_data)
                    
                    # 添加股票基本信息
                    df['stock_code'] = stock_code
                    df['stock_name'] = stock_name
                    df['exchange'] = stock_row['exchange']
                    df['date'] = date_int
                    
                    # 数据类型优化(减小文件大小)
                    if 'price' in df.columns:
                        df['price'] = df['price'].astype('float32')
                    if 'vol' in df.columns:
                        df['vol'] = df['vol'].astype('int32')
                    
                    # 保存为parquet格式(压缩+快速读取)
                    file_path = os.path.join(save_dir, f"{stock_code}.parquet")
                    df.to_parquet(file_path, compression='gzip', index=False)
                    
                    return (stock_code, True, f"成功下载 {len(all_data)} 条数据")
                else:
                    return (stock_code, False, f"当日无数据 (Exchange:{market})")
                    
            except Exception as e:
                last_error = str(e)
                try:
                    api.disconnect()
                except:
                    pass
                continue
        
        return (stock_code, False, f"尝试均失败. 错误: {last_error}")
    
    def download_all_stocks(self, date_int, max_workers=10, output_dir=None, max_retry=3, progress_callback=None):
        """
        下载全市场股票数据
        
        Args:
            date_int: 日期(20251216格式)
            max_workers: 线程数
            output_dir: 输出目录
            max_retry: 失败重试次数
            progress_callback: 进度回调函数 func(current, total)
        """
    def download_all_stocks(self, date_int, max_workers=10, output_dir=None, max_retry=3, progress_callback=None):
        """
        下载全市场股票数据
        """
        # 1. 先测试连通性，并只保留有效服务器
        valid_servers = self.test_connectivity()
        if valid_servers:
            logging.info(f"将仅使用 {len(valid_servers)} 个可用服务器进行下载")
            self.TDX_SERVERS = valid_servers
        else:
            logging.error("没有可用服务器，尝试使用默认列表继续（可能会很慢）...")
        
        # 创建保存目录
        if output_dir is None:
            output_dir = f"data/tick_{date_int}"
        os.makedirs(output_dir, exist_ok=True)
        
        logging.info(f"开始下载 {date_int} 的数据,共 {len(self.stocks_df)} 只股票")
        logging.info(f"数据保存至: {output_dir}")
        
        # 第一次下载
        success_count, failed_list = self._download_batch(
            self.stocks_df, date_int, output_dir, max_workers, "初次下载", progress_callback
        )
        
        # === 诊断信息：打印前几个失败的原因 ===
        if failed_list:
            logging.info("-" * 30)
            logging.info("🛑 失败诊断 (前5个):")
            for code, reason in failed_list[:5]:
                logging.info(f"   {code}: {reason}")
            logging.info("-" * 30)
            
            # 如果原因是"无数据", 提示用户
            if "当日无数据" in failed_list[0][1]:
                logging.warning("⚠️ 提示: 看起来服务器没有这一天的数据。")
                logging.warning("   如果是下载【今天】的数据，通常需要等到晚上(18:00后)服务器归档后才能下载历史分时。")
        # ==================================
        
        # 失败重试 (如果是因为无数据，重试也没用，这里简单判断一下)
        # 如果大量失败且原因是无数据，跳过重试
        if len(failed_list) > len(self.stocks_df) * 0.9 and "当日无数据" in failed_list[0][1]:
             logging.warning("绝大多数股票无数据，跳过重试。")
        else:
            retry_round = 1
            while failed_list and retry_round <= max_retry:
                logging.info(f"第 {retry_round} 次重试,剩余失败股票: {len(failed_list)} 只")
                
                # 从失败列表中获取股票代码
                failed_codes = [code for code, _ in failed_list]
                retry_df = self.stocks_df[self.stocks_df['stock_code'].isin(failed_codes)]
                
                retry_success, retry_failed = self._download_batch(
                    retry_df, date_int, output_dir, max_workers, f"第{retry_round}次重试", None
                )
                
                success_count += retry_success
                failed_list = retry_failed
                retry_round += 1
        
        # 生成下载报告
        self._generate_report(date_int, output_dir, success_count, failed_list)
    
    def _download_batch(self, stocks_df, date_int, output_dir, max_workers, batch_name, progress_callback=None):
        """
        批量下载股票数据
        """
        success_count = 0
        failed_list = []
        
        # 使用线程池并发下载
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # 提交所有任务
            futures = {
                executor.submit(self.fetch_single_stock, row, date_int, output_dir): row['stock_code']
                for _, row in stocks_df.iterrows()
            }
            
            total_tasks = len(futures)
            completed_tasks = 0
            
            # 使用tqdm显示进度条
            with tqdm(total=total_tasks, desc=batch_name, ncols=100) as pbar:
                for future in as_completed(futures):
                    stock_code, success, message = future.result()
                    
                    if success:
                        success_count += 1
                    else:
                        failed_list.append((stock_code, message))
                    
                    pbar.update(1)
                    pbar.set_postfix({'成功': success_count, '失败': len(failed_list)})
                    
                    # 调用外部回调
                    completed_tasks += 1
                    if progress_callback:
                        try:
                            progress_callback(completed_tasks, total_tasks)
                        except:
                            pass
        
        return success_count, failed_list
        
    def _generate_report(self, date_int, output_dir, success_count, failed_list):
        """生成下载报告"""
        total = len(self.stocks_df)
        
        logging.info("="*50)
        logging.info(f"下载完成!")
        logging.info(f"日期: {date_int}")
        logging.info(f"总计: {total} 只股票")
        logging.info(f"成功: {success_count} ({success_count/total*100:.1f}%)")
        logging.info(f"失败: {len(failed_list)} ({len(failed_list)/total*100:.1f}%)")
        logging.info("="*50)
        
        # 保存失败列表
        if failed_list:
            failed_df = pd.DataFrame(failed_list, columns=['stock_code', 'reason'])
            failed_path = os.path.join(output_dir, 'failed_stocks.csv')
            failed_df.to_csv(failed_path, index=False)
            logging.info(f"失败列表已保存至: {failed_path}")
    
    def download_date_range(self, start_date, end_date, max_workers=10, max_retry=3):
        """
        批量下载日期范围内的数据
        
        Args:
            start_date: 开始日期字符串 '20251201'
            end_date: 结束日期字符串 '20251220'
            max_workers: 线程数
            max_retry: 每日失败重试次数
        """
        from datetime import datetime, timedelta
        
        start = datetime.strptime(start_date, '%Y%m%d')
        end = datetime.strptime(end_date, '%Y%m%d')
        
        current = start
        while current <= end:
            # 跳过周末
            if current.weekday() < 5:  # 0-4是周一到周五
                date_int = int(current.strftime('%Y%m%d'))
                logging.info(f"\n{'='*60}")
                logging.info(f"处理日期: {current.strftime('%Y-%m-%d')} ({current.strftime('%A')})")
                logging.info(f"{'='*60}")
                
                self.download_all_stocks(date_int, max_workers, max_retry=max_retry)
            
            current += timedelta(days=1)


def main():
    """主函数"""
    # 创建下载器实例
    downloader = StockDataDownloader()
    
    # 示例1: 下载单个交易日数据
    # downloader.download_all_stocks(20251216, max_workers=10)
    
    # 示例2: 批量下载多日数据
    downloader.download_date_range('20251210', '20251220', max_workers=15)


if __name__ == "__main__":
    main()
