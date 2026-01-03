"""
下载股票昨收价数据 - 高速并发版本
使用线程池并发请求，速度提升 10-20 倍
"""

import pandas as pd
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
import time
from tqdm import tqdm

def get_stock_pre_close_single(stock_code):
    """
    获取单只股票的昨收价
    
    Args:
        stock_code: 股票代码
    
    Returns:
        dict: {'stock_code': code, 'pre_close': price} 或 None
    """
    # 判断市场代码
    if stock_code.startswith('6'):
        secid = f"1.{stock_code}"  # 上海
    elif stock_code.startswith('0') or stock_code.startswith('3'):
        secid = f"0.{stock_code}"  # 深圳
    elif stock_code.startswith('8') or stock_code.startswith('4'):
        secid = f"0.{stock_code}"  # 北交所
    else:
        secid = f"0.{stock_code}"  # 默认深圳
    
    base_url = "http://push2.eastmoney.com/api/qt/stock/get"
    params = {
        'secid': secid,
        'fields': 'f60',  # f60 是昨收价
    }
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    }
    
    try:
        response = requests.get(base_url, params=params, headers=headers, timeout=10)
        
        if response.status_code != 200:
            return None # 状态码异常
            
        data = response.json()
        
        if data.get('data') and data['data'].get('f60'):
            pre_close = data['data']['f60']
            # 过滤掉无效值
            if pre_close == '-':
                return None
                
            return {
                'stock_code': stock_code,
                'pre_close': pre_close
            }
    except Exception as e:
        # 仅在调试时打印
        # print(f"Error {stock_code}: {e}")
        pass
    
    return None


def _download_batch_internal(stock_codes, max_workers, desc):
    """内部批量下载 helper"""
    results = []
    failed_codes = []
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_code = {
            executor.submit(get_stock_pre_close_single, code): code 
            for code in stock_codes
        }
        
        # ncols=80 避免换行混乱
        with tqdm(total=len(stock_codes), desc=desc, ncols=80) as pbar:
            for future in as_completed(future_to_code):
                code = future_to_code[future]
                try:
                    result = future.result()
                    if result:
                        results.append(result)
                    else:
                        failed_codes.append(code)
                except Exception:
                    failed_codes.append(code)
                pbar.update(1)
    
    return results, failed_codes

def download_pre_close_parallel(stock_codes, max_workers=50, max_retries=3):
    """
    并发下载昨收价 (带重试机制)
    
    Args:
        stock_codes: 股票代码列表
        max_workers: 最大并发线程数
        max_retries: 失败重试次数
    
    Returns:
        DataFrame with columns: stock_code, pre_close
    """
    all_results = []
    
    print(f"开始并发下载 {len(stock_codes)} 只股票的昨收价...")
    print(f"并发线程数: {max_workers}")

    # 1. 初次下载
    results, failed = _download_batch_internal(stock_codes, max_workers, "初次下载")
    all_results.extend(results)
    
    # 2. 失败重试
    for i in range(max_retries):
        if not failed:
            break
            
        print(f"\n🔄 第 {i+1}/{max_retries} 次失败重试 (剩余 {len(failed)} 只)...")
        # 重试时降低并发，减少被封概率
        retry_workers = max(5, int(max_workers * 0.5))
        time.sleep(1) # 歇一会
        
        results, new_failed = _download_batch_internal(failed, retry_workers, f"重试 {i+1}")
        all_results.extend(results)
        failed = new_failed
        
    print(f"\n✅ 最终成功: {len(all_results)} 只")
    print(f"❌ 最终失败: {len(failed)} 只")
    
    if failed and len(failed) <= 10:
        print(f"失败股票: {', '.join(failed)}")
    
    return pd.DataFrame(all_results)


def main():
    import argparse
    
    parser = argparse.ArgumentParser(description='下载股票昨收价数据')
    parser.add_argument('--date', type=str, default='20251222', help='日期，格式 YYYYMMDD')
    parser.add_argument('--workers', type=int, default=50, help='并发线程数（默认50）')
    args = parser.parse_args()
    
    date_str = args.date
    max_workers = args.workers
    
    # 读取所有股票代码
    stocks_df = pd.read_csv('data/eastmoney_all_stocks.csv')
    stocks_df['stock_code'] = stocks_df['stock_code'].astype(str).str.zfill(6)
    
    # 检查 tick 数据目录
    tick_dir = Path(f'data/{date_str}/tick')
    if tick_dir.exists():
        tick_stocks = [f.stem for f in tick_dir.glob('*.parquet')]
        print(f"找到 {len(tick_stocks)} 只有 tick 数据的股票")
        stock_codes = tick_stocks
    else:
        print(f"未找到 tick 数据目录: {tick_dir}")
        print("获取所有股票昨收价")
        stock_codes = stocks_df['stock_code'].tolist()
    
    # 记录开始时间
    start_time = time.time()
    
    # 并发下载
    pre_close_df = download_pre_close_parallel(stock_codes, max_workers=max_workers)
    
    # 计算耗时
    elapsed = time.time() - start_time
    print(f"\n⏱️  总耗时: {elapsed:.1f} 秒")
    print(f"📊 平均速度: {len(stock_codes) / elapsed:.1f} 只/秒")
    
    # 保存到对应日期的目录
    output_dir = Path(f'data/{date_str}')
    output_dir.mkdir(parents=True, exist_ok=True)
    output_file = output_dir / f'stock_pre_close_{date_str}.csv'
    
    pre_close_df.to_csv(output_file, index=False, encoding='utf-8-sig')
    
    print(f"\n✅ 完成！共获取 {len(pre_close_df)} 只股票的昨收价")
    print(f"💾 保存到: {output_file}")
    
    # 显示样例
    print("\n样例数据:")
    print(pre_close_df.head(10))


if __name__ == '__main__':
    main()
