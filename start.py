#!/usr/bin/env python3
"""
A股历史复盘系统 - 快速启动脚本
"""

import sys
import subprocess
from pathlib import Path


def check_dependencies():
    """检查依赖包是否已安装"""
    print("📦 检查依赖包...")
    
    required_packages = [
        'pandas',
        'pytdx',
        'streamlit',
        'tqdm',
        'pyarrow',
    ]
    
    missing_packages = []
    
    for package in required_packages:
        try:
            __import__(package)
        except ImportError:
            missing_packages.append(package)
    
    if missing_packages:
        print(f"❌ 缺少以下依赖包: {', '.join(missing_packages)}")
        print("\n请运行以下命令安装:")
        print("  pip install -r requirements.txt")
        return False
    
    print("✅ 所有依赖包已安装")
    return True


def check_data():
    """检查是否有数据"""
    print("\n📊 检查数据...")
    
    data_dir = Path("data")
    
    # 检查股票列表
    stock_list = data_dir / "eastmoney_all_stocks.csv"
    if not stock_list.exists():
        print(f"❌ 未找到股票列表: {stock_list}")
        return False
    
    print(f"✅ 股票列表: {stock_list}")
    
    # 检查tick数据
    tick_dirs = list(data_dir.glob("tick_*"))
    
    if not tick_dirs:
        print("⚠️  未找到tick数据目录")
        print("\n请先下载数据:")
        print("  python downloader.py")
        return False
    
    print(f"✅ 找到 {len(tick_dirs)} 个交易日数据:")
    for tick_dir in tick_dirs[:5]:
        parquet_count = len(list(tick_dir.glob("*.parquet")))
        print(f"   - {tick_dir.name}: {parquet_count} 只股票")
    
    if len(tick_dirs) > 5:
        print(f"   ... 还有 {len(tick_dirs) - 5} 个交易日")
    
    return True


def main():
    """主函数"""
    print("=" * 60)
    print("🚀 A股历史复盘系统 - 快速启动")
    print("=" * 60)
    
    # 检查依赖
    if not check_dependencies():
        sys.exit(1)
    
    # 检查数据
    has_data = check_data()
    
    print("\n" + "=" * 60)
    print("📋 启动选项:")
    print("=" * 60)
    print("1. 启动Web复盘界面")
    print("2. 下载历史数据")
    print("3. 生成行业映射文件")
    print("4. 退出")
    
    choice = input("\n请选择 (1-4): ").strip()
    
    if choice == '1':
        if not has_data:
            print("\n⚠️  警告: 未找到数据,复盘界面可能无法正常使用")
            proceed = input("是否继续启动? (y/n): ").strip().lower()
            if proceed != 'y':
                return
        
        print("\n🌐 启动Web界面...")
        print("浏览器将自动打开 http://localhost:8501")
        print("\n按 Ctrl+C 停止服务\n")
        
        subprocess.run(['streamlit', 'run', 'app.py'])
    
    elif choice == '2':
        print("\n📥 数据下载向导")
        print("-" * 60)
        
        # 获取日期
        date_str = input("请输入日期 (格式: YYYYMMDD, 如 20251216): ").strip()
        
        if len(date_str) != 8 or not date_str.isdigit():
            print("❌ 日期格式错误!")
            return
        
        # 获取线程数
        workers = input("并发线程数 (默认: 15): ").strip()
        workers = int(workers) if workers.isdigit() else 15
        
        print(f"\n开始下载 {date_str} 的数据,使用 {workers} 个线程...")
        print("=" * 60)
        
        # 执行下载
        code = f"""
from downloader import StockDataDownloader
downloader = StockDataDownloader()
downloader.download_all_stocks({date_str}, max_workers={workers})
"""
        exec(code)
    
    elif choice == '3':
        print("\n🏢 生成行业映射文件...")
        subprocess.run(['python', 'generate_industry_mapping.py'])
    
    elif choice == '4':
        print("\n👋 再见!")
        return
    
    else:
        print("\n❌ 无效选项!")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n👋 用户取消操作")
    except Exception as e:
        print(f"\n❌ 发生错误: {e}")
        import traceback
        traceback.print_exc()
