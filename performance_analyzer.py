"""
性能分析和优化工具
用于分析复盘系统的性能瓶颈
"""

import time
import psutil
import os
from pathlib import Path
from functools import wraps
import pandas as pd


def timing_decorator(func):
    """计时装饰器"""
    @wraps(func)
    def wrapper(*args, **kwargs):
        start = time.time()
        result = func(*args, **kwargs)
        end = time.time()
        print(f"⏱️  {func.__name__} 耗时: {end - start:.2f}秒")
        return result
    return wrapper


class PerformanceAnalyzer:
    """性能分析器"""
    
    def __init__(self):
        self.process = psutil.Process(os.getpid())
        
    def get_memory_usage(self):
        """获取当前内存使用情况"""
        mem_info = self.process.memory_info()
        mem_mb = mem_info.rss / 1024 / 1024
        return mem_mb
    
    def get_cpu_usage(self):
        """获取CPU使用率"""
        return self.process.cpu_percent(interval=1)
    
    def analyze_data_size(self, data_dir):
        """分析数据大小"""
        data_path = Path(data_dir)
        
        print("=" * 60)
        print("📊 数据大小分析")
        print("=" * 60)
        
        # 统计文件数量和大小
        parquet_files = list(data_path.glob("*.parquet"))
        
        if not parquet_files:
            print("未找到parquet文件")
            return
        
        total_size = sum(f.stat().st_size for f in parquet_files)
        avg_size = total_size / len(parquet_files)
        
        print(f"文件数量: {len(parquet_files)}")
        print(f"总大小: {total_size / 1024 / 1024:.2f} MB")
        print(f"平均大小: {avg_size / 1024:.2f} KB")
        
        # 采样分析
        sample_size = min(10, len(parquet_files))
        sample_files = parquet_files[:sample_size]
        
        print(f"\n📋 采样分析 (前{sample_size}个文件):")
        print("-" * 60)
        
        total_rows = 0
        for file_path in sample_files:
            df = pd.read_parquet(file_path)
            rows = len(df)
            size_kb = file_path.stat().st_size / 1024
            total_rows += rows
            
            print(f"{file_path.stem}: {rows:6,} 行 | {size_kb:8.2f} KB")
        
        avg_rows = total_rows / sample_size
        estimated_total_rows = avg_rows * len(parquet_files)
        
        print("-" * 60)
        print(f"平均每文件: {avg_rows:,.0f} 行")
        print(f"估计总行数: {estimated_total_rows:,.0f} 行")
        
        return {
            'file_count': len(parquet_files),
            'total_size_mb': total_size / 1024 / 1024,
            'avg_rows': avg_rows,
            'estimated_total_rows': estimated_total_rows,
        }
    
    @timing_decorator
    def benchmark_loading(self, data_dir, sample_size=100):
        """基准测试: 数据加载速度"""
        print("\n" + "=" * 60)
        print("⚡ 加载速度基准测试")
        print("=" * 60)
        
        data_path = Path(data_dir)
        parquet_files = list(data_path.glob("*.parquet"))[:sample_size]
        
        start_mem = self.get_memory_usage()
        
        # 测试加载
        dfs = []
        for file_path in parquet_files:
            df = pd.read_parquet(file_path)
            dfs.append(df)
        
        end_mem = self.get_memory_usage()
        mem_increase = end_mem - start_mem
        
        print(f"加载文件数: {sample_size}")
        print(f"内存增加: {mem_increase:.2f} MB")
        print(f"平均每文件: {mem_increase / sample_size:.2f} MB")
        
        # 估算全量加载内存需求
        total_files = len(list(data_path.glob("*.parquet")))
        estimated_mem = mem_increase / sample_size * total_files
        
        print(f"\n💡 全量加载估算:")
        print(f"预计总内存: {estimated_mem:.2f} MB ({estimated_mem / 1024:.2f} GB)")
        
        return {
            'sample_size': sample_size,
            'mem_increase_mb': mem_increase,
            'estimated_total_mem_gb': estimated_mem / 1024,
        }
    
    @timing_decorator
    def benchmark_ranking_calculation(self, data_dir):
        """基准测试: 排行榜计算速度"""
        print("\n" + "=" * 60)
        print("📈 排行榜计算基准测试")
        print("=" * 60)
        
        from replay_engine import ReplayEngine
        from datetime import datetime
        
        engine = ReplayEngine(data_dir)
        
        # 加载部分数据
        data_path = Path(data_dir)
        parquet_files = list(data_path.glob("*.parquet"))[:500]
        
        print(f"加载 {len(parquet_files)} 只股票数据...")
        for file_path in parquet_files:
            engine.lazy_load_stock(file_path.stem)
        
        # 测试快照生成
        test_time = datetime(2025, 12, 16, 10, 30, 0)
        
        start = time.time()
        snapshot = engine.get_snapshot_at_time(test_time)
        snapshot_time = time.time() - start
        
        print(f"快照生成: {snapshot_time:.3f}秒")
        
        # 测试排行计算
        start = time.time()
        stock_rankings = engine.calculate_stock_rankings(snapshot, top_n=30)
        ranking_time = time.time() - start
        
        print(f"个股排行: {ranking_time:.3f}秒")
        
        # 测试拉升检测
        start = time.time()
        rapid_rise = engine.detect_rapid_rise(time_window_minutes=5, pct_threshold=3.0)
        rapid_time = time.time() - start
        
        print(f"拉升检测: {rapid_time:.3f}秒")
        
        total_time = snapshot_time + ranking_time + rapid_time
        
        print(f"\n总耗时: {total_time:.3f}秒")
        print(f"预估FPS: {1 / total_time:.1f} 次/秒")
        
        return {
            'snapshot_time': snapshot_time,
            'ranking_time': ranking_time,
            'rapid_time': rapid_time,
            'total_time': total_time,
        }
    
    def generate_optimization_report(self, data_dir):
        """生成优化建议报告"""
        print("\n" + "=" * 60)
        print("💡 优化建议")
        print("=" * 60)
        
        # 获取系统信息
        total_mem = psutil.virtual_memory().total / 1024 / 1024 / 1024
        available_mem = psutil.virtual_memory().available / 1024 / 1024 / 1024
        cpu_count = psutil.cpu_count()
        
        print(f"\n🖥️  系统配置:")
        print(f"CPU核心数: {cpu_count}")
        print(f"总内存: {total_mem:.1f} GB")
        print(f"可用内存: {available_mem:.1f} GB")
        
        # 分析数据
        data_stats = self.analyze_data_size(data_dir)
        
        if data_stats:
            estimated_mem_gb = data_stats.get('estimated_total_mem_gb', 0)
            
            print(f"\n📊 数据分析:")
            print(f"股票数量: {data_stats['file_count']}")
            print(f"数据大小: {data_stats['total_size_mb']:.2f} MB")
            print(f"估计总行数: {data_stats['estimated_total_rows']:,.0f}")
            
            print(f"\n💾 内存需求:")
            print(f"全量加载需要: {estimated_mem_gb:.2f} GB")
            
            # 生成建议
            print(f"\n✅ 优化建议:")
            
            if estimated_mem_gb > available_mem * 0.8:
                print("1. ⚠️  内存可能不足,建议使用按需加载策略")
                print("   - 在 replay_engine.py 中不调用 load_all_data()")
                print("   - 或减少同时分析的股票数量")
            else:
                print("1. ✅ 内存充足,可以使用全量加载策略")
                print("   - 在 replay_engine.py 中调用 load_all_data()")
            
            if cpu_count >= 8:
                print(f"2. ✅ CPU核心充足({cpu_count}核),可增加下载线程数")
                print(f"   - max_workers 可设置为 {cpu_count * 2}-{cpu_count * 3}")
            else:
                print(f"2. ⚠️  CPU核心较少({cpu_count}核),建议适度并发")
                print(f"   - max_workers 建议设置为 {cpu_count}-{cpu_count * 2}")
            
            if data_stats['avg_rows'] > 10000:
                print("3. 💡 分时数据较多,建议:")
                print("   - 使用 parquet 压缩格式(已默认)")
                print("   - 考虑数据降采样(如每5秒一个点)")
            
            print("\n4. 🚀 性能优化技巧:")
            print("   - 使用SSD存储数据")
            print("   - 关闭不必要的后台程序")
            print("   - 减少排行榜显示数量")
            print("   - 增加回放速度间隔(如5-10秒)")


def main():
    """主函数"""
    print("=" * 60)
    print("🔍 A股复盘系统 - 性能分析工具")
    print("=" * 60)
    
    # 选择数据目录
    data_dirs = list(Path("data").glob("tick_*"))
    
    if not data_dirs:
        print("❌ 未找到数据目录")
        return
    
    print("\n可用的数据目录:")
    for idx, dir_path in enumerate(data_dirs, 1):
        print(f"{idx}. {dir_path.name}")
    
    choice = input(f"\n请选择 (1-{len(data_dirs)}): ").strip()
    
    try:
        selected_dir = data_dirs[int(choice) - 1]
    except (ValueError, IndexError):
        print("无效选择")
        return
    
    # 执行分析
    analyzer = PerformanceAnalyzer()
    
    print(f"\n正在分析: {selected_dir}")
    
    # 1. 数据大小分析
    data_stats = analyzer.analyze_data_size(selected_dir)
    
    # 2. 加载速度测试
    if input("\n是否进行加载速度测试? (y/n): ").strip().lower() == 'y':
        sample_size = int(input("测试样本大小 (建议100-500): ").strip() or "100")
        analyzer.benchmark_loading(selected_dir, sample_size=sample_size)
    
    # 3. 计算速度测试
    if input("\n是否进行计算速度测试? (y/n): ").strip().lower() == 'y':
        analyzer.benchmark_ranking_calculation(selected_dir)
    
    # 4. 生成优化报告
    analyzer.generate_optimization_report(selected_dir)
    
    print("\n" + "=" * 60)
    print("✅ 分析完成!")
    print("=" * 60)


if __name__ == "__main__":
    main()
