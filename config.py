"""
复盘系统配置文件
"""

# ========== 数据下载配置 ==========

# 通达信服务器列表
TDX_SERVERS = [
    ('119.147.212.81', 7709),
    ('202.108.253.131', 7709),
    ('218.108.47.69', 7709),
]

# 下载配置
DOWNLOAD_CONFIG = {
    'max_workers': 15,          # 并发线程数(建议10-20)
    'batch_size': 2000,         # 单次请求数据量
    'retry_count': 3,           # 重试次数
    'timeout': 30,              # 超时时间(秒)
}

# 数据存储配置
STORAGE_CONFIG = {
    'format': 'parquet',        # 存储格式: parquet/csv
    'compression': 'gzip',      # 压缩方式: gzip/snappy/none
    'data_dir': 'data',         # 数据根目录
}

# ========== 复盘引擎配置 ==========

# 交易时间
TRADING_HOURS = {
    'morning_start': '09:30:00',
    'morning_end': '11:30:00',
    'afternoon_start': '13:00:00',
    'afternoon_end': '15:00:00',
}

# 复盘设置
REPLAY_CONFIG = {
    'default_speed': 5,         # 默认回放速度(秒/次)
    'min_speed': 1,             # 最小速度
    'max_speed': 60,            # 最大速度
    'load_strategy': 'lazy',    # 加载策略: 'all'全量 / 'lazy'按需
}

# 排行榜配置
RANKING_CONFIG = {
    'stock_top_n': 30,          # 个股排行榜显示数量
    'sector_top_n': 15,         # 板块排行榜显示数量
    'refresh_interval': 1,      # 刷新间隔(秒)
}

# 板块映射配置
SECTOR_MAPPING_CONFIG = {
    'source': 'iwencai',        # 数据源: iwencai / eastmoney
    'iwencai_files': {
        'industry': 'data/iwencai_industry_stocks.csv',
        'concept': 'data/iwencai_concept_stocks.csv',
        'region': 'data/iwencai_region_stocks.csv'
    },
    'eastmoney_files': {
        'industry': 'data/eastmoney_stocks_by_industry.csv',
        'concept': 'data/eastmoney_stocks_by_concept.csv',
        'region': 'data/eastmoney_stocks_by_region.csv'
    }
}

# 拉升检测配置
RAPID_RISE_CONFIG = {
    'time_window': 5,           # 时间窗口(分钟)
    'pct_threshold': 3.0,       # 涨幅阈值(%)
    'min_price': 2.0,           # 最低价格过滤(元)
    'min_volume': 10000,        # 最小成交量(手)
}

# ========== 可视化配置 ==========

# 界面主题
THEME_CONFIG = {
    'primary_color': '#FF4444',
    'background_color': '#FFFFFF',
    'secondary_background': '#F0F2F6',
    'text_color': '#262730',
}

# 图表配置
CHART_CONFIG = {
    'height': 400,
    'show_grid': True,
    'show_legend': True,
}

# ========== 性能优化配置 ==========

# 缓存配置
CACHE_CONFIG = {
    'enable_cache': True,
    'cache_ttl': 60,            # 缓存有效期(秒)
    'max_cache_size': 1000,     # 最大缓存条目数
}

# 内存管理
MEMORY_CONFIG = {
    'max_stocks_in_memory': 500,  # 内存中最多保留股票数
    'gc_interval': 100,            # 垃圾回收间隔
}

# ========== 日志配置 ==========

LOGGING_CONFIG = {
    'level': 'INFO',            # 日志级别: DEBUG/INFO/WARNING/ERROR
    'format': '%(asctime)s - %(levelname)s - %(message)s',
    'file': 'replay_system.log',
    'max_size': 10 * 1024 * 1024,  # 最大文件大小(10MB)
    'backup_count': 5,          # 保留备份数
}

# ========== 数据验证配置 ==========

# 数据质量检查
DATA_QUALITY = {
    'check_duplicates': True,   # 检查重复数据
    'check_missing': True,      # 检查缺失值
    'outlier_threshold': 3,     # 异常值检测阈值(标准差倍数)
}

# ========== 高级功能配置 ==========

# 策略回测配置
BACKTEST_CONFIG = {
    'initial_capital': 1000000,  # 初始资金(元)
    'commission_rate': 0.0003,   # 手续费率
    'slippage': 0.001,           # 滑点
}

# 预警配置
ALERT_CONFIG = {
    'enable_alert': False,
    'alert_channels': ['console'],  # 预警渠道: console/email/dingtalk
    'alert_rules': {
        'limit_up': True,        # 涨停预警
        'limit_down': True,      # 跌停预警
        'rapid_rise': True,      # 快速拉升预警
        'huge_volume': True,     # 放量预警
    }
}
