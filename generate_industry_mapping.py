"""
生成股票行业映射文件
从东方财富获取股票行业分类信息
"""

import pandas as pd
import logging

logging.basicConfig(level=logging.INFO)


def generate_industry_mapping_from_csv():
    """
    从现有的eastmoney_all_stocks.csv生成简单的行业映射
    注意: 这是一个示例,实际应该从东方财富API获取完整行业数据
    """
    # 读取股票列表
    stocks_df = pd.read_csv('data/eastmoney_all_stocks.csv')
    
    # 简单分类逻辑(基于股票代码段,仅供演示)
    def classify_by_code(code):
        """简单的行业分类规则(仅供演示)"""
        code = str(code).zfill(6)
        
        # 银行
        if code in ['600000', '600015', '600016', '600036', '601169', '601328', '601398', '601939', '601988', '601998']:
            return '银行'
        
        # 券商
        if code in ['600030', '600109', '600369', '600999', '601066', '601099', '601198', '601377', '601555', '601688', '601788', '601881', '601901']:
            return '券商'
        
        # 保险
        if code in ['601318', '601319', '601336', '601601', '601628', '601838']:
            return '保险'
        
        # 房地产
        if code.startswith('000') and int(code) % 7 == 0:
            return '房地产'
        
        # 科技
        if code.startswith('300') or code.startswith('688'):
            return '科技'
        
        # 医药
        if code.startswith('6') and int(code) % 11 == 0:
            return '医药'
        
        # 消费
        if code.startswith('0') and int(code) % 13 == 0:
            return '消费'
        
        # 制造业
        if code.startswith('6') and int(code) % 5 == 0:
            return '制造业'
        
        # 其他
        return '其他'
    
    # 应用分类
    stocks_df['industry'] = stocks_df['stock_code'].apply(classify_by_code)
    
    # 保存映射文件
    mapping_df = stocks_df[['stock_code', 'industry']].copy()
    mapping_df['stock_code'] = mapping_df['stock_code'].astype(str).str.zfill(6)
    
    output_path = 'data/stock_industry_mapping.csv'
    mapping_df.to_csv(output_path, index=False)
    
    logging.info(f"行业映射文件已生成: {output_path}")
    logging.info("\n行业分布:")
    logging.info(mapping_df['industry'].value_counts())


def fetch_real_industry_data():
    """
    从东方财富获取真实的行业数据
    
    注意: 这需要安装akshare库: pip install akshare
    """
    try:
        import akshare as ak
        
        logging.info("正在从东方财富获取行业数据...")
        
        # 获取股票行业分类
        # 注意: akshare的API可能会变化,请查阅最新文档
        industry_data = ak.stock_board_industry_name_em()
        
        logging.info(f"获取到 {len(industry_data)} 个行业板块")
        
        # 获取每个行业的成分股
        all_stocks = []
        
        for idx, row in industry_data.iterrows():
            industry_name = row['板块名称']
            logging.info(f"正在获取 {industry_name} 的成分股...")
            
            try:
                # 获取行业成分股
                stocks = ak.stock_board_industry_cons_em(symbol=industry_name)
                
                for _, stock in stocks.iterrows():
                    all_stocks.append({
                        'stock_code': stock['代码'],
                        'stock_name': stock['名称'],
                        'industry': industry_name,
                    })
            except Exception as e:
                logging.warning(f"获取 {industry_name} 失败: {e}")
        
        # 保存
        if all_stocks:
            result_df = pd.DataFrame(all_stocks)
            result_df = result_df.drop_duplicates(subset=['stock_code'])
            
            output_path = 'data/stock_industry_mapping.csv'
            result_df[['stock_code', 'industry']].to_csv(output_path, index=False)
            
            logging.info(f"✅ 成功获取 {len(result_df)} 只股票的行业信息")
            logging.info(f"保存至: {output_path}")
            
            return result_df
        else:
            logging.error("未获取到任何数据")
            return None
            
    except ImportError:
        logging.error("请先安装akshare: pip install akshare")
        return None
    except Exception as e:
        logging.error(f"获取数据失败: {e}")
        return None


if __name__ == "__main__":
    print("=" * 60)
    print("股票行业映射文件生成工具")
    print("=" * 60)
    print("\n请选择生成方式:")
    print("1. 快速生成(基于股票代码简单分类,仅供测试)")
    print("2. 从东方财富获取真实数据(需要安装akshare)")
    
    choice = input("\n请输入选项(1或2): ").strip()
    
    if choice == '1':
        generate_industry_mapping_from_csv()
    elif choice == '2':
        result = fetch_real_industry_data()
        if result is None:
            logging.info("真实数据获取失败,回退到简单分类...")
            generate_industry_mapping_from_csv()
    else:
        print("无效选项!")
