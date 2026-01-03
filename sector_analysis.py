"""
板块分析页面组件
提供行业、概念、地区三维度的板块热度分析
"""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots


def render_sector_card(sector_name, avg_pct, stock_count, total_volume, rank, sector_type='industry'):
    """
    渲染单个板块卡片
    
    Args:
        sector_name: 板块名称
        avg_pct: 平均涨跌幅
        stock_count: 成分股数量
        total_volume: 总成交量
        rank: 排名
        sector_type: 板块类型
    """
    # 确定颜色
    if avg_pct > 0:
        color = "#ff4444"  # 红色
        icon = "🔥"
    elif avg_pct < 0:
        color = "#00aa00"  # 绿色
        icon = "❄️"
    else:
        color = "#666666"  # 灰色
        icon = "⚪"
    
    # 创建卡片HTML
    card_html = f"""
    <div style="
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 20px;
        border-radius: 10px;
        box-shadow: 0 4px 6px rgba(0,0,0,0.1);
        margin: 10px 0;
        color: white;
    ">
        <div style="display: flex; justify-content: space-between; align-items: center;">
            <div style="flex: 1;">
                <div style="font-size: 12px; opacity: 0.8;">#{rank}</div>
                <div style="font-size: 18px; font-weight: bold; margin: 5px 0;">{icon} {sector_name}</div>
                <div style="font-size: 12px; opacity: 0.8;">成分股: {stock_count}只</div>
            </div>
            <div style="text-align: right;">
                <div style="font-size: 32px; font-weight: bold; color: {color};">
                    {avg_pct:+.2f}%
                </div>
                <div style="font-size: 12px; opacity: 0.8;">
                    成交: {total_volume:,.0f}
                </div>
            </div>
        </div>
    </div>
    """
    
    st.markdown(card_html, unsafe_allow_html=True)


def render_sector_analysis(engine, snapshot, top_n=10):
    """
    渲染板块分析页面
    
    Args:
        engine: 复盘引擎实例
        snapshot: 市场快照
        top_n: 显示前N个板块
    """
    st.header("📊 板块热度分析")
    
    # 创建三列布局
    col1, col2, col3 = st.columns(3)
    
    # 行业板块
    with col1:
        st.subheader("🏭 行业板块")
        industry_rankings = engine.calculate_sector_rankings(
            snapshot, 
            sector_type='industry', 
            top_n=top_n
        )
        
        if not industry_rankings.empty:
            for idx, row in industry_rankings.iterrows():
                render_sector_card(
                    row['sector'],
                    row['avg_pct_change'],
                    row['stock_count'],
                    row['total_volume'],
                    idx,
                    'industry'
                )
        else:
            st.info("暂无行业数据")
    
    # 概念板块
    with col2:
        st.subheader("💡 概念板块")
        concept_rankings = engine.calculate_sector_rankings(
            snapshot,
            sector_type='concept',
            top_n=top_n
        )
        
        if not concept_rankings.empty:
            for idx, row in concept_rankings.iterrows():
                render_sector_card(
                    row['sector'],
                    row['avg_pct_change'],
                    row['stock_count'],
                    row['total_volume'],
                    idx,
                    'concept'
                )
        else:
            st.info("暂无概念数据")
    
    # 地区板块
    with col3:
        st.subheader("🌏 地区板块")
        region_rankings = engine.calculate_sector_rankings(
            snapshot,
            sector_type='region',
            top_n=top_n
        )
        
        if not region_rankings.empty:
            for idx, row in region_rankings.iterrows():
                render_sector_card(
                    row['sector'],
                    row['avg_pct_change'],
                    row['stock_count'],
                    row['total_volume'],
                    idx,
                    'region'
                )
        else:
            st.info("暂无地区数据")


def render_sector_heatmap(engine, snapshot):
    """
    渲染板块热力图
    
    Args:
        engine: 复盘引擎实例
        snapshot: 市场快照
    """
    st.subheader("🗺️ 板块热力图")
    
    # 选择维度
    dimension = st.radio(
        "选择维度",
        ["行业", "概念", "地区"],
        horizontal=True
    )
    
    # 映射维度类型
    sector_type_map = {
        "行业": "industry",
        "概念": "concept",
        "地区": "region"
    }
    
    sector_type = sector_type_map[dimension]
    
    # 获取数据
    rankings = engine.calculate_sector_rankings(
        snapshot,
        sector_type=sector_type,
        top_n=30
    )
    
    if not rankings.empty:
        # 创建热力图
        fig = go.Figure(data=go.Bar(
            x=rankings['avg_pct_change'],
            y=rankings['sector'],
            orientation='h',
            marker=dict(
                color=rankings['avg_pct_change'],
                colorscale='RdYlGn',
                colorbar=dict(title="涨跌幅%"),
                cmin=-5,
                cmax=5
            ),
            text=rankings['avg_pct_change'].apply(lambda x: f"{x:+.2f}%"),
            textposition='auto',
        ))
        
        fig.update_layout(
            title=f"{dimension}板块涨跌幅分布",
            xaxis_title="平均涨跌幅(%)",
            yaxis_title=dimension,
            height=800,
            yaxis={'categoryorder': 'total ascending'}
        )
        
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info(f"暂无{dimension}数据")


def render_rapid_rise_sectors(engine, snapshot, time_window=5, threshold=3.0, top_n=10):
    """
    渲染快速拉升板块
    
    Args:
        engine: 复盘引擎实例
        snapshot: 市场快照
        time_window: 时间窗口(分钟)
        threshold: 涨幅阈值(%)
        top_n: 显示前N个
    """
    st.subheader("🚀 快速拉升板块")
    
    # 获取快速拉升股票
    rapid_stocks = engine.detect_rapid_rise(
        time_window_minutes=time_window,
        pct_threshold=threshold
    )
    
    if not rapid_stocks:
        st.info(f"暂无{time_window}分钟内涨幅超过{threshold}%的板块")
        return
    
    # 统计各板块的拉升股票数
    sector_rapid_count = {
        'industry': {},
        'concept': {},
        'region': {}
    }
    
    for stock in rapid_stocks:
        code = stock['stock_code']
        
        # 行业
        industries = engine.industry_map.get(code, ['未知'])
        for industry in industries:
            sector_rapid_count['industry'][industry] = sector_rapid_count['industry'].get(industry, 0) + 1
        
        # 概念
        concepts = engine.concept_map.get(code, ['未知'])
        for concept in concepts:
            sector_rapid_count['concept'][concept] = sector_rapid_count['concept'].get(concept, 0) + 1
        
        # 地区
        regions = engine.region_map.get(code, ['未知'])
        for region in regions:
            sector_rapid_count['region'][region] = sector_rapid_count['region'].get(region, 0) + 1
    
    # 展示三维度拉升板块
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown("### 🏭 行业拉升")
        if sector_rapid_count['industry']:
            sorted_industries = sorted(
                sector_rapid_count['industry'].items(),
                key=lambda x: x[1],
                reverse=True
            )[:top_n]
            
            for sector, count in sorted_industries:
                st.metric(
                    label=sector,
                    value=f"{count}只",
                    delta="拉升中"
                )
    
    with col2:
        st.markdown("### 💡 概念拉升")
        if sector_rapid_count['concept']:
            sorted_concepts = sorted(
                sector_rapid_count['concept'].items(),
                key=lambda x: x[1],
                reverse=True
            )[:top_n]
            
            for sector, count in sorted_concepts:
                st.metric(
                    label=sector,
                    value=f"{count}只",
                    delta="拉升中"
                )
    
    with col3:
        st.markdown("### 🌏 地区拉升")
        if sector_rapid_count['region']:
            sorted_regions = sorted(
                sector_rapid_count['region'].items(),
                key=lambda x: x[1],
                reverse=True
            )[:top_n]
            
            for sector, count in sorted_regions:
                st.metric(
                    label=sector,
                    value=f"{count}只",
                    delta="拉升中"
                )
