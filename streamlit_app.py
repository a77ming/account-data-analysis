import streamlit as st
import pandas as pd
import requests
from datetime import datetime
import time
import io
import numpy as np
from typing import List

# 设置页面配置
st.set_page_config(
    page_title="TikTok 账号价值分析工具",
    page_icon="📊",
    layout="wide"
)



def fetch_user_info(username: str, log_container=None) -> dict:
    """获取用户详细信息"""
    url = f"https://www.tikwm.com/api/user/info?unique_id={username}"
    try:
        response = requests.get(url, timeout=15)  # 增加超时时间
        data = response.json()
        if data.get("code") == 0 and "data" in data:
            user_data = data["data"]["user"]
            stats_data = data["data"]["stats"]
            result = {
                "昵称": user_data.get("nickname", ""),
                "头像": user_data.get("avatarMedium", user_data.get("avatarThumb", "")),
                "关注数": stats_data.get("followingCount", 0),
                "粉丝数": stats_data.get("followerCount", 0),
                "获赞数": stats_data.get("heartCount", stats_data.get("heart", 0)),
                "总视频数": stats_data.get("videoCount", 0)
            }
            # 记录到日志容器
            if log_container:
                log_container.success(f"✅ {username}: 粉丝{result['粉丝数']}人, 关注{result['关注数']}人, 获赞{result['获赞数']}个")
            return result
        else:
            # API返回错误
            error_msg = data.get('msg', '未知错误')
            if log_container:
                log_container.error(f"❌ {username} 用户信息获取失败: {error_msg}")
            return {
                "昵称": username,
                "头像": "",
                "关注数": 0,
                "粉丝数": 0,
                "获赞数": 0,
                "总视频数": 0
            }
    except Exception as e:
        # 网络错误
        if log_container:
            log_container.error(f"🚨 {username} 网络请求失败: {str(e)}")
        return {
            "昵称": username,
            "头像": "",
            "关注数": 0,
            "粉丝数": 0,
            "获赞数": 0,
            "总视频数": 0
        }

def fetch_user_videos(username: str, limit: int = 3, log_container=None) -> List[dict]:
    """抓取指定用户的视频数据"""
    url = f"https://www.tikwm.com/api/user/posts?unique_id={username}"
    try:
        response = requests.get(url, timeout=15)  # 增加超时时间
        data = response.json()
        result = []

        if data.get("code") == 0 and "data" in data:
            # 获取用户详细信息
            user_info = fetch_user_info(username, log_container)
            
            # 检查数据结构
            data_content = data["data"]
            videos = data_content.get("videos", [])[:limit]
            
            if not videos:
                if log_container:
                    log_container.warning(f"⚠️ {username} 没有找到视频数据")
                return []
            
            for video in videos:
                # 每个视频都有author信息，使用当前视频的author信息
                author = video.get("author", {})
                
                result.append({
                    "账号": username,
                    "昵称": user_info.get("昵称", author.get("nickname", author.get("unique_id", ""))),
                    "头像": user_info.get("头像", author.get("avatar", "")),
                    "关注数": user_info.get("关注数", 0),
                    "粉丝数": user_info.get("粉丝数", 0),
                    "获赞数": user_info.get("获赞数", 0),
                    "总视频数": user_info.get("总视频数", 0),
                    "视频链接": f"https://www.tiktok.com/@{username}/video/{video.get('video_id', '')}",
                    "发布时间": datetime.fromtimestamp(video.get("create_time", 0)).strftime("%Y-%m-%d %H:%M:%S") if video.get("create_time") else "",
                    "播放量": video.get("play_count", 0),
                    "点赞": video.get("digg_count", 0),
                    "评论": video.get("comment_count", 0),
                    "收藏": video.get("collect_count", 0),
                    "封面图链接": video.get("cover", "")
                })
            
            if log_container:
                log_container.success(f"✅ {username} 成功获取 {len(result)} 条视频数据")
            
        else:
            error_msg = data.get('msg', '未知错误')
            if log_container:
                log_container.error(f"❌ {username} 视频获取失败: {error_msg}")

        return result
    except Exception as e:
        if log_container:
            log_container.error(f"🚨 {username} 视频获取网络错误: {str(e)}")
        return []

def detect_throttling(df: pd.DataFrame) -> pd.DataFrame:
    """检测视频限流状态"""
    df_with_throttling = df.copy()
    
    # 限流检测参数
    CLEAR_THROTTLING_THRESHOLD = 10  # 明确限流：播放量阈值
    SUSPECTED_THROTTLING_THRESHOLD = 50  # 疑似限流：播放量阈值
    MIN_ENGAGEMENT_RATE = 0.001  # 最低互动率阈值
    
    def classify_throttling(row):
        play_count = row['播放量']
        like_count = row['点赞']
        comment_count = row['评论']
        collect_count = row['收藏']
        
        # 计算总互动数
        total_engagement = like_count + comment_count + collect_count
        
        # 计算互动率
        engagement_rate = total_engagement / max(play_count, 1)  # 避免除零
        
        # 明确限流判断
        if play_count <= CLEAR_THROTTLING_THRESHOLD and total_engagement <= 5:
            return "明确限流"
        
        # 疑似限流判断
        elif play_count <= SUSPECTED_THROTTLING_THRESHOLD:
            # 如果播放量低但互动率正常，可能是限流
            if engagement_rate < MIN_ENGAGEMENT_RATE:
                return "疑似限流"
            else:
                return "正常"
        
        # 异常情况：播放量正常但互动异常低
        elif play_count > SUSPECTED_THROTTLING_THRESHOLD and engagement_rate < MIN_ENGAGEMENT_RATE * 0.1:
            return "疑似限流"
        
        else:
            return "正常"
    
    # 应用限流检测
    df_with_throttling['限流状态'] = df_with_throttling.apply(classify_throttling, axis=1)
    
    # 计算互动率用于显示
    df_with_throttling['互动率'] = (df_with_throttling['点赞'] + df_with_throttling['评论'] + df_with_throttling['收藏']) / df_with_throttling['播放量'].replace(0, 1)
    
    return df_with_throttling

def generate_account_throttling_list(df: pd.DataFrame) -> pd.DataFrame:
    """生成账号级别的限流名单"""
    account_throttling_list = []
    
    # 账号限流判断阈值
    ACCOUNT_CLEAR_THROTTLING_THRESHOLD = 0.6  # 60%以上视频被明确限流
    ACCOUNT_SUSPECTED_THROTTLING_THRESHOLD = 0.4  # 40%以上视频被疑似限流
    ACCOUNT_TOTAL_THROTTLING_THRESHOLD = 0.5  # 50%以上视频被限流（明确+疑似）
    
    # 按账号分组分析
    for username in df['账号'].unique():
        user_data = df[df['账号'] == username].copy()
        
        if len(user_data) == 0:
            continue
        
        # 基础信息
        user_info = user_data.iloc[0]
        total_videos = len(user_data)
        
        # 限流统计
        throttling_stats = user_data['限流状态'].value_counts()
        clear_throttling = throttling_stats.get('明确限流', 0)
        suspected_throttling = throttling_stats.get('疑似限流', 0)
        normal_videos = throttling_stats.get('正常', 0)
        
        # 计算比例
        clear_throttling_rate = clear_throttling / total_videos
        suspected_throttling_rate = suspected_throttling / total_videos
        total_throttling_rate = (clear_throttling + suspected_throttling) / total_videos
        
        # 判断账号限流状态
        if clear_throttling_rate >= ACCOUNT_CLEAR_THROTTLING_THRESHOLD:
            account_status = "明确限流"
            risk_level = "🔴 高风险"
        elif total_throttling_rate >= ACCOUNT_TOTAL_THROTTLING_THRESHOLD:
            account_status = "疑似限流"
            risk_level = "🟡 中风险"
        elif suspected_throttling_rate >= ACCOUNT_SUSPECTED_THROTTLING_THRESHOLD:
            account_status = "疑似限流"
            risk_level = "🟡 中风险"
        else:
            account_status = "正常"
            risk_level = "🟢 低风险"
        
        # 计算平均数据
        avg_play_count = user_data['播放量'].mean()
        avg_engagement_rate = user_data['互动率'].mean()
        
        account_throttling_list.append({
            '账号': username,
            '昵称': user_info['昵称'],
            '粉丝数': user_info['粉丝数'],
            '账号状态': account_status,
            '风险等级': risk_level,
            '视频总数': total_videos,
            '明确限流数': clear_throttling,
            '疑似限流数': suspected_throttling,
            '正常视频数': normal_videos,
            '明确限流率': f"{clear_throttling_rate*100:.1f}%",
            '疑似限流率': f"{suspected_throttling_rate*100:.1f}%",
            '总限流率': f"{total_throttling_rate*100:.1f}%",
            '平均播放量': int(avg_play_count),
            '平均互动率': f"{avg_engagement_rate:.4f}",
            '限流原因分析': get_throttling_reason(clear_throttling_rate, suspected_throttling_rate, total_throttling_rate, avg_play_count, avg_engagement_rate)
        })
    
    return pd.DataFrame(account_throttling_list)

def get_throttling_reason(clear_rate, suspected_rate, total_rate, avg_play, avg_engagement):
    """分析限流原因"""
    reasons = []
    
    if clear_rate >= 0.6:
        reasons.append("大量视频播放量极低")
    if suspected_rate >= 0.4:
        reasons.append("多数视频互动率异常")
    if avg_play < 100:
        reasons.append("整体播放量偏低")
    if avg_engagement < 0.01:
        reasons.append("互动率严重不足")
    
    if not reasons:
        return "数据正常"
    
    return " | ".join(reasons)

def calculate_analytics(df: pd.DataFrame) -> pd.DataFrame:
    """计算各种分析指标"""
    analytics_data = []
    
    # 按账号分组计算指标
    for username in df['账号'].unique():
        user_data = df[df['账号'] == username].copy()
        
        if len(user_data) == 0:
            continue
            
        # 基础数据
        user_info = user_data.iloc[0]
        followers = user_info['粉丝数'] if user_info['粉丝数'] > 0 else 1  # 避免除零
        
        # 1️⃣ 内容质量指标 - 互动率
        user_data['总互动'] = user_data['点赞'] + user_data['评论'] + user_data['收藏']
        user_data['互动率'] = user_data['总互动'] / (user_data['播放量'] + 1)  # 避免除零
        avg_engagement_rate = user_data['互动率'].mean()
        
        # 2️⃣ 账号影响力 - 单位粉丝互动效率
        total_interactions = user_data['总互动'].sum()
        fan_efficiency = total_interactions / followers
        
        # 3️⃣ 内容稳定性 - 播放量变化系数
        play_counts = user_data['播放量']
        if len(play_counts) > 1 and play_counts.mean() > 0:
            play_stability = play_counts.std() / play_counts.mean()
        else:
            play_stability = 0
        
        # 4️⃣ 增长趋势 - 近期播放增长率
        if len(user_data) >= 4:
            # 按发布时间排序（最新的在前）
            user_data_sorted = user_data.sort_values('发布时间', ascending=False)
            latest_2 = user_data_sorted.head(2)['播放量'].mean()
            earliest_2 = user_data_sorted.tail(2)['播放量'].mean()
            if earliest_2 > 0:
                growth_trend = (latest_2 - earliest_2) / earliest_2
            else:
                growth_trend = 0
        else:
            growth_trend = 0
        
        # 5️⃣ 转化深度 - 深度互动比例
        deep_interactions = user_data['评论'].sum() + user_data['收藏'].sum()
        total_interactions_for_depth = user_data['点赞'].sum() + user_data['评论'].sum() + user_data['收藏'].sum()
        if total_interactions_for_depth > 0:
            deep_engagement = deep_interactions / total_interactions_for_depth
        else:
            deep_engagement = 0
        
        # 限流统计
        throttling_stats = user_data['限流状态'].value_counts()
        clear_throttling = throttling_stats.get('明确限流', 0)
        suspected_throttling = throttling_stats.get('疑似限流', 0)
        normal_videos = throttling_stats.get('正常', 0)
        
        analytics_data.append({
            '账号': username,
            '昵称': user_info['昵称'],
            '粉丝数': user_info['粉丝数'],
            '视频数量': len(user_data),
            '平均播放量': user_data['播放量'].mean(),
            '互动率': avg_engagement_rate,
            '粉丝互动效率': fan_efficiency,
            '内容稳定性': play_stability,
            '增长趋势': growth_trend,
            '深度互动比例': deep_engagement,
            '明确限流数': clear_throttling,
            '疑似限流数': suspected_throttling,
            '正常视频数': normal_videos,
            '限流比例': (clear_throttling + suspected_throttling) / len(user_data) if len(user_data) > 0 else 0
        })
    
    return pd.DataFrame(analytics_data)

def get_metric_explanation(metric_name: str) -> str:
    """获取指标解释"""
    explanations = {
        '互动率': "互动率 = (点赞 + 评论 + 收藏) / 播放量\n💡 直接反映内容对观众的吸引力，是最核心的质量指标。一般来说：\n• 优秀: >5%\n• 良好: 2-5%\n• 一般: 1-2%\n• 较差: <1%",
        '粉丝互动效率': "粉丝互动效率 = 总互动数 / 粉丝数\n💡 衡量粉丝质量，区分僵尸粉vs活跃粉。数值越高说明粉丝越活跃：\n• 优秀: >0.5\n• 良好: 0.2-0.5\n• 一般: 0.1-0.2\n• 较差: <0.1",
        '内容稳定性': "内容稳定性 = 播放量标准差 / 播放量均值\n💡 低变化系数说明内容质量稳定，商业价值更高：\n• 优秀: <0.5 (很稳定)\n• 良好: 0.5-1.0 (较稳定)\n• 一般: 1.0-2.0 (波动较大)\n• 较差: >2.0 (极不稳定)",
        '增长趋势': "增长趋势 = (最新2条均值 - 最早2条均值) / 最早2条均值\n💡 判断账号是上升期还是衰退期：\n• 强势增长: >50%\n• 稳定增长: 10-50%\n• 平稳发展: -10%-10%\n• 下降趋势: <-10%",
        '深度互动比例': "深度互动比例 = (评论 + 收藏) / (点赞 + 评论 + 收藏)\n💡 衡量用户参与深度，比例越高说明内容越有价值：\n• 优秀: >30%\n• 良好: 20-30%\n• 一般: 10-20%\n• 较差: <10%"
    }
    return explanations.get(metric_name, "暂无解释")

def display_analytics_section(df: pd.DataFrame):
    """显示数据分析部分"""
    st.markdown("---")
    st.header("📊 数据分析")
    
    # 先进行限流检测
    df_with_throttling = detect_throttling(df)
    
    # 计算分析指标
    analytics_df = calculate_analytics(df_with_throttling)
    
    if analytics_df.empty:
        st.warning("没有足够的数据进行分析")
        return
    
    # 生成账号限流名单
    account_throttling_df = generate_account_throttling_list(df_with_throttling)
    
    # 账号限流名单概览
    st.subheader("🚨 账号限流名单")
    
    # 账号级别统计
    account_summary = account_throttling_df['账号状态'].value_counts()
    total_accounts = len(account_throttling_df)
    
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        clear_accounts = account_summary.get('明确限流', 0)
        st.metric("明确限流账号", clear_accounts, f"{clear_accounts/total_accounts*100:.1f}%")
    with col2:
        suspected_accounts = account_summary.get('疑似限流', 0)
        st.metric("疑似限流账号", suspected_accounts, f"{suspected_accounts/total_accounts*100:.1f}%")
    with col3:
        normal_accounts = account_summary.get('正常', 0)
        st.metric("正常账号", normal_accounts, f"{normal_accounts/total_accounts*100:.1f}%")
    with col4:
        throttled_accounts = clear_accounts + suspected_accounts
        st.metric("账号总限流率", f"{throttled_accounts/total_accounts*100:.1f}%", 
                 f"{throttled_accounts}/{total_accounts}")
    
    # 账号限流名单表格
    st.subheader("📋 账号限流详细名单")
    
    # 筛选选项
    account_filter = st.selectbox(
        "筛选账号状态",
        ["全部", "明确限流", "疑似限流", "正常"],
        index=0,
        key="account_filter"
    )
    
    # 根据筛选显示数据
    if account_filter != "全部":
        filtered_account_df = account_throttling_df[account_throttling_df['账号状态'] == account_filter]
    else:
        filtered_account_df = account_throttling_df
    
    # 按风险等级排序
    risk_order = {'🔴 高风险': 0, '🟡 中风险': 1, '🟢 低风险': 2}
    filtered_account_df = filtered_account_df.copy()
    filtered_account_df['风险排序'] = filtered_account_df['风险等级'].map(risk_order)
    filtered_account_df = filtered_account_df.sort_values(['风险排序', '总限流率'], ascending=[True, False])
    
    # 显示账号名单
    display_account_columns = ['账号', '昵称', '粉丝数', '账号状态', '风险等级', '视频总数', 
                              '明确限流数', '疑似限流数', '总限流率', '平均播放量', '限流原因分析']
    st.dataframe(
        filtered_account_df[display_account_columns],
        use_container_width=True
    )
    
    # 限流账号快速导出
    if throttled_accounts > 0:
        st.subheader("⚡ 限流账号快速名单")
        
        # 只显示限流账号
        throttled_only = account_throttling_df[account_throttling_df['账号状态'].isin(['明确限流', '疑似限流'])]
        throttled_only = throttled_only.sort_values(['风险等级', '总限流率'], ascending=[True, False])
        
        # 简化显示
        simple_columns = ['账号', '昵称', '账号状态', '风险等级', '总限流率', '限流原因分析']
        st.dataframe(
            throttled_only[simple_columns],
            use_container_width=True
        )
        
        # 生成纯文本名单
        st.subheader("📝 纯文本限流名单")
        throttled_list_text = "\n".join([
            f"• {row['账号']} ({row['昵称']}) - {row['账号状态']} - {row['总限流率']} - {row['限流原因分析']}"
            for _, row in throttled_only.iterrows()
        ])
        st.text_area("复制以下限流账号名单：", throttled_list_text, height=200)
    
    # 视频级别限流统计概览
    st.subheader("📊 视频级别限流概览")
    
    # 整体限流统计
    throttling_summary = df_with_throttling['限流状态'].value_counts()
    total_videos = len(df_with_throttling)
    
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        clear_count = throttling_summary.get('明确限流', 0)
        st.metric("明确限流视频", clear_count, f"{clear_count/total_videos*100:.1f}%")
    with col2:
        suspected_count = throttling_summary.get('疑似限流', 0)
        st.metric("疑似限流视频", suspected_count, f"{suspected_count/total_videos*100:.1f}%")
    with col3:
        normal_count = throttling_summary.get('正常', 0)
        st.metric("正常视频", normal_count, f"{normal_count/total_videos*100:.1f}%")
    with col4:
        throttled_total = clear_count + suspected_count
        st.metric("视频总限流率", f"{throttled_total/total_videos*100:.1f}%", 
                 f"{throttled_total}/{total_videos}")
    
    # 限流详情表格
    st.subheader("📋 限流详情")
    
    # 筛选选项
    filter_option = st.selectbox(
        "筛选视频状态",
        ["全部", "明确限流", "疑似限流", "正常"],
        index=0
    )
    
    # 根据筛选显示数据
    if filter_option != "全部":
        filtered_df = df_with_throttling[df_with_throttling['限流状态'] == filter_option]
    else:
        filtered_df = df_with_throttling
    
    # 显示筛选后的数据
    display_columns = ['账号', '昵称', '发布时间', '播放量', '点赞', '评论', '收藏', '互动率', '限流状态']
    st.dataframe(
        filtered_df[display_columns].round({'互动率': 4}),
        use_container_width=True
    )
    
    # 创建标签页
    tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
        "1️⃣ 内容质量指标", 
        "2️⃣ 账号影响力", 
        "3️⃣ 内容稳定性", 
        "4️⃣ 增长趋势", 
        "5️⃣ 转化深度",
        "6️⃣ 限流分析"
    ])
    
    with tab1:
        st.subheader("互动率分析")
        col1, col2 = st.columns([2, 1])
        
        with col1:
            # 排序并显示
            sorted_df = analytics_df.sort_values('互动率', ascending=False)
            st.dataframe(
                sorted_df[['账号', '昵称', '互动率', '平均播放量']].round(4),
                use_container_width=True
            )
        
        with col2:
            st.info(get_metric_explanation('互动率'))
            
        # 可视化
        if len(analytics_df) > 1:
            st.bar_chart(
                analytics_df.set_index('账号')['互动率'],
                use_container_width=True
            )
    
    with tab2:
        st.subheader("粉丝互动效率分析")
        col1, col2 = st.columns([2, 1])
        
        with col1:
            sorted_df = analytics_df.sort_values('粉丝互动效率', ascending=False)
            st.dataframe(
                sorted_df[['账号', '昵称', '粉丝互动效率', '粉丝数']].round(4),
                use_container_width=True
            )
        
        with col2:
            st.info(get_metric_explanation('粉丝互动效率'))
            
        if len(analytics_df) > 1:
            st.bar_chart(
                analytics_df.set_index('账号')['粉丝互动效率'],
                use_container_width=True
            )
    
    with tab3:
        st.subheader("内容稳定性分析")
        col1, col2 = st.columns([2, 1])
        
        with col1:
            sorted_df = analytics_df.sort_values('内容稳定性', ascending=True)  # 越小越好
            st.dataframe(
                sorted_df[['账号', '昵称', '内容稳定性', '视频数量']].round(4),
                use_container_width=True
            )
        
        with col2:
            st.info(get_metric_explanation('内容稳定性'))
            
        if len(analytics_df) > 1:
            st.bar_chart(
                analytics_df.set_index('账号')['内容稳定性'],
                use_container_width=True
            )
    
    with tab4:
        st.subheader("增长趋势分析")
        col1, col2 = st.columns([2, 1])
        
        with col1:
            sorted_df = analytics_df.sort_values('增长趋势', ascending=False)
            # 格式化为百分比
            display_df = sorted_df[['账号', '昵称', '增长趋势', '视频数量']].copy()
            display_df['增长趋势'] = (display_df['增长趋势'] * 100).round(2)
            st.dataframe(display_df, use_container_width=True)
        
        with col2:
            st.info(get_metric_explanation('增长趋势'))
            
        if len(analytics_df) > 1:
            st.bar_chart(
                analytics_df.set_index('账号')['增长趋势'],
                use_container_width=True
            )
    
    with tab5:
        st.subheader("转化深度分析")
        col1, col2 = st.columns([2, 1])
        
        with col1:
            sorted_df = analytics_df.sort_values('深度互动比例', ascending=False)
            # 格式化为百分比
            display_df = sorted_df[['账号', '昵称', '深度互动比例', '视频数量']].copy()
            display_df['深度互动比例'] = (display_df['深度互动比例'] * 100).round(2)
            st.dataframe(display_df, use_container_width=True)
        
        with col2:
            st.info(get_metric_explanation('深度互动比例'))
            
        if len(analytics_df) > 1:
            st.bar_chart(
                analytics_df.set_index('账号')['深度互动比例'],
                use_container_width=True
            )
    
    with tab6:
        st.subheader("限流状况分析")
        col1, col2 = st.columns([2, 1])
        
        with col1:
            # 按账号显示限流统计
            throttling_analysis = analytics_df[['账号', '昵称', '视频数量', '明确限流数', '疑似限流数', '正常视频数', '限流比例']].copy()
            throttling_analysis['限流比例'] = (throttling_analysis['限流比例'] * 100).round(2)
            throttling_analysis = throttling_analysis.sort_values('限流比例', ascending=False)
            st.dataframe(throttling_analysis, use_container_width=True)
        
        with col2:
            st.info("""
            **限流检测规则：**
            
            **明确限流：**
            • 播放量 ≤ 10
            • 总互动数 ≤ 5
            
            **疑似限流：**
            • 播放量 ≤ 50 且互动率过低
            • 播放量正常但互动率异常低
            
            **正常：**
            • 播放量和互动率都在正常范围
            """)
        
        # 限流趋势图
        if len(analytics_df) > 1:
            st.subheader("各账号限流比例对比")
            st.bar_chart(
                analytics_df.set_index('账号')['限流比例'],
                use_container_width=True
            )
    
    # 综合分析报告
    st.markdown("---")
    st.subheader("📋 综合分析报告")
    
    # 找出各项指标的最佳账号
    best_engagement = analytics_df.loc[analytics_df['互动率'].idxmax()]
    best_fan_efficiency = analytics_df.loc[analytics_df['粉丝互动效率'].idxmax()]
    best_stability = analytics_df.loc[analytics_df['内容稳定性'].idxmin()]  # 越小越好
    best_growth = analytics_df.loc[analytics_df['增长趋势'].idxmax()]
    best_depth = analytics_df.loc[analytics_df['深度互动比例'].idxmax()]
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.metric(
            "🏆 最佳互动率",
            f"{best_engagement['昵称']} ({best_engagement['账号']})",
            f"{best_engagement['互动率']:.4f}"
        )
        st.metric(
            "🎯 最佳粉丝效率",
            f"{best_fan_efficiency['昵称']} ({best_fan_efficiency['账号']})",
            f"{best_fan_efficiency['粉丝互动效率']:.4f}"
        )
    
    with col2:
        st.metric(
            "📈 最稳定内容",
            f"{best_stability['昵称']} ({best_stability['账号']})",
            f"{best_stability['内容稳定性']:.4f}"
        )
        st.metric(
            "🚀 最佳增长",
            f"{best_growth['昵称']} ({best_growth['账号']})",
            f"{best_growth['增长趋势']*100:.2f}%"
        )
    
    with col3:
        st.metric(
            "💎 最深度互动",
            f"{best_depth['昵称']} ({best_depth['账号']})",
            f"{best_depth['深度互动比例']*100:.2f}%"
        )

def process_usernames(usernames: List[str], video_limit: int, sleep_time: float) -> pd.DataFrame:
    """处理用户名列表，抓取所有数据"""
    all_data = []
    progress_bar = st.progress(0)
    status_text = st.empty()
    
    # 创建可折叠的日志区域
    with st.expander("📋 查看详细抓取日志", expanded=False):
        log_container = st.container()
        log_container.info("开始抓取数据...")
    
    # 统计信息
    success_count = 0
    error_count = 0
    
    for idx, username in enumerate(usernames):
        # 更新进度
        progress = (idx + 1) / len(usernames)
        progress_bar.progress(progress)
        status_text.text(f"正在抓取第 {idx + 1}/{len(usernames)} 个账号: {username} (成功: {success_count}, 失败: {error_count})")
        
        # 抓取数据
        user_data = fetch_user_videos(username, video_limit, log_container)
        
        if user_data:
            all_data.extend(user_data)
            success_count += 1
        else:
            error_count += 1
        
        # 延时
        if idx < len(usernames) - 1:  # 最后一个不需要延时
            time.sleep(sleep_time)
    
    # 最终统计
    with log_container:
        st.success(f"🎉 抓取完成！成功: {success_count}个账号, 失败: {error_count}个账号, 总计获取: {len(all_data)}条视频数据")
    
    status_text.text(f"抓取完成！成功: {success_count}个账号, 失败: {error_count}个账号")
    return pd.DataFrame(all_data)

def main():
    st.title("📊 TikTok 账号价值分析工具")
    st.markdown("---")
    
    # 侧边栏配置
    st.sidebar.header("⚙️ 配置参数")
    
    # 参数配置
    video_limit = st.sidebar.number_input(
        "每个账号抓取视频数量", 
        min_value=1, 
        max_value=50, 
        value=3, 
        help="建议不要设置太高，避免被限速"
    )
    
    sleep_time = st.sidebar.number_input(
        "请求间隔时间（秒）", 
        min_value=0.5, 
        max_value=10.0, 
        value=1.5, 
        step=0.1,
        help="设置较长的间隔可以避免被限速"
    )
    
    st.sidebar.markdown("---")
    st.sidebar.markdown("### 📋 使用说明")
    st.sidebar.markdown("""
    1. 上传包含username的 CSV 或 Excel 文件
    例如：username   表头要命名为username！！！
    2. 配置抓取参数
    3. 点击开始抓取
    4. 下载结果文件
    """)
    
    # 主界面
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.subheader("📁 上传用户名文件")
        uploaded_file = st.file_uploader(
            "选择 CSV 或 Excel 文件", 
            type=['csv', 'xlsx', 'xls'],
            help="文件应包含一列名为 'username' 的用户名数据"
        )
        
        if uploaded_file is not None:
            try:
                # 根据文件类型读取文件
                file_extension = uploaded_file.name.split('.')[-1].lower()
                
                if file_extension == 'csv':
                    df_users = pd.read_csv(uploaded_file)
                elif file_extension in ['xlsx', 'xls']:
                    df_users = pd.read_excel(uploaded_file)
                else:
                    st.error("❌ 不支持的文件格式")
                    return
                
                # 验证文件格式
                if 'username' not in df_users.columns:
                    st.error("❌ 文件必须包含 'username' 列")
                    return
                
                usernames = df_users['username'].dropna().tolist()
                
                if not usernames:
                    st.error("❌ 没有找到有效的用户名")
                    return
                
                st.success(f"✅ 成功加载 {len(usernames)} 个用户名")
                
                # 显示用户名预览
                st.subheader("👀 用户名预览")
                preview_df = pd.DataFrame({'用户名': usernames[:10]})
                st.dataframe(preview_df, use_container_width=True)
                
                if len(usernames) > 10:
                    st.info(f"只显示前10个用户名，总共有 {len(usernames)} 个")
                
            except Exception as e:
                st.error(f"❌ 文件读取失败: {e}")
                return
    
    with col2:
        st.subheader("📊 抓取统计")
        if uploaded_file is not None and 'usernames' in locals():
            st.metric("用户总数", len(usernames))
            st.metric("预计视频数", len(usernames) * video_limit)
            estimated_time = len(usernames) * sleep_time / 60
            st.metric("预计耗时", f"{estimated_time:.1f} 分钟")
    
    # 开始抓取按钮
    if uploaded_file is not None and 'usernames' in locals():
        st.markdown("---")
        
        col1, col2, col3 = st.columns([1, 2, 1])
        with col2:
            if st.button("🚀 开始抓取数据", type="primary", use_container_width=True):
                start_time = time.time()
                
                with st.spinner("正在抓取数据，请稍候..."):
                    result_df = process_usernames(usernames, video_limit, sleep_time)
                
                end_time = time.time()
                duration = end_time - start_time
                
                if not result_df.empty:
                    st.success(f"🎉 抓取完成！耗时 {duration:.1f} 秒，共获取 {len(result_df)} 条视频数据")
                    
                    # 显示结果预览
                    st.subheader("📋 数据预览")
                    st.dataframe(result_df.head(10), use_container_width=True)
                    
                    # 统计信息
                    st.subheader("📈 统计信息")
                    col1, col2, col3, col4 = st.columns(4)
                    
                    with col1:
                        st.metric("总视频数", len(result_df))
                    with col2:
                        st.metric("总播放量", f"{result_df['播放量'].sum():,}")
                    with col3:
                        st.metric("总点赞数", f"{result_df['点赞'].sum():,}")
                    with col4:
                        st.metric("总评论数", f"{result_df['评论'].sum():,}")
                    
                    # 用户统计信息
                    st.subheader("👥 用户统计")
                    col1, col2, col3, col4 = st.columns(4)
                    
                    # 去重统计用户信息
                    unique_users = result_df.drop_duplicates(subset=['账号'])
                    
                    with col1:
                        st.metric("总粉丝数", f"{unique_users['粉丝数'].sum():,}")
                    with col2:
                        st.metric("总关注数", f"{unique_users['关注数'].sum():,}")
                    with col3:
                        st.metric("总获赞数", f"{unique_users['获赞数'].sum():,}")
                    with col4:
                        st.metric("平均粉丝数", f"{unique_users['粉丝数'].mean():.0f}")
                    
                    # 数据分析部分
                    display_analytics_section(result_df)
                    
                    # 生成下载文件
                    excel_buffer = io.BytesIO()
                    with pd.ExcelWriter(excel_buffer, engine='openpyxl') as writer:
                        # 添加限流检测
                        result_df_with_throttling = detect_throttling(result_df)
                        result_df_with_throttling.to_excel(writer, index=False, sheet_name='原始数据')
                        
                        # 添加分析数据
                        analytics_df = calculate_analytics(result_df_with_throttling)
                        if not analytics_df.empty:
                            analytics_df.to_excel(writer, index=False, sheet_name='数据分析')
                        
                        # 添加限流统计
                        throttling_stats = result_df_with_throttling.groupby(['账号', '限流状态']).size().unstack(fill_value=0)
                        throttling_stats.to_excel(writer, sheet_name='视频限流统计')
                        
                        # 添加账号限流名单
                        account_throttling_df = generate_account_throttling_list(result_df_with_throttling)
                        account_throttling_df.to_excel(writer, index=False, sheet_name='账号限流名单')
                    
                    excel_data = excel_buffer.getvalue()
                    
                    # 下载按钮
                    st.markdown("---")
                    col1, col2, col3 = st.columns([1, 2, 1])
                    with col2:
                        st.download_button(
                            label="⬇️ 下载 Excel 文件",
                            data=excel_data,
                            file_name=f"tiktok_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                            type="primary",
                            use_container_width=True
                        )
                
                else:
                    st.warning("⚠️ 没有抓取到任何数据，请检查用户名是否正确或网络连接")
    
    # 底部信息
    st.markdown("---")
    st.markdown(
        """
        <div style='text-align: center; color: #666;'>
        💡 提示：如果遇到限速问题，请适当增加请求间隔时间
        </div>
        """, 
        unsafe_allow_html=True
    )

if __name__ == "__main__":
    main()