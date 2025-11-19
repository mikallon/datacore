#!/usr/bin/env python3
"""
测试 MetricFlow Python API 集成
"""
import sys
import os
sys.path.insert(0, 'web_ui/backend')

from metricflow_client import MetricFlowClient
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

def test_metricflow_api():
    """测试 MetricFlow Python API"""
    project_dir = os.path.abspath('.')
    
    print("=" * 80)
    print("测试 MetricFlow Python API")
    print("=" * 80)
    
    # 初始化客户端
    client = MetricFlowClient(project_dir=project_dir, profiles_dir=project_dir)
    
    # 测试列出指标
    print("\n1. 测试列出指标...")
    metrics = client.list_metrics()
    print(f"   找到 {len(metrics)} 个指标:")
    for metric in metrics[:5]:  # 只显示前5个
        print(f"   - {metric['name']}: {metric.get('description', '')}")
    
    # 测试生成 SQL
    print("\n2. 测试生成 SQL...")
    # MetricFlow 需要使用完整的维度路径
    result = client.generate_sql(
        metrics=['daily_revenue'],
        group_by=['metric_time__day', 'transaction__city'],  # 使用 MetricFlow 的维度路径
        start_time='2025-01-01',
        end_time='2025-01-07'
    )
    
    if result['success']:
        print("   ✅ SQL 生成成功")
        print(f"   生成的 SQL (前200字符):")
        print(f"   {result['sql'][:200]}...")
    else:
        print(f"   ❌ SQL 生成失败: {result.get('error', '未知错误')}")
    
    print("\n" + "=" * 80)
    print("测试完成")
    print("=" * 80)

if __name__ == '__main__':
    test_metricflow_api()

