"""
MetricFlow 客户端模块
优先使用 MetricFlow Python API 生成 SQL 查询，失败时回退到命令行工具
"""
import subprocess
import json
import os
import tempfile
from pathlib import Path
from typing import List, Optional, Dict, Any
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

# 尝试导入 MetricFlow Python API
try:
    from dbt_metricflow.cli.dbt_connectors.dbt_config_accessor import dbtProjectMetadata, dbtArtifacts
    from dbt_metricflow.cli.dbt_connectors.adapter_backed_client import AdapterBackedSqlClient
    from metricflow_semantics.model.semantic_manifest_lookup import SemanticManifestLookup
    from metricflow.engine.metricflow_engine import MetricFlowEngine, MetricFlowQueryRequest
    METRICFLOW_API_AVAILABLE = True
except ImportError as e:
    logger.warning(f"MetricFlow Python API 不可用，将使用命令行工具: {e}")
    METRICFLOW_API_AVAILABLE = False

class MetricFlowClient:
    """MetricFlow 客户端，用于生成 SQL 查询"""
    
    def __init__(self, project_dir: str, profiles_dir: str = "."):
        """
        初始化 MetricFlow 客户端
        
        Args:
            project_dir: dbt 项目根目录
            profiles_dir: dbt profiles 目录
        """
        self.project_dir = os.path.abspath(project_dir)
        self.profiles_dir = os.path.abspath(profiles_dir)
        self._mf_engine: Optional[MetricFlowEngine] = None
        self._use_api = METRICFLOW_API_AVAILABLE
        
    def _init_mf_engine(self) -> Optional[MetricFlowEngine]:
        """初始化 MetricFlowEngine（使用 Python API）"""
        if not self._use_api:
            return None
            
        if self._mf_engine is not None:
            return self._mf_engine
            
        try:
            # 检查 semantic_manifest.json 是否存在
            manifest_path = Path(self.project_dir) / "target" / "semantic_manifest.json"
            if not manifest_path.exists():
                logger.warning(f"semantic_manifest.json 不存在，请先运行 `dbt parse` 或 `dbt build`")
                logger.info("回退到命令行工具模式")
                self._use_api = False
                return None
            
            # 加载 dbt 项目元数据
            project_metadata = dbtProjectMetadata.load_from_paths(
                profiles_path=Path(self.profiles_dir),
                project_path=Path(self.project_dir)
            )
            
            # 加载 dbt artifacts（包括 adapter 和 semantic_manifest）
            dbt_artifacts = dbtArtifacts.load_from_project_metadata(project_metadata)
            
            # 创建 SemanticManifestLookup
            semantic_manifest_lookup = SemanticManifestLookup(dbt_artifacts.semantic_manifest)
            
            # 创建 SqlClient（使用 dbt adapter）
            sql_client = AdapterBackedSqlClient(dbt_artifacts.adapter)
            
            # 初始化 MetricFlowEngine
            self._mf_engine = MetricFlowEngine(
                semantic_manifest_lookup=semantic_manifest_lookup,
                sql_client=sql_client
            )
            
            logger.info("✅ MetricFlow Python API 初始化成功")
            return self._mf_engine
            
        except Exception as e:
            logger.warning(f"MetricFlow Python API 初始化失败: {e}")
            logger.info("回退到命令行工具模式")
            self._use_api = False
            return None
        
    def list_metrics(self) -> List[Dict[str, Any]]:
        """
        列出所有可用的指标
        
        Returns:
            指标列表
        """
        # 尝试使用 Python API
        mf_engine = self._init_mf_engine()
        if mf_engine is not None:
            try:
                # 使用 MetricFlowEngine 的 list_metrics 方法
                metrics = mf_engine.list_metrics()
                return [
                    {
                        'name': metric.name,  # metric.name 是字符串
                        'label': metric.label or metric.name,
                        'description': metric.description or ''
                    }
                    for metric in metrics
                ]
            except Exception as e:
                logger.warning(f"使用 Python API 列出指标失败: {e}，回退到命令行工具")
                import traceback
                logger.debug(traceback.format_exc())
        
        # 回退到命令行工具
        try:
            result = subprocess.run(
                ['mf', 'list', 'metrics'],
                cwd=self.project_dir,
                capture_output=True,
                text=True,
                timeout=30
            )
            
            if result.returncode == 0:
                # MetricFlow 输出文本格式，需要解析
                return self._parse_text_metrics(result.stdout)
            else:
                logger.warning(f"MetricFlow list metrics 失败: {result.stderr}")
                return []
        except Exception as e:
            logger.error(f"列出指标失败: {e}")
            return []
    
    def _parse_text_metrics(self, text: str) -> List[Dict[str, Any]]:
        """解析文本格式的指标列表"""
        metrics = []
        lines = text.split('\n')
        in_metrics_section = False
        
        for line in lines:
            line = line.strip()
            # 查找指标列表开始
            if 'metric' in line.lower() and ('name' in line.lower() or 'available' in line.lower()):
                in_metrics_section = True
                continue
            
            # 跳过空行和分隔线
            if not line or line.startswith('-') or line.startswith('=') or line.startswith('#'):
                continue
            
            # 提取指标名称（可能是表格格式或列表格式）
            if in_metrics_section:
                # 尝试提取指标名称（可能是第一列）
                parts = line.split()
                if parts:
                    metric_name = parts[0].strip()
                    if metric_name and not metric_name.startswith('─'):
                        metrics.append({
                            'name': metric_name,
                            'label': metric_name,
                            'description': ' '.join(parts[1:]) if len(parts) > 1 else ''
                        })
        
        return metrics
    
    def generate_sql(
        self,
        metrics: List[str],
        group_by: Optional[List[str]] = None,
        where: Optional[List[str]] = None,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
        limit: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        使用 MetricFlow 生成 SQL 查询
        
        Args:
            metrics: 指标名称列表
            group_by: 分组维度列表
            where: WHERE 条件列表
            start_time: 开始时间（ISO 8601 格式）
            end_time: 结束时间（ISO 8601 格式）
            limit: 结果限制数量
            
        Returns:
            包含 SQL 和元数据的字典
        """
        # 尝试使用 Python API
        mf_engine = self._init_mf_engine()
        if mf_engine is not None:
            try:
                # 解析时间字符串为 datetime 对象
                time_constraint_start = None
                time_constraint_end = None
                if start_time:
                    try:
                        # 尝试解析 ISO 8601 格式
                        if 'T' in start_time:
                            time_constraint_start = datetime.fromisoformat(start_time.replace('Z', '+00:00'))
                        else:
                            time_constraint_start = datetime.strptime(start_time, '%Y-%m-%d')
                    except ValueError:
                        logger.warning(f"无法解析开始时间: {start_time}")
                
                if end_time:
                    try:
                        if 'T' in end_time:
                            time_constraint_end = datetime.fromisoformat(end_time.replace('Z', '+00:00'))
                        else:
                            time_constraint_end = datetime.strptime(end_time, '%Y-%m-%d')
                    except ValueError:
                        logger.warning(f"无法解析结束时间: {end_time}")
                
                # 构建 MetricFlowQueryRequest
                mf_request = MetricFlowQueryRequest.create_with_random_request_id(
                    metric_names=metrics,
                    group_by_names=group_by,
                    time_constraint_start=time_constraint_start,
                    time_constraint_end=time_constraint_end,
                    where_constraints=where,
                    limit=limit
                )
                
                # 使用 explain 方法生成 SQL（不执行）
                explain_result = mf_engine.explain(mf_request=mf_request)
                
                # 提取 SQL 语句
                sql = explain_result.sql_statement.without_descriptions.sql
                
                logger.info("✅ 使用 MetricFlow Python API 生成 SQL 成功")
                return {
                    'sql': sql,
                    'raw_output': sql,  # 为了兼容性
                    'success': True
                }
                
            except Exception as e:
                logger.warning(f"使用 Python API 生成 SQL 失败: {e}，回退到命令行工具")
                import traceback
                logger.debug(traceback.format_exc())
        
        # 回退到命令行工具
        try:
            # 构建 mf query 命令
            cmd = ['mf', 'query']
            
            # 添加指标
            if metrics:
                cmd.extend(['--metrics', ','.join(metrics)])
            
            # 添加分组维度
            if group_by:
                cmd.extend(['--group-by', ','.join(group_by)])
            
            # 添加时间范围
            if start_time:
                cmd.extend(['--start-time', start_time])
            if end_time:
                cmd.extend(['--end-time', end_time])
            
            # 添加 WHERE 条件
            if where:
                for condition in where:
                    cmd.extend(['--where', condition])
            
            # 添加限制
            if limit:
                cmd.extend(['--limit', str(limit)])
            
            # 使用 --explain 获取 SQL（不执行查询）
            cmd.append('--explain')
            
            # 执行命令
            result = subprocess.run(
                cmd,
                cwd=self.project_dir,
                capture_output=True,
                text=True,
                timeout=60
            )
            
            if result.returncode == 0:
                # 解析输出，提取 SQL
                output = result.stdout
                
                # MetricFlow 的 --explain 输出包含 SQL
                # 尝试提取 SQL 部分
                sql = self._extract_sql_from_explain(output)
                
                return {
                    'sql': sql,
                    'raw_output': output,
                    'success': True
                }
            else:
                error_msg = result.stderr or result.stdout
                logger.error(f"MetricFlow 生成 SQL 失败: {error_msg}")
                return {
                    'sql': None,
                    'error': error_msg,
                    'success': False
                }
                
        except subprocess.TimeoutExpired:
            logger.error("MetricFlow 查询超时")
            return {
                'sql': None,
                'error': '查询超时',
                'success': False
            }
        except Exception as e:
            logger.error(f"生成 SQL 失败: {e}")
            return {
                'sql': None,
                'error': str(e),
                'success': False
            }
    
    def _extract_sql_from_explain(self, output: str) -> str:
        """
        从 MetricFlow explain 输出中提取 SQL
        
        Args:
            output: MetricFlow 的输出文本
            
        Returns:
            提取的 SQL 语句
        """
        # MetricFlow 的 explain 输出可能包含 SQL 或查询计划
        # 尝试多种方式提取 SQL
        
        # 方式1: 查找 SQL 代码块（```sql ... ```）
        import re
        sql_block_match = re.search(r'```(?:sql)?\s*\n(.*?)\n```', output, re.DOTALL | re.IGNORECASE)
        if sql_block_match:
            return sql_block_match.group(1).strip()
        
        # 方式2: 查找 SELECT 语句
        lines = output.split('\n')
        sql_lines = []
        in_sql = False
        
        for line in lines:
            line_upper = line.upper().strip()
            # 查找 SQL 开始标记
            if 'SELECT' in line_upper or (in_sql and line_upper):
                in_sql = True
                sql_lines.append(line)
                # 检查是否结束（遇到某些标记）
                if line_upper.startswith('--') and 'END' in line_upper:
                    break
            elif in_sql and not line.strip():
                # 空行可能表示 SQL 结束
                if len(sql_lines) > 3:  # 确保有足够的 SQL 行
                    break
        
        if sql_lines:
            sql = '\n'.join(sql_lines).strip()
            # 清理可能的注释
            sql = re.sub(r'^--.*$', '', sql, flags=re.MULTILINE)
            return sql.strip()
        
        # 方式3: 如果没有找到，返回整个输出（可能已经是 SQL）
        return output.strip()
    
    def query_metrics(
        self,
        metrics: List[str],
        group_by: Optional[List[str]] = None,
        where: Optional[List[str]] = None,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
        limit: Optional[int] = None,
        output_format: str = 'json'
    ) -> Dict[str, Any]:
        """
        查询指标数据（生成 SQL 并执行）
        
        注意：此方法只生成 SQL，实际执行需要在应用层完成
        
        Args:
            metrics: 指标名称列表
            group_by: 分组维度列表
            where: WHERE 条件列表
            start_time: 开始时间
            end_time: 结束时间
            limit: 结果限制
            output_format: 输出格式（json/csv）
            
        Returns:
            包含 SQL 和查询信息的字典
        """
        # 生成 SQL
        result = self.generate_sql(
            metrics=metrics,
            group_by=group_by,
            where=where,
            start_time=start_time,
            end_time=end_time,
            limit=limit
        )
        
        if result['success']:
            return {
                'sql': result['sql'],
                'metrics': metrics,
                'group_by': group_by or [],
                'filters': where or [],
                'time_range': {
                    'start': start_time,
                    'end': end_time
                }
            }
        else:
            return result

