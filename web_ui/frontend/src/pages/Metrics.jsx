import React, { useEffect, useState, useRef } from 'react'
import {
  Card,
  Select,
  Button,
  DatePicker,
  Row,
  Col,
  Table,
  Tag,
  Space,
  Spin,
  Alert,
  Form,
  Checkbox,
  Divider,
  Input,
  List,
  Avatar,
  Typography
} from 'antd'
import {
  ReloadOutlined,
  DownloadOutlined,
  LineChartOutlined,
  SendOutlined,
  UserOutlined,
  RobotOutlined
} from '@ant-design/icons'
import api from '../utils/api'
import ReactECharts from 'echarts-for-react'
import dayjs from 'dayjs'
// Ant Design 5 的 DatePicker 返回 dayjs 对象

const { RangePicker } = DatePicker
const { Option } = Select
const { TextArea } = Input
const { Text, Paragraph } = Typography

export default function Metrics() {
  const [loading, setLoading] = useState(false)
  const [metrics, setMetrics] = useState([])
  const [selectedMetric, setSelectedMetric] = useState(null)
  const [dimensions, setDimensions] = useState([])
  const [filters, setFilters] = useState({})
  const [dateRange, setDateRange] = useState(null)
  const [queryResult, setQueryResult] = useState(null)
  const [queryError, setQueryError] = useState(null)
  
  // 聊天相关状态
  const [chatMode, setChatMode] = useState(false) // 是否使用聊天模式
  const [chatInput, setChatInput] = useState('')
  const [chatHistory, setChatHistory] = useState([])
  const [chatLoading, setChatLoading] = useState(false)
  const chatEndRef = useRef(null)
  const chatContainerRef = useRef(null)

  // 可用的维度选项
  const availableDimensions = [
    { value: 'city', label: '城市' },
    { value: 'station_name', label: '收费站' },
    { value: 'vehicle_type_name', label: '车型' },
    { value: 'payment_method_name', label: '支付方式' },
    { value: 'highway_code', label: '高速公路编码' }
  ]

  useEffect(() => {
    loadMetrics()
  }, [])

  const loadMetrics = async () => {
    try {
      const data = await api.get('/metrics')
      setMetrics(data)
      if (data.length > 0) {
        setSelectedMetric(data[0])
      }
    } catch (error) {
      console.error('Failed to load metrics:', error)
    }
  }

  const handleQuery = async () => {
    if (!selectedMetric) {
      setQueryError('请选择指标')
      return
    }

    setLoading(true)
    setQueryError(null)

    try {
      if (!dateRange || !dateRange[0] || !dateRange[1]) {
        setQueryError('请选择时间范围')
        setLoading(false)
        return
      }

      const query = {
        metric_name: selectedMetric.name,
        dimensions: dimensions.length > 0 ? dimensions : undefined,
        filters: Object.keys(filters).length > 0 ? filters : undefined,
        start_date: dateRange[0].format('YYYY-MM-DD'),
        end_date: dateRange[1].format('YYYY-MM-DD')
      }

      const result = await api.post('/metrics/query', query)
      setQueryResult(result)
    } catch (error) {
      console.error('Query failed:', error)
      setQueryError(error.response?.data?.detail || '查询失败')
    } finally {
      setLoading(false)
    }
  }

  // 自然语言查询处理
  const handleNaturalLanguageQuery = async () => {
    if (!chatInput.trim()) {
      return
    }

    const userMessage = chatInput.trim()
    setChatInput('')
    setChatLoading(true)
    setQueryError(null)

    // 添加用户消息到聊天历史
    const newHistory = [...chatHistory, {
      role: 'user',
      content: userMessage,
      timestamp: new Date()
    }]
    setChatHistory(newHistory)

    try {
      const result = await api.post('/metrics/query/natural', {
        query: userMessage,
        use_llm: true, // 使用 LLM 解析
        provider: 'local' // 使用本地 LLM Studio
      })

      // 添加系统回复到聊天历史
      const responseMessage = {
        role: 'assistant',
        content: `已为您查询到 ${result.data?.length || 0} 条数据`,
        data: result,
        timestamp: new Date()
      }
      setChatHistory([...newHistory, responseMessage])

      // 更新查询结果，用于显示图表和表格
      setQueryResult(result)
      
      // 如果有解析的查询信息，更新表单
      if (result.parsed_query) {
        const parsed = result.parsed_query
        if (parsed.metric_name) {
          const metric = metrics.find(m => m.name === parsed.metric_name)
          if (metric) {
            setSelectedMetric(metric)
          }
        }
        if (parsed.dimensions) {
          setDimensions(parsed.dimensions)
        }
        if (parsed.start_date && parsed.end_date) {
          // 将字符串转换为 dayjs 对象
          setDateRange([
            dayjs(parsed.start_date),
            dayjs(parsed.end_date)
          ])
        }
      }

      // 滚动到底部
      setTimeout(() => {
        chatEndRef.current?.scrollIntoView({ behavior: 'smooth' })
      }, 100)

    } catch (error) {
      console.error('Natural language query failed:', error)
      const errorMessage = error.response?.data?.detail || '查询失败，请尝试更清晰的表达'
      
      // 添加错误消息到聊天历史
      setChatHistory([...newHistory, {
        role: 'assistant',
        content: `❌ ${errorMessage}`,
        error: true,
        timestamp: new Date()
      }])
      
      setQueryError(errorMessage)
    } finally {
      setChatLoading(false)
    }
  }

  // 处理回车键
  const handleChatInputKeyPress = (e) => {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault()
      handleNaturalLanguageQuery()
    }
  }

  // 滚动到底部
  useEffect(() => {
    if (chatContainerRef.current) {
      chatContainerRef.current.scrollTop = chatContainerRef.current.scrollHeight
    }
  }, [chatHistory])

  const getChartOption = () => {
    if (!queryResult || !queryResult.data || queryResult.data.length === 0) {
      return null
    }

    const data = queryResult.data
    const hasDimensions = dimensions.length > 0

    if (hasDimensions) {
      // 多维度图表 - 使用堆叠柱状图
      const dimensionValues = {}
      const dates = [...new Set(data.map(d => d.transaction_date))].sort()

      data.forEach(item => {
        const date = item.transaction_date
        const dimKey = dimensions.map(d => item[d]).join(' - ')
        if (!dimensionValues[dimKey]) {
          dimensionValues[dimKey] = {}
        }
        dimensionValues[dimKey][date] = item.metric_value
      })

      const series = Object.keys(dimensionValues).map(dimKey => ({
        name: dimKey,
        type: 'bar',
        stack: 'total',
        data: dates.map(date => dimensionValues[dimKey][date] || 0)
      }))

      return {
        title: {
          text: selectedMetric?.label || '指标趋势',
          left: 'center'
        },
        tooltip: {
          trigger: 'axis',
          axisPointer: { type: 'shadow' }
        },
        legend: {
          data: Object.keys(dimensionValues),
          top: 30
        },
        grid: {
          left: '3%',
          right: '4%',
          bottom: '3%',
          containLabel: true
        },
        xAxis: {
          type: 'category',
          data: dates
        },
        yAxis: {
          type: 'value',
          axisLabel: {
            formatter: (value) => {
              if (selectedMetric?.unit === '元') {
                return (value / 10000).toFixed(1) + '万'
              }
              return value
            }
          }
        },
        series
      }
    } else {
      // 单维度图表 - 使用折线图
      const dates = data.map(d => d.transaction_date).sort()
      const values = dates.map(date => {
        const item = data.find(d => d.transaction_date === date)
        return item ? item.metric_value : 0
      })

      return {
        title: {
          text: selectedMetric?.label || '指标趋势',
          left: 'center'
        },
        tooltip: {
          trigger: 'axis'
        },
        xAxis: {
          type: 'category',
          data: dates
        },
        yAxis: {
          type: 'value',
          axisLabel: {
            formatter: (value) => {
              if (selectedMetric?.unit === '元') {
                return (value / 10000).toFixed(1) + '万'
              }
              if (selectedMetric?.unit === '%') {
                return value.toFixed(1) + '%'
              }
              return value
            }
          }
        },
        series: [
          {
            name: selectedMetric?.label || '指标值',
            type: 'line',
            data: values,
            smooth: true,
            itemStyle: { color: '#1890ff' },
            areaStyle: {
              color: {
                type: 'linear',
                x: 0,
                y: 0,
                x2: 0,
                y2: 1,
                colorStops: [
                  { offset: 0, color: 'rgba(24, 144, 255, 0.3)' },
                  { offset: 1, color: 'rgba(24, 144, 255, 0.1)' }
                ]
              }
            }
          }
        ]
      }
    }
  }

  const getTableColumns = () => {
    const columns = [
      {
        title: '日期',
        dataIndex: 'transaction_date',
        key: 'transaction_date',
        fixed: 'left',
        width: 120
      }
    ]

    // 添加维度列
    dimensions.forEach(dim => {
      const dimOption = availableDimensions.find(d => d.value === dim)
      columns.push({
        title: dimOption?.label || dim,
        dataIndex: dim,
        key: dim
      })
    })

    // 添加指标值列
    columns.push({
      title: selectedMetric?.label || '指标值',
      dataIndex: 'metric_value',
      key: 'metric_value',
      align: 'right',
      render: (value) => {
        if (value === null || value === undefined) return '-'
        const formatted = typeof value === 'number' 
          ? value.toLocaleString('zh-CN', { maximumFractionDigits: 2 })
          : value
        return (
          <span style={{ fontWeight: 'bold', color: '#1890ff' }}>
            {formatted} {selectedMetric?.unit || ''}
          </span>
        )
      }
    })

    return columns
  }

  return (
    <div>
      <Card
        title={
          <Space>
            <LineChartOutlined />
            <span>语义层指标查询</span>
          </Space>
        }
        extra={
          <Space>
            <Button
              type={chatMode ? 'default' : 'primary'}
              onClick={() => setChatMode(!chatMode)}
            >
              {chatMode ? '切换到表单模式' : '切换到聊天模式'}
            </Button>
            {!chatMode && (
              <Button
                icon={<ReloadOutlined />}
                onClick={handleQuery}
                loading={loading}
                type="primary"
              >
                查询
              </Button>
            )}
          </Space>
        }
      >
        {chatMode ? (
          // 聊天模式
          <div style={{ display: 'flex', flexDirection: 'column', height: '600px' }}>
            {/* 聊天历史 */}
            <div
              ref={chatContainerRef}
              style={{
                flex: 1,
                overflowY: 'auto',
                padding: '16px',
                background: '#f5f5f5',
                borderRadius: '8px',
                marginBottom: '16px'
              }}
            >
              {chatHistory.length === 0 ? (
                <div style={{ textAlign: 'center', padding: '40px', color: '#999' }}>
                  <RobotOutlined style={{ fontSize: 48, marginBottom: 16 }} />
                  <div style={{ fontSize: 18, fontWeight: 'bold', marginBottom: 8 }}>👋 您好！我是智能查询助手</div>
                  <div style={{ marginTop: 8, fontSize: 14, marginBottom: 24 }}>
                    您可以用自然语言查询指标数据，点击下方示例快速开始：
                  </div>
                  
                  {/* 基础查询示例 */}
                  <div style={{ textAlign: 'left', marginBottom: 24 }}>
                    <div style={{ fontSize: 14, fontWeight: 'bold', marginBottom: 12, color: '#1890ff' }}>
                      📊 基础查询
                    </div>
                    <div style={{ display: 'flex', flexWrap: 'wrap', gap: 8, justifyContent: 'center' }}>
                      <Tag color="blue" style={{ margin: 0, cursor: 'pointer', padding: '8px 12px', fontSize: 13 }}
                        onClick={() => setChatInput('查询最近7天的日收入，按城市分组')}>
                        查询最近7天的日收入，按城市分组
                      </Tag>
                      <Tag color="blue" style={{ margin: 0, cursor: 'pointer', padding: '8px 12px', fontSize: 13 }}
                        onClick={() => setChatInput('显示北京本月的交易笔数')}>
                        显示北京本月的交易笔数
                      </Tag>
                      <Tag color="blue" style={{ margin: 0, cursor: 'pointer', padding: '8px 12px', fontSize: 13 }}
                        onClick={() => setChatInput('查看昨天的正常交易率')}>
                        查看昨天的正常交易率
                      </Tag>
                    </div>
                  </div>

                  {/* 复杂查询示例 */}
                  <div style={{ textAlign: 'left', marginBottom: 24 }}>
                    <div style={{ fontSize: 14, fontWeight: 'bold', marginBottom: 12, color: '#52c41a' }}>
                      🔍 复杂查询
                    </div>
                    <div style={{ display: 'flex', flexWrap: 'wrap', gap: 8, justifyContent: 'center' }}>
                      <Tag color="green" style={{ margin: 0, cursor: 'pointer', padding: '8px 12px', fontSize: 13 }}
                        onClick={() => setChatInput('查询最近30天的平均交易金额，按城市和车型分组')}>
                        查询最近30天的平均交易金额，按城市和车型分组
                      </Tag>
                      <Tag color="green" style={{ margin: 0, cursor: 'pointer', padding: '8px 12px', fontSize: 13 }}
                        onClick={() => setChatInput('统计本月的交易笔数和总收入，按收费站和支付方式分组')}>
                        统计本月的交易笔数和总收入，按收费站和支付方式分组
                      </Tag>
                      <Tag color="green" style={{ margin: 0, cursor: 'pointer', padding: '8px 12px', fontSize: 13 }}
                        onClick={() => setChatInput('查看最近一周的数据质量率和正常交易率，按城市分组')}>
                        查看最近一周的数据质量率和正常交易率，按城市分组
                      </Tag>
                    </div>
                  </div>

                  {/* 时间范围查询示例 */}
                  <div style={{ textAlign: 'left', marginBottom: 24 }}>
                    <div style={{ fontSize: 14, fontWeight: 'bold', marginBottom: 12, color: '#fa8c16' }}>
                      ⏰ 时间范围查询
                    </div>
                    <div style={{ display: 'flex', flexWrap: 'wrap', gap: 8, justifyContent: 'center' }}>
                      <Tag color="orange" style={{ margin: 0, cursor: 'pointer', padding: '8px 12px', fontSize: 13 }}
                        onClick={() => setChatInput('查询2025年1月1日到1月31日的日收入趋势，按城市分组')}>
                        查询2025年1月1日到1月31日的日收入趋势，按城市分组
                      </Tag>
                      <Tag color="orange" style={{ margin: 0, cursor: 'pointer', padding: '8px 12px', fontSize: 13 }}
                        onClick={() => setChatInput('显示最近3个月的交易笔数，按收费站类型分组')}>
                        显示最近3个月的交易笔数，按收费站类型分组
                      </Tag>
                      <Tag color="orange" style={{ margin: 0, cursor: 'pointer', padding: '8px 12px', fontSize: 13 }}
                        onClick={() => setChatInput('查看本周的车辆数和平均交易金额')}>
                        查看本周的车辆数和平均交易金额
                      </Tag>
                    </div>
                  </div>

                  {/* 多维度分析示例 */}
                  <div style={{ textAlign: 'left' }}>
                    <div style={{ fontSize: 14, fontWeight: 'bold', marginBottom: 12, color: '#eb2f96' }}>
                      📈 多维度分析
                    </div>
                    <div style={{ display: 'flex', flexWrap: 'wrap', gap: 8, justifyContent: 'center' }}>
                      <Tag color="magenta" style={{ margin: 0, cursor: 'pointer', padding: '8px 12px', fontSize: 13 }}
                        onClick={() => setChatInput('分析最近7天各城市、各车型的收入分布情况')}>
                        分析最近7天各城市、各车型的收入分布情况
                      </Tag>
                      <Tag color="magenta" style={{ margin: 0, cursor: 'pointer', padding: '8px 12px', fontSize: 13 }}
                        onClick={() => setChatInput('对比不同支付方式在本月的交易笔数和收入')}>
                        对比不同支付方式在本月的交易笔数和收入
                      </Tag>
                      <Tag color="magenta" style={{ margin: 0, cursor: 'pointer', padding: '8px 12px', fontSize: 13 }}
                        onClick={() => setChatInput('查看各高速公路编码的日收入，按收费站分组')}>
                        查看各高速公路编码的日收入，按收费站分组
                      </Tag>
                    </div>
                  </div>
                </div>
              ) : (
                <List
                  dataSource={chatHistory}
                  renderItem={(item, index) => (
                    <List.Item
                      key={index}
                      style={{
                        border: 'none',
                        padding: '12px 0',
                        justifyContent: item.role === 'user' ? 'flex-end' : 'flex-start'
                      }}
                    >
                      <Space
                        direction={item.role === 'user' ? 'horizontal-reverse' : 'horizontal'}
                        style={{ width: '100%', maxWidth: '80%' }}
                      >
                        <Avatar
                          icon={item.role === 'user' ? <UserOutlined /> : <RobotOutlined />}
                          style={{
                            backgroundColor: item.role === 'user' ? '#1890ff' : '#52c41a'
                          }}
                        />
                        <div
                          style={{
                            background: item.role === 'user' ? '#1890ff' : '#fff',
                            color: item.role === 'user' ? '#fff' : '#000',
                            padding: '12px 16px',
                            borderRadius: '12px',
                            boxShadow: '0 2px 8px rgba(0,0,0,0.1)',
                            wordBreak: 'break-word'
                          }}
                        >
                          <Text style={{ color: item.role === 'user' ? '#fff' : '#000' }}>
                            {item.content}
                          </Text>
                          {item.error && (
                            <div style={{ marginTop: 8, fontSize: 12, opacity: 0.8 }}>
                              提示：可以尝试更清晰的表达，或切换到表单模式手动选择
                            </div>
                          )}
                        </div>
                      </Space>
                    </List.Item>
                  )}
                />
              )}
              <div ref={chatEndRef} />
            </div>

            {/* 输入框 */}
            <Space.Compact style={{ width: '100%' }}>
              <TextArea
                value={chatInput}
                onChange={(e) => setChatInput(e.target.value)}
                onKeyPress={handleChatInputKeyPress}
                placeholder="输入您的问题，例如：查询最近7天的日收入，按城市分组"
                autoSize={{ minRows: 2, maxRows: 4 }}
                disabled={chatLoading}
              />
              <Button
                type="primary"
                icon={<SendOutlined />}
                onClick={handleNaturalLanguageQuery}
                loading={chatLoading}
                style={{ height: 'auto' }}
                disabled={!chatInput.trim()}
              >
                发送
              </Button>
            </Space.Compact>
          </div>
        ) : (
          // 表单模式
          <>
        <Row gutter={[16, 16]}>
          <Col xs={24} sm={12} md={8}>
            <div>
              <label style={{ display: 'block', marginBottom: 8, fontWeight: 'bold' }}>
                选择指标
              </label>
              <Select
                style={{ width: '100%' }}
                value={selectedMetric?.name}
                onChange={(value) => {
                  const metric = metrics.find(m => m.name === value)
                  setSelectedMetric(metric)
                }}
                placeholder="请选择指标"
              >
                {metrics.map(metric => (
                  <Option key={metric.name} value={metric.name}>
                    <Space>
                      <span>{metric.label}</span>
                      <Tag color="blue">{metric.unit}</Tag>
                    </Space>
                  </Option>
                ))}
              </Select>
              {selectedMetric && (
                <div style={{ marginTop: 8, color: '#666', fontSize: 12 }}>
                  {selectedMetric.description}
                </div>
              )}
            </div>
          </Col>

          <Col xs={24} sm={12} md={8}>
            <div>
              <label style={{ display: 'block', marginBottom: 8, fontWeight: 'bold' }}>
                选择维度
              </label>
              <Select
                mode="multiple"
                style={{ width: '100%' }}
                value={dimensions}
                onChange={setDimensions}
                placeholder="选择分组维度（可选）"
                maxTagCount="responsive"
              >
                {availableDimensions.map(dim => (
                  <Option key={dim.value} value={dim.value}>
                    {dim.label}
                  </Option>
                ))}
              </Select>
            </div>
          </Col>

          <Col xs={24} sm={12} md={8}>
            <div>
              <label style={{ display: 'block', marginBottom: 8, fontWeight: 'bold' }}>
                时间范围
              </label>
              <RangePicker
                style={{ width: '100%' }}
                value={dateRange}
                onChange={setDateRange}
                format="YYYY-MM-DD"
              />
            </div>
          </Col>
        </Row>

        {queryError && (
          <Alert
            message="查询错误"
            description={queryError}
            type="error"
            showIcon
            style={{ marginTop: 16 }}
            closable
            onClose={() => setQueryError(null)}
          />
        )}
          </>
        )}
      </Card>

      {queryResult && (
        <>
          <Card
            title="趋势图表"
            style={{ marginTop: 16 }}
            extra={
              <Space>
                <Tag color="blue">{selectedMetric?.label}</Tag>
                <Tag color="green">数据量: {queryResult.data?.length || 0}</Tag>
              </Space>
            }
          >
            {getChartOption() ? (
              <ReactECharts
                option={getChartOption()}
                style={{ height: 400 }}
                opts={{ renderer: 'svg' }}
              />
            ) : (
              <div style={{ textAlign: 'center', padding: 40, color: '#999' }}>
                暂无数据
              </div>
            )}
          </Card>

          <Card title="数据表格" style={{ marginTop: 16 }}>
            <Table
              columns={getTableColumns()}
              dataSource={queryResult.data || []}
              rowKey={(record, index) => 
                `${record.transaction_date}-${index}-${dimensions.map(d => record[d]).join('-')}`
              }
              pagination={{
                pageSize: 20,
                showSizeChanger: true,
                showTotal: (total) => `共 ${total} 条记录`
              }}
              scroll={{ x: 'max-content' }}
              size="small"
            />
          </Card>

          {queryResult.query_sql && (
            <Card title="生成的SQL（调试用）" style={{ marginTop: 16 }}>
              <pre style={{ 
                background: '#f5f5f5', 
                padding: 16, 
                borderRadius: 4,
                overflow: 'auto',
                fontSize: 12
              }}>
                {queryResult.query_sql}
              </pre>
            </Card>
          )}
        </>
      )}

      {!queryResult && !loading && (
        <Card style={{ marginTop: 16, textAlign: 'center' }}>
          <div style={{ padding: 40, color: '#999' }}>
            <LineChartOutlined style={{ fontSize: 48, marginBottom: 16 }} />
            <div>请选择指标并点击查询按钮</div>
          </div>
        </Card>
      )}
    </div>
  )
}

