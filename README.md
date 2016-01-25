# DASCAN
DBScan算法的详细介绍见http://blog.csdn.net/u012050154/article/details/50502154

DBScan算法：基于密度的空间聚类算法 ，这里基于Spark平台对部分学生上网时间进行聚类，得到研究对象月上网时间分布。实例中将初始数据抽取有用信息，得到<MACAddress,OnlineTime>形式的有效数据，MACAddress作为对象标识，OnlineTime是该对象的上网时间，度量对象与对象的距离就是一维空间下的绝对值距离（即：Distance=|OnlineTime1-OnlineTime2|）


TestData.txt中是程序测试用例，给出的是学生上网的数据，多面手根据学生上网时间的时长进行密度聚类
