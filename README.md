# NewsRecommend

## 1.说明

### 1.1 版本要求
- 本程序基于spark-1.6/scala-2.10,在集群中亲测可用

### 1.2 思路说明
- 本程序是基于内容的新闻推荐系统，推荐与文章关键词最接近的十篇其他文章。其完整思路如下
	- 对所有新闻标题进行分词，构造出以newsid/keyword1-keywords2....格式的文件存入HDFS/本地
	- spark按行读取新闻，构造出(newid,(keyword1,keywords2...))格式的RDD
	- 调用mllib包，计算关键词的TF-IDF值
	- 计算所有新闻之间的余弦相似度，并按照newsID分组，按照相似度大小排序
	- 按照newsid,recommend1=score1 recommend2=score2...格式输出前十个结果

### 1.3 使用说明
- 按照格式要求组织新闻数据
- 根据使用情况选择测试配置/集群配置，并按照实际情况选择输入输出地址
- 集群使用中，打成jar包上传到集群
- 执行spark-submit
	```
	spark-submit --master yarn-cluster  --driver-memory 8G --num-executors 100 --executor-cores 5 --executor-memory 4G  --class NewsRelation  ./NewsRelation.jar
	```


