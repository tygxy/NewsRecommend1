/**
  * Created by guoxingyu on 2017/7/13.
  */
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.feature.{HashingTF, IDF}
import org.apache.spark.mllib.linalg.{SparseVector => SV}
import org.apache.spark.{SparkConf, SparkContext}
import breeze.linalg._


object NewsRelation {

  def main(args: Array[String]) {


    /**
      * 测试配置
      */
        val conf = new SparkConf().setMaster("local[*]").setAppName("TF_IDF")
        val newsDocumentsPath = "/Users/guoxingyu/Documents/work/spark/NewsRelation/000000_0"
        val outputPath = "/Users/guoxingyu/Documents/work/spark/NewsRelation/output"

    /**
      * 集群配置
      */
//        val conf = new SparkConf().setAppName("TF_IDF")
//        val newsDocumentsPath = "/qcdq/recommend/newsrecommend/baseOnContent/newsInfo"
//        val outputPath = "/qcdq/recommend/newsrecommend/baseOnContent/output"

        val sc = new SparkContext(conf)
        // 读取新闻信息
        val newsDocuments = sc.textFile(newsDocumentsPath)

        // 构造(newid,(keyword1,keywords2...))格式
        val newsInfo: RDD[(String, Seq[String])] = newsDocuments.map { x =>
          val data: Array[String] = x.trim.split("/")
          val keywords = data(1).split("-").toSeq
          (data(0).toString, keywords)
        }

        // TF
        val hashingTF = new HashingTF(Math.pow(2, 18).toInt)
        val tf_num_pairs = newsInfo.map {
          case (num, seq) =>
            val tf = hashingTF.transform(seq)
            (num, tf)
        }
        tf_num_pairs.cache()

        //构建idf model
        val idf = new IDF().fit(tf_num_pairs.values)
        //将tf向量转换成tf-idf向量
        val num_idf_pairs = tf_num_pairs.mapValues(v => idf.transform(v))
        //广播一份tf-idf向量集
        val b_num_idf_pairs = sc.broadcast(num_idf_pairs.collect())

        //计算新闻之间余弦相似度
        val docSims: RDD[(String, String, Double)] = num_idf_pairs.flatMap {
          case (id1, idf1) =>
            val idfs = b_num_idf_pairs.value.filter(_._1 != id1)
            val sv1 = idf1.asInstanceOf[SV]
            val bsv1 = new SparseVector[Double](sv1.indices, sv1.values, sv1.size)
            idfs.map {
              case (id2, idf2) =>
                val sv2 = idf2.asInstanceOf[SV]
                val bsv2 = new SparseVector[Double](sv2.indices, sv2.values, sv2.size)
                val cosSim = bsv1.dot(bsv2) / (norm(bsv1) * norm(bsv2))
                (id1, id2, cosSim)
            }
        }

        val allDocsims: RDD[(String, String, Double)] = docSims.cache()

        // 按照newsid,recommend1=score1 recommend2=score2...格式输出结果
        val docSimsSorted: RDD[(String, String, Double)] = allDocsims.sortBy(_._3,false)
        val docSimsGroupByKey: RDD[(String, Iterable[(String, String, Double)])] = docSimsSorted.groupBy(_._1)
        val result: RDD[String] = docSimsGroupByKey.map { x =>
          var recommend = ""
          var recommendNum = 0
          for (i <- x._2) {
            if (recommendNum < 10) {
              if (i._3.toString.length <= 4) {
                recommend += i._2 + "=" + i._3 + " "
              } else {
                recommend += i._2 + "=" + i._3.toString.substring(0,4) + " "
              }
              recommendNum += 1
            }
          }
          x._1 + "," + recommend
        }

//        result.coalesce(1,true).saveAsTextFile(outputPath)
          result.saveAsTextFile(outputPath)
//        print("Have Finished")
        sc.stop()
  }
}
