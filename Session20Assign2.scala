import org.apache.spark.sql.{Column, Row, SQLContext, SparkSession}  //Explanation is already given in Assignment18.1


object Session20Assign2 extends App {
  val spark = SparkSession.builder()
    .master("local")
    .appName("Session20Assign2")
    .config("spark.sql.warehouse.dir", "file:///G:/ACADGILD/course material/Hadoop/Sessions/Session 20/Assignments/Assignment2")
    .getOrCreate()
  //Explanation is already given in Assignment 18.1

  //setting path of winutils.exe
  System.setProperty("hadoop.home.dir","F:/Softwares/winutils")
  //winutils.exe needs to be present inside HADOOP_HOME directory, else below error is returned:
  //error: java.io.IOException: Could not locate executable null\bin\winutils.exe in the Hadoop binaries.

  //reading "demonetization-tweets.csv" file
  val tweets = spark.sparkContext.textFile("file:///G:/ACADGILD/course material/Hadoop/Sessions/Session 20/Assignments/Assignment2/demonetization-tweets.csv")
  //tweets -->> RDD[String]
  tweets.foreach(x => println(x))
  //REFER Screenshot 1 for OUTPUT

  //filtering data with ',' as delimiter and resulting fields having length >= 2
  val filtertweets = tweets.map(x => x.split(",")).filter(x=>x.length>=2)
  //filtertweets -->> Rdd[Array[String]]
  filtertweets.foreach(x => x.foreach(println))
  //REFER Screenshot 2 for OUTPUT

  //replacing '\' with '' inside field 0 and field 1 and changing all characters of field 1 to lower case of field 2 and furthermore, creating tuple having two fields
  val replacefiltertweets = filtertweets.map(x => (x(0).replaceAll("\"",""),x(1).replaceAll("\"","").toLowerCase))
  //replacefiltertweets -->> RDD[(String,String)]
  replacefiltertweets.foreach(x => println(x._1 + "------"+ x._2))
  //REFER Screenshot 3 for OUTPUT

  //below import is required to convert rdd to dataframe
  import spark.implicits._
  //tuple of two elements is created, where second element of tuple contains words separated by space
  val tupletweets = replacefiltertweets.map(x => (x._1,x._2.split(" "))).toDF("id","words")
  //tupletweets -->> sql.DataFrame
  tupletweets.show(12000)
  //REFER Screenshot 4 for OUTPUT

  //temporary view is created from tupletweets dataframe
  tupletweets.createOrReplaceTempView("tweets")
  spark.sql("select * from tweets").show(12000)
  //REFER Screenshot 5 for OUTPUT

  //explode function places each word on separate line along with id
  val explode = spark.sql("select id as id,explode(words) as word from tweets")
  //explode -->> unit

  //temporary view is created from explode unit
  explode.createOrReplaceTempView("tweet_word")
  spark.sql("select * from tweet_word").show(120000)
  //REFER Screenshot 6 for OUTPUT

  //reading AFINN-111.txt where elements of each row are separated by ', ' and thereby converting rdd to dataframe
  val afinn = spark.sparkContext.textFile("file:///G:/ACADGILD/course material/Hadoop/Sessions/Session 20/Assignments/Assignment2/AFINN-111.txt").map(x => x.split(", ")).map(x => (x(0),x(1))).toDF("word","rating")
  //afinn -->> sql.DataFrame
  afinn.show(2500)
  //REFER Screenshot 7 for OUTPUT

  //temporary view is created from afinn dataframe
  afinn.createOrReplaceTempView("afinn")
  spark.sql("select * from afinn").show(2500)
  //REFER Screenshot 8 for OUTPUT

  //afinn and tweet_word views are joined on the basis of common "words" field and for each id, average of rating is found to analyse sentiment on demonetization
  val join = spark.sql("select t.id,AVG(a.rating) as rating from tweet_word t join afinn a on t.word=a.word group by t.id order by rating desc")
  //join -->> sql.DataFrame
  join.show(2500)
  //REFER Screenshot 9 for OUTPUT

}