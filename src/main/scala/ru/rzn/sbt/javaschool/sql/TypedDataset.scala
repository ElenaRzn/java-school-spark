package ru.rzn.sbt.javaschool.sql

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
 * Рассмотрите файл 2016-stack-overflow-survey-responses.csv, выполните задания, для вывода используйте метод show()
 */
object TypedDataset extends App {
  val AGE_MIDPOINT = "age_midpoint"
  val SALARY_MIDPOINT = "salary_midpoint"
  val SALARY_MIDPOINT_BUCKET = "salaryMidpointBucket"

  Logger.getLogger("org").setLevel(Level.ERROR)
  val session = SparkSession.builder().appName("StackOverFlowSurvey").master("local[*]").getOrCreate()
  val dataFrameReader = session.read

  val responses = dataFrameReader
    .option("header", "true")
    .option("inferSchema", value = true)
    .csv("src/main/resource/in/2016-stack-overflow-survey-responses.csv")

  val responseWithSelectedColumns = responses.select("country", "age_midpoint", "occupation", "salary_midpoint")

  import session.implicits._
  val typedDataset = responseWithSelectedColumns.as[Response]

  // TODO выведите схему
  System.out.println("=== Schema ===")
  typedDataset.printSchema()

  // TODO выведите первые 20 записей
  System.out.println("=== 20 records ===")
  typedDataset.show(20)

  // TODO отфильтруйте все ответы из Австралии
  System.out.println("=== responses from Australia ===")
  typedDataset.filter(response => response.country == "Australia").show()

  // TODO сгруппируйте данные по роду занятия (occupations) и выведите количество записей в каждой группе
  System.out.println("=== count of occupations ===")
  typedDataset.groupBy(typedDataset.col("occupation")).count().show()

  // TODO отфильтруйте ответы со значением age_midpoint меньше 20
  System.out.println("=== responses with average mid age less than 20 ===")
  typedDataset.filter(response => response.age_midpoint.isDefined && response.age_midpoint.get < 20.0).show()

  // TODO отсортируйте ответы по полю salary_midpoint по убывающей
  System.out.println("=== print the result by salary middle point in descending order ===")
  typedDataset.orderBy(typedDataset.col(SALARY_MIDPOINT).desc).show()

  // TODO сгруппируйте ответы по стране и выведите среднее значение поля salary_midpoint
  System.out.println("=== group by country and aggregate by average salary middle point ===")
  typedDataset.filter(response => response.salary_midpoint.isDefined).groupBy("country").avg(SALARY_MIDPOINT).show()

}
