package main.scala.com.bddprocessing.kafka

import java.util.regex.Matcher

import main.scala.com.bddprocessing.commons.Utils
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object Streaming {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)

    val ssc = new StreamingContext("local[*]",
      "analisis-web-log",Seconds(1))

    ssc.checkpoint("out/checkpoint_practice")

    //parametros kafka
    val kafkaParams = Map[String,Object](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "spark-group",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false:java.lang.Boolean)
    )

    val spark=SparkSession.builder().appName("Palabras RT")
      .config("spark.master","local[1]")//para crear un DF y poder hacer queries
      .getOrCreate()

    //a que topic me quiero subscribir como consumer (driver program)
    val topics = Array("practice")

    //crear el DStream
    val stream = KafkaUtils.createDirectStream[String,String](ssc,PreferConsistent,
      Subscribe[String,String](topics,kafkaParams))

    val lines = stream.map(item => item.value)

    //transformación sobre todos los elementos de la colección. por cada línea
    val consumos = lines.map(x => (x.split(Utils.COMMA_DELIMITER)(1).toInt,
      (x.split(Utils.COMMA_DELIMITER)(9).toDouble,1)))

    val consumoNaves = consumos.reduceByKeyAndWindow( (x,y) => (x._1 + y._1 , x._2 + y._2)
      , (x,y) => (x._1 - y._1 , x._2 - y._2),Seconds(300),Seconds(1))
      .mapValues(infoValor => (infoValor._1,infoValor._2,infoValor._1/infoValor._2))//cuenta, consumo total y consumo promedio

    //Ordenamos para ver las naves con consumo promedio más económico.
    // Ordenamos por el 3er elemento de la 2da tupla de forma ascendente
    val sortedResults = consumoNaves.transform(rdd => rdd.sortBy(x => x._2._3,true))
    //sortedResults.print()

    ////////Ingestamos datos de naves y resultados batch para hacer las queries de comparación batch vs live streaming
    ////Naves
    val naves = spark.read
      .option("header","true")
      .option("inferSchema",value = true)
      .csv("in/naves_transporte.csv")
      .filter("Descripcion not like '%Scala%' and Descripcion not like '%Master'")
    //excluimos naves con código erróneo Scala y Master
    //creacion de una tabla temporal (tipo relacional) do
    naves.createOrReplaceTempView("tabla_naves")

    ////Resultados batch
    val resultBatch = spark.read
      .option("header","true")
      .option("inferSchema",value = true)
      .csv("out/practica_batch.csv")
    //creacion de una tabla temporal (tipo relacional) do
    resultBatch.createOrReplaceTempView("tabla_resbatch")
    //resultBatch.printSchema()

    sortedResults.foreachRDD { rdd =>
      val df=spark.createDataFrame(rdd.map{x => (x._1,x._2._1,x._2._2,x._2._3)})
        .withColumnRenamed("_1","codNave")
        .withColumnRenamed("_2","consumo")
        .withColumnRenamed("_3","númeroTrayectos")
        .withColumnRenamed("_4","consumoPromedio")
      df.createOrReplaceTempView("tabla_strlive")
      //df.show()
      val comparisson = spark.sql("SELECT sl.*, tn.Descripcion nombreNave, rb.consumoPromedioBatch" +
        ", sl.consumoPromedio-consumoPromedioBatch diferenciaMedias " +
        "FROM tabla_strlive sl" +
        " INNER join  tabla_resbatch rb" +
        "   on cast(sl.codNave as int)=cast(rb.codigoNave as int)" +
        " inner join tabla_naves tn" +
        "   on cast(sl.codNave as int)=cast(tn.Codigo as int)" +
        "order by diferenciaMedias")
      comparisson.show()//Diferencia de consumos medios
      //Lista con 3 mejores naves
      comparisson.select("codNave","nombreNave").rdd.map(r => (r(0),r(1)))
        .collect.toList.take(3).foreach(println)//Tupla con 3 naves más eficientes
    }

    ssc.start()
    ssc.awaitTermination()
  }

}
