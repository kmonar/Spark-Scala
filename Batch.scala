package main.scala.com.bddprocessing.Práctica

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object Batch {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder()
      .appName("trayectos-data")
      .config("spark.master","local[*]")
      .getOrCreate()
    import spark.implicits._


    val trayectos = spark.read
      .option("header","false")
      .option("inferSchema",value = true)
      .csv("in/trayectos.csv")

    val naves = spark.read
      .option("header","true")
      .option("inferSchema",value = true)
      .csv("in/naves_transporte.csv")
      .filter("Descripcion not like '%Scala' and Descripcion not like '%Master'")
    //excluimos naves con código erróneo Scala y Master

    //creacion de una tabla temporal (tipo relacional) donde realizar las queries
    trayectos.createOrReplaceTempView("tabla_trayectos")

    //creacion de una tabla temporal (tipo relacional) do
    naves.createOrReplaceTempView("tabla_naves")
    //
    val trayectos_limpio = spark.sql("SELECT tr._c1 codigoNave, tn.Descripcion nombreNave, tr._c9 consumo " +
      "FROM tabla_trayectos tr left join tabla_naves tn on tr._c1=tn.Codigo")

    trayectos.show()
    trayectos.printSchema()

    naves.printSchema()
    naves.show()  

    trayectos_limpio.printSchema()
    trayectos_limpio.show()

    trayectos_limpio.groupBy("codigoNave", "nombreNave")
      .avg("consumo").sort("avg(consumo)").show()


    val resBatch=trayectos_limpio.groupBy("codigoNave", "nombreNave").avg("consumo")
      .sort("avg(consumo)").toDF("codigoNave","nombreNave","consumoPromedioBatch")
      .coalesce(1)
      .write.format("com.databricks.spark.csv").mode("overwrite").option("header", "true")
      .save("out/practica_batch.csv")

  }

}
