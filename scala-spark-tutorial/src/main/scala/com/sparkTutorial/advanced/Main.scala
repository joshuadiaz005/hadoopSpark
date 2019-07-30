package com.sparkTutorial.advanced

import org.apache.spark._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Main {
  val spark = SparkSession.builder().appName("JoshuaYKarvin").config("spark.some.configuration.option", "some-value").getOrCreate()
  val sparkConfiguration = new SparkConf().setAppName("JoshuaYKarvin").setMaster("local[3]")

  val sparkContext = new SparkContext(sparkConfiguration)

  import spark.implicits._
  val archivo = sparkContext.textFile("hdfs://localhost:9870/final/prueba.txt").map(archivo1 => archivo1.split("\t")).map(archivo2 => (archivo2(0), archivo2(1), archivo2(2).toInt)).toDF("ciudad", "mes", "monto")

  archivo.show()

  //a. Total Ventas x Cada ciudad
  archivo.select("ciudad", "monto").groupBy("ciudad").sum().show()

  //b. Total Ventas x Cada mes
  archivo.select("mes", "monto").groupBy("mes").sum().show()


  //c. mes con Mayor Venta
    archivo.select("mes", "monto").groupBy("mes").sum().sort(desc("sum(monto)")).limit(1).show()


  //d. ciudad con Mayor Venta
    archivo.select("ciudad", "monto").groupBy("ciudad").sum().sort(desc("sum(monto)")).limit(1).show()


  //e. Mostrar el Total Ventas para los meses Enero y Marzo en la ciudades de
  //Santiago, La Vega y Moca.
   archivo.select("mes", "ciudad", "monto").groupBy("mes", "ciudad").sum().where(($"mes" === "Enero" || $"mes" === "Marzo") && ($"ciudad" === "Santiago" || $"ciudad" === "Moca"))


}
