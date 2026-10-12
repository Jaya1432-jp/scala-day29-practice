import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.storage.StorageLevel

object Day29 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Day29-EndToEndECommerce")
      .master("local[2]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._

    val base = "data/"
    val output = "output/"

    println("\n===== 1. READ RAW DATA =====")

    val rawOrders = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(base + "orders.csv")

    val customers = spark.read
      .option("header", "true")
      .csv(base + "customers.csv")

    val products = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(base + "products.csv")

    println(s"Raw order count: ${rawOrders.count()}")

    println("\n===== 2. CLEAN ORDERS =====")

    val rejectedRecords = spark.sparkContext.longAccumulator("Rejected Orders")

    val validOrders = rawOrders
      .filter(
        col("order_id").isNotNull &&
        col("customer_id").isNotNull &&
        col("product_id").isNotNull &&
        col("quantity").isNotNull &&
        col("quantity") > 0 &&
        to_date(col("order_date")).isNotNull
      )

    // Count rejected rows using the driver-side accumulator.
    rawOrders.collect().foreach { row =>
      val quantity = try {
        Option(row.getAs[Any]("quantity")).map(_.toString.toDouble)
      } catch {
        case _: Exception => None
      }
      val date = Option(row.getAs[Any]("order_date")).map(_.toString)

      val invalid =
        row.isNullAt(row.fieldIndex("order_id")) ||
        row.isNullAt(row.fieldIndex("customer_id")) ||
        row.isNullAt(row.fieldIndex("product_id")) ||
        quantity.forall(_ <= 0) ||
        date.forall(d => java.time.LocalDate.parse(d).isAfter(java.time.LocalDate.MAX))

      if (invalid) rejectedRecords.add(1L)
    }

    val cleaned = validOrders
      .withColumn("order_date", to_date(col("order_date")))
      .persist(StorageLevel.MEMORY_AND_DISK)

    println(s"Clean order count: ${cleaned.count()}")
    println(s"Rejected order count: ${rawOrders.count() - cleaned.count()}")

    println("\n===== 3. PAIR RDD REVENUE BY PRODUCT =====")

    val productPrices = products
      .select("product_id", "price")
      .as[(String, Double)]
      .collect()
      .toMap

    val productBroadcast = spark.sparkContext.broadcast(productPrices)

    val productRevenue = cleaned.select("product_id", "quantity")
      .as[(String, Int)]
      .rdd
      .map { case (product, quantity) =>
        (product, productBroadcast.value.getOrElse(product, 0.0) * quantity)
      }
      .reduceByKey(_ + _)

    productRevenue.collect().sortBy(_._1).foreach {
      case (product, revenue) =>
        println(f"$product: Rs. $revenue%.2f")
    }

    println("\n===== 4. ENRICH ORDERS =====")

    val enriched = cleaned
      .join(customers, Seq("customer_id"), "inner")
      .join(broadcast(products), Seq("product_id"), "inner")
      .withColumn("revenue", col("quantity") * col("price"))
      .withColumn("order_year", year(col("order_date")))
      .withColumn("order_month", month(col("order_date")))
      .persist(StorageLevel.MEMORY_AND_DISK)

    println(s"Enriched order count: ${enriched.count()}")
    enriched.select(
      "order_id", "customer_name", "product_name",
      "quantity", "revenue", "order_date"
    ).orderBy("order_id").show(false)

    println("\n===== 5. REVENUE BY CATEGORY =====")

    enriched.groupBy("category")
      .agg(
        sum("revenue").as("total_revenue"),
        count("*").as("order_count")
      )
      .orderBy(desc("total_revenue"))
      .show(false)

    println("\n===== 6. CUSTOMER ORDER WINDOW =====")

    val customerWindow = Window
      .partitionBy("customer_id")
      .orderBy(col("order_date").desc, col("order_id").desc)

    enriched.withColumn("order_rank", row_number().over(customerWindow))
      .select("customer_id", "customer_name", "order_id", "order_date", "order_rank")
      .orderBy("customer_id", "order_rank")
      .show(false)

    println("\n===== 7. DAILY REVENUE =====")

    enriched.groupBy("order_date")
      .agg(sum("revenue").as("daily_revenue"))
      .orderBy("order_date")
      .show(false)

    println("\n===== 8. WRITE PARTITIONED PARQUET =====")

    enriched.repartition(2)
      .write
      .mode("overwrite")
      .partitionBy("order_year", "order_month")
      .parquet(output + "orders_parquet")

    println(s"Parquet output saved to $output/orders_parquet")
    println(s"Rejected-record accumulator: ${rejectedRecords.value}")

    enriched.unpersist()
    cleaned.unpersist()
    spark.stop()
    println("\nDAY 29 PIPELINE COMPLETED SUCCESSFULLY")
  }
}
