import java.util.concurrent.Executors
import scala.concurrent.{Future, Await, ExecutionContext}
import scala.concurrent.duration.Duration
import scala.io.{Codec, Source}
import scala.util.{Failure, Success, Try, Using}
//TIP To <b>Run</b> code, press <shortcut actionId="Run"/> or click the <icon src="AllIcons.Actions.Execute"/> icon in the gutter.
object Main extends App {
  SimpleLogger.info("Engine Started.")

  // Creating a dedicated thread pool for our parallel workers
  implicit val ec: ExecutionContext = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(8))

  def readFile(fileName: String, codec: String = Codec.default.toString): Try[List[String]] = {
    SimpleLogger.info("Reading the raw data.")

    Using(Source.fromFile(fileName, codec)) { source =>
      source.getLines().toList
    }
  }

  def order_split(order: String) = {
    order.split(",").toList
  }

  def evaluating_discounts(order: List[String])= {
    val discounts = Rules.rules_fn.map(f => f(order)).toVector.sortBy(-_)
    val dis = {
      if(discounts(1) != 0){ math.round( (discounts(0)+discounts(1)) / 2.0 * 100 ) / 100.0 }
      else { discounts(0) }
    }

    order :+ dis.toString
  }


  def processMassiveFileParallel(fileName: String): Unit = {
    SimpleLogger.info("Starting PARALLEL streaming pipeline for raw data.")

    Using(Source.fromFile(fileName, Codec.default.toString)) { source =>
      val lines = source.getLines().drop(1) // Drop header

      // 1. Group into "Super-Batches" of 50,000 to manage memory safely
      lines.grouped(50000).zipWithIndex.foreach { case (superBatch, superIndex) =>

        // 2. Split into parallelizable units of 10,000 and spawn Futures
        val futures = superBatch.grouped(10000).zipWithIndex.map { case (batch, batchIndex) =>
          Future {
            // Pure CPU functional transformation happens in parallel
            val processedBatch = batch.map(order_split).map(evaluating_discounts)

            // 3. Thread-safe I/O: EACH THREAD OPENS ITS OWN DB CONNECTION
            DBTest.openConnection() match {
              case Success(conn) =>
                val result = DBTest.insertBatch(conn, processedBatch)
                DBTest.closeConnection(conn) // Always close the connection when the thread is done
                result match {
                  case Success(_) => SimpleLogger.info(s"Super-batch ${superIndex + 1}: Thread ${batchIndex + 1} finished DB insert.")
                  case Failure(e) => SimpleLogger.error(s"Super-batch ${superIndex + 1}: Thread ${batchIndex + 1} DB failure - ${e.getMessage}")
                }
              case Failure(e) =>
                SimpleLogger.error(s"Thread failed to connect to DB: ${e.getMessage}")
            }
          }
        }.toList // .toList is required here to actually trigger the evaluation of the Futures

        // 4. Wait for all threads in this Super-Batch to finish before pulling the next 50,000 rows
        Await.result(Future.sequence(futures), Duration.Inf)
        SimpleLogger.info(s"Completed all parallel tasks for Super-Batch ${superIndex + 1}.")
      }
    } match {
      case Success(_) => SimpleLogger.info("Finished processing all batches completely.")
      case Failure(e) => SimpleLogger.error(s"File streaming failed catastrophically: ${e.getMessage}")
    }
  }

  // Main Execution
  processMassiveFileParallel("src/Resources/TRX10M.csv")

  SimpleLogger.info("Engine Ended.")
}

