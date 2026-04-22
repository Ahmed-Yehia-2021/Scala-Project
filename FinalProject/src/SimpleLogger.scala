import java.io.{BufferedWriter, FileWriter}
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import scala.util.{Try, Using}

object SimpleLogger {

  private val logFile = "rules_engine.log"
  private val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")

  def log(level: String, message: String): Try[Unit] = {
    val timestamp = LocalDateTime.now().format(formatter)
    val logLine = s"$timestamp $level $message"

    Using(new BufferedWriter(new FileWriter(logFile, true))) { bw =>
      bw.write(logLine)
      bw.newLine()
    }
  }

  def info(message: String): Unit = log("INFO", message)
  def error(message: String): Try[Unit] = log("ERROR", message)
}