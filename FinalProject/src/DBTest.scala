import java.sql.{Connection, DriverManager, PreparedStatement}
import java.sql.{Date, Timestamp}
import java.time.{Instant, LocalDate}
import java.time.format.DateTimeFormatter
import scala.util.Try

object DBTest {

  val url = "jdbc:mysql://127.0.0.1:3306/iti?useSSL=false&serverTimezone=UTC"
  val userName = "root"
  val password = "root"

  def openConnection(): Try[Connection] = Try {
    Class.forName("com.mysql.cj.jdbc.Driver")
    DriverManager.getConnection(url, userName, password)
  }

  def closeConnection(connection: Connection): Try[Unit] = Try {
    if (connection != null) connection.close()
  }

  def insertBatch(connection: Connection, orders: Seq[List[String]]): Try[Unit] = Try {
    val query =
      """INSERT INTO orders_discounts (
        |timestamp, product_name, expiry_date,
        |quantity, unit_price, channel,
        |payment_method, discount
        |) VALUES (?, ?, ?, ?, ?, ?, ?, ?)""".stripMargin

    val pstmt: PreparedStatement = connection.prepareStatement(query)

    // Disable auto-commit for massive performance boost during batching
    connection.setAutoCommit(false)

    val formatter = DateTimeFormatter.ofPattern("M/d/yyyy")

    orders.foreach { order =>
      val timestamp = Try(Timestamp.from(Instant.parse(order.head)))
        .orElse(Try(Timestamp.valueOf(order.head)))
        .getOrElse(Timestamp.valueOf(order.head + " 00:00:00"))
      val expiryDate = Try(LocalDate.parse(order(2), DateTimeFormatter.ofPattern("M/d/yyyy")))
        .getOrElse(LocalDate.parse(order(2)))

      pstmt.setTimestamp(1, timestamp)
      pstmt.setString(2, order(1))
      pstmt.setDate(3, Date.valueOf(expiryDate))
      pstmt.setInt(4, order(3).toInt)
      pstmt.setDouble(5, order(4).toDouble)
      pstmt.setString(6, order(5))
      pstmt.setString(7, order(6))
      pstmt.setDouble(8, order(7).toDouble)

      pstmt.addBatch() // Add to the batch payload instead of executing immediately
    }

    pstmt.executeBatch() // Send the whole chunk at once
    connection.commit()  // Commit the transaction
    pstmt.close()
  }
}