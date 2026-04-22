import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit
import scala.util.Try

object Rules {
  def type_quality(order: List[String]) = {
    val category = order(1)
    if( category.contains("Wine") ){ 0.05 }
    else if( category.contains("Cheese") ){ 0.1 }
    else { 0 }
  }

  def unit_quality(order: List[String]) = {
    val unit = order(3).toInt
    if( unit >= 15 ){ 0.1 }
    else if( unit >= 10 ){ 0.07 }
    else if( unit >= 6 ){ 0.05 }
    else { 0 }
  }

  def march_quality(order: List[String]) = {
    val sold_date = order.head
    if( sold_date.contains("-03-23") ){ 0.5 }
    else { 0 }
  }

  def expiry_date_quality(order: List[String]): Double = {
    val curr_date = LocalDate.now().minusYears(3)
    val expiry_date_string = order(2)

    // Fallback parsing: Try "M/d/yyyy", default to "yyyy-MM-dd"
    val expiry_date = Try(LocalDate.parse(expiry_date_string, DateTimeFormatter.ofPattern("M/d/yyyy")))
      .getOrElse(LocalDate.parse(expiry_date_string))

    val days = ChronoUnit.DAYS.between(curr_date, expiry_date)
    if( days > 0 && days < 30 ){ (30-days) / 100.0 }
    else{ 0 }
  }

  def visa_quality(order: List[String]) = {
    if(order.last == "Visa"){ 0.05 }
    else { 0.0 }
  }

  def app_quality(order: List[String]) = {
    val qty = order(3).toInt
    if(order(5) == "App"){
      val mod = qty % 5
      if(mod == 0) { qty / 100.0 }
      else { (qty - mod + 5) / 100.0 }
    }
    else { 0.0 }
  }

  val rules_fn: List[List[String] => Double] = List(type_quality, unit_quality, march_quality, expiry_date_quality, visa_quality, app_quality)
}
