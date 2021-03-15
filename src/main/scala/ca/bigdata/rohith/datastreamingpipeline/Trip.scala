package ca.bigdata.rohith.datastreamingpipeline

case class Trip(
                 start_date: String,
                 start_station_code : Int,
                 end_date : String,
                 end_station_code: Int,
                 duration_sec :Int,
                 is_member :Int
               )

object Trip {
  def apply(csv: String): Trip = {
    val a = csv.split(",")
    Trip(a(0), a(1).toInt, a(2), a(3).toInt, a(4).toInt,a(5).toInt)
  }
}
