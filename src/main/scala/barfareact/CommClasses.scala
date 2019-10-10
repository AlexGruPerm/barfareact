package barfareact

import java.time.LocalDate

final case class CassConnectException(private val message: String = "",
                                      private val cause: Throwable = None.orNull) extends Exception(message, cause)

case class BarProperty(tickerId: Int,
                       bws: Int,
                       isEnabled: Int){
  def unapply :(Int,Int) =
    (this.tickerId, this.bws)
}

case class barsForFutAnalyze(
                              tickerId    :Int,
                              barWidthSec :Int,
                              dDate       :LocalDate,
                              ts_begin    :Long,
                              ts_end      :Long,
                              o           :Double,
                              h           :Double,
                              l           :Double,
                              c           :Double
                            ){
  val minOHLC = Seq(o,h,l,c).min
  val maxOHLC = Seq(o,h,l,c).max
}

case class barsFutAnalyzeRes(
                              srcBar   : barsForFutAnalyze,
                              resAnal  : Option[barsForFutAnalyze],
                              p        : Double,
                              resType  : String
                            )

case class barsResToSaveDB(
                            tickerId     :Int,
                            dDate        :LocalDate,
                            barWidthSec  :Int,
                            ts_end       :Long,
                            c            :Double,
                            log_oe       :Double,
                            ts_end_res   :Long,
                            dursec_res   :Int,
                            ddate_res    :LocalDate,
                            c_res        :Double,
                            res_type     :String
                          )




