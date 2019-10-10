package barfareact

import java.time.LocalDate
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import com.typesafe.config.Config
import org.slf4j.{Logger, LoggerFactory}

/*
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

*/

class BarFaCalcManager(config :Config, sess :CassSessionInstance.type) {
  val log: Logger = LoggerFactory.getLogger(getClass.getName)

  def run :Unit ={
    val tickerDict :Seq[BarProperty] = sess.getAllBarsProperties

    val (tickerID :Int, barWidthSec :Int) =
      tickerDict.find(p => p.tickerId==1 && p.bws==30).getOrElse(BarProperty(0,0,0)).unapply

    val startReadFrom: Option[LocalDate] = sess.getFaLastDate(tickerID, barWidthSec) match {
      case Some(faLastDate) => Some(faLastDate)
      case _ => sess.getBarMinDate(tickerID, barWidthSec)
    }

    log.info(s"startReadFrom = $startReadFrom  ")


    val seqBars: Seq[barsForFutAnalyze] = sess.readBars (
      tickerID,
      barWidthSec,
      startReadFrom,
      10)

    log.info(s"seqBars.size = ${seqBars.size} ")

    val seqPercents :Seq[Double] = Seq(0.0025)

    val t1FAnal = System.currentTimeMillis
    val futuresFutAnalRes :Seq[Future[Seq[barsFutAnalyzeRes]]] = seqPercents.map(p => Future{sess.makeAnalyze(seqBars, p)})
    val values = Future.sequence(futuresFutAnalRes)
    val futAnalRes: Seq[barsFutAnalyzeRes] = Await.result(values,Duration.Inf).flatten
    val t2FAnal = System.currentTimeMillis
    log.info("After analyze RES.size = " + futAnalRes.size + " Duration " + (t2FAnal - t1FAnal) + " msecs.")

    val resFSave: Seq[barsResToSaveDB] = sess.convertFaDataToSaveData(futAnalRes)

    log.info(s"futAnalRes.size = ${futAnalRes.size} resFSave.size = ${resFSave.size}")

    val t1Save = System.currentTimeMillis
    sess.saveBarsFutAnal(resFSave)
    val t2Save = System.currentTimeMillis
    log.info("Duration of saving into mts_bars.bars_fa - " + (t2Save - t1Save) + " msecs.")

  }

}
