package barfareact

import java.time.LocalDate
import java.util.concurrent.Executors

import scala.concurrent.ExecutionContext
//import scala.concurrent.ExecutionContext.Implicits.global
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
  private val log: Logger = LoggerFactory.getLogger(getClass.getName)
  implicit val ec = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(4))
  private val seqPercents: Seq[Double] = Seq(0.0012, 0.0025, 0.0050) //(15,30,60, basic points Fx.)

  private def calcFa(seqPercents: Seq[Double], seqBars: Seq[barsForFutAnalyze]): Seq[barsFutAnalyzeRes] = {
    val futuresFutAnalRes: Seq[Future[Seq[barsFutAnalyzeRes]]] = seqPercents
      .map(p => Future {sess.makeAnalyze(seqBars, p)}(ec))
    Await.result(Future.sequence(futuresFutAnalRes), Duration.Inf).flatten
  }

  def stat() :Unit = {
    log.info(" TCKR_ID  BWS  DistDates    BarsCnt   ReadDur ms.  FaDates  FaCnt")
    sess.getAllBarsProperties()
      .filter(tb => tb.tickerId==1)
      .sortBy(tb => (tb.tickerId,tb.bws)).collect {
      case thisTickerBarProp: BarProperty =>
        val (tickerID: Int, barWidthSec: Int) = thisTickerBarProp.unapply
        val distDates: Seq[LocalDate] = sess.getAllDates(tickerID, barWidthSec)

        val t1 = System.currentTimeMillis
        val barTotalCntByPk: Long = distDates.map(d => sess.getBarCntPerPrimKey(tickerID, barWidthSec, d)).sum
        val distFaDates :Seq[LocalDate] = sess.getAllFaDates(tickerID, barWidthSec)
        val faTotalCntByPK :Long = distFaDates.map(d => sess.getFaCntPerPrimKey(tickerID, barWidthSec, d)).sum
        val t2 = System.currentTimeMillis
        log.info(s" $tickerID        $barWidthSec    ${distDates.size}         $barTotalCntByPk     ${(t2 - t1)/1000}            ${distFaDates.size}    $faTotalCntByPK")
    }
  }

  def run() :Unit = {
    sess.getAllBarsProperties().collect {
      case thisTickerBarProp :BarProperty =>
        val (tickerID: Int, barWidthSec: Int) = thisTickerBarProp.unapply
        log.info(s"TICKER_ID = $tickerID BWS = $barWidthSec")

        val distDates: Seq[LocalDate] = sess.getAllDates(tickerID, barWidthSec)
        log.info(s"There are ${distDates.size} distinct dates. head = ${distDates.headOption}")

        val startReadFrom: Option[LocalDate] = sess.getFaLastDate(tickerID, barWidthSec) match {
          case Some(faLastDate) => Some(faLastDate)
          case _ => sess.getBarMinDate(tickerID, barWidthSec)
        }
        log.info(s"startReadFrom = $startReadFrom  ")

        val tr1 = System.currentTimeMillis
        val seqBars: Seq[barsForFutAnalyze] = sess.readBars(tickerID, barWidthSec, startReadFrom, distDates)
        val tr2 = System.currentTimeMillis
        log.info(s"readBars : seqBars.size = ${seqBars.size} read duration ${(tr2 - tr1)} ms.")

        val t1 = System.currentTimeMillis
        val futAnalRes: Seq[barsFutAnalyzeRes] = calcFa(seqPercents, seqBars)
        val t2 = System.currentTimeMillis
        log.info("Analyze duration " + (t2 - t1) + " ms.")

        val resFSave: Seq[barsResToSaveDB] = sess.convertFaDataToSaveData(futAnalRes)
        log.info(s"futAnalRes.size = ${futAnalRes.size} resFSave.size = ${resFSave.size}")

        val t1S = System.currentTimeMillis
        sess.saveBarsFutAnal(resFSave)
        val t2S = System.currentTimeMillis
        log.info("Duration of saving into mts_bars.bars_fa - " + (t2S - t1S) + " ms.")
    }
  }

}
