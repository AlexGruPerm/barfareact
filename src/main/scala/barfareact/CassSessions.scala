package barfareact

import java.net.InetSocketAddress
import java.time.LocalDate

import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.cql.{BatchStatement, BoundStatement, DefaultBatchType, Row}
import com.typesafe.config.{Config, ConfigFactory}
import org.slf4j.LoggerFactory

import scala.jdk.CollectionConverters._

trait CassSession extends CassQueries {
  val log = LoggerFactory.getLogger(getClass.getName)

  val config :Config = ConfigFactory.load()
  val confConnectPath :String = "cassandra.connection."

  def getNodeAddressDc(path :String) :(String,String) =
    (config.getString(confConnectPath+"ip"),
      config.getString(confConnectPath+"dc"))

  def createSession(node :String,dc :String,port :Int = 9042) :CqlSession =
    CqlSession.builder()
      .addContactPoint(new InetSocketAddress(node, port))
      .withLocalDatacenter(dc).build()

  def prepareSql(sess :CqlSession,sqlText :String) :BoundStatement =
    try {
      sess.prepare(sqlText).bind()
    }
     catch {
      case e: com.datastax.oss.driver.api.core.servererrors.SyntaxError =>
        log.error(" prepareSQL - "+e.getMessage)
        throw e
    }
}


object CassSessionInstance extends CassSession{
  private val (node :String,dc :String) = getNodeAddressDc("src")
  log.info("CassSessionInstance DB Address : "+node+" - "+dc)
  val sess :CqlSession = createSession(node,dc)
  log.info("CassSessionInstance session is connected = " + !sess.isClosed)

  private val prepBCalcProps :BoundStatement = prepareSql(sess,sqlBCalcProps)
  private val prepMaxDateFa: BoundStatement = prepareSql(sess, queryMaxDateFa)
  private val prepMinDateBar: BoundStatement = prepareSql(sess, queryMinDateBar)
  private val prepReadBarsOneDate: BoundStatement = prepareSql(sess, queryReadBarsOneDate)
  private val prepSaveFa: BoundStatement = prepareSql(sess, querySaveFa)

  implicit def LocalDateToOpt(v :LocalDate) :Option[LocalDate] = Option(v)

  lazy val getFaLastDate :(Int, Int) => Option[LocalDate] = (tickerID, barWidthSec) =>
    sess.execute(prepMaxDateFa
      .setInt("pTickerId", tickerID)
      .setInt("pBarWidthSec", barWidthSec))
      .one().getLocalDate("faLastDate")

  lazy val getBarMinDate :(Int, Int) => Option[LocalDate] = (tickerID, barWidthSec) =>
    sess.execute(prepMinDateBar
      .setInt("pTickerId", tickerID)
      .setInt("pBarWidthSec", barWidthSec))
      .one().getLocalDate("mindate")

  val rowToBarData : (Row, Int, Int, LocalDate) => barsForFutAnalyze =
    (row: Row, tickerID: Int, barWidthSec: Int, dDate: LocalDate) =>
    barsForFutAnalyze(
      tickerID,
      barWidthSec,
      dDate,
      row.getLong("ts_begin"),
      row.getLong("ts_end"),
      row.getDouble("o"),
      row.getDouble("h"),
      row.getDouble("l"),
      row.getDouble("c")
    )


  /** Read bars from DB beginning from startReadDate if Some,
    * otherwise return empty Seq.
    */
  def readBars(tickerID: Int, barWidthSec: Int, startReadDate: Option[LocalDate], cntDays: Integer = 1): Seq[barsForFutAnalyze] =
    startReadDate match {
      case Some(startDate) =>
        (0 until cntDays).flatMap {
          addDays =>
            sess.execute(prepReadBarsOneDate
              .setInt("pTickerId", tickerID)
              .setInt("pBarWidthSec", barWidthSec)
              .setLocalDate("pDate", startDate.plusDays(addDays)))
              .all()
              .iterator.asScala.toSeq.map(r => rowToBarData(r, tickerID, barWidthSec, startDate.plusDays(addDays)))
              .sortBy(sr => (sr.dDate, sr.ts_end))
        }
      case None => {
        log.info("There is no start point to read bars. startReadDate = None")
        Nil}
    }


  def makeAnalyze(seqB: Seq[barsForFutAnalyze], p: Double): Seq[barsFutAnalyzeRes] = {
    for (
      currBarWithIndex <- seqB.zipWithIndex;
      currBar = currBarWithIndex._1;
      searchSeq = seqB.drop(currBarWithIndex._2 + 1)
    ) yield {
      val pUp: Double = (Math.exp(Math.log(currBar.c) + p) * 10000).round / 10000.toDouble
      val pDw: Double = (Math.exp(Math.log(currBar.c) - p) * 10000).round / 10000.toDouble

      searchSeq.find(srcElm => (pUp >= srcElm.minOHLC && pUp <= srcElm.maxOHLC) ||
        (pDw >= srcElm.minOHLC && pDw <= srcElm.maxOHLC) ||
        (pUp <= srcElm.maxOHLC && pDw >= srcElm.minOHLC)
      ) match {
        case Some(fBar) =>
          if (pUp >= fBar.minOHLC && pUp <= fBar.maxOHLC) barsFutAnalyzeRes(currBar, Some(fBar), p, "mx")
          else if (pDw >= fBar.minOHLC && pDw <= fBar.maxOHLC) barsFutAnalyzeRes(currBar, Some(fBar), p, "mn")
          else if (pUp <= fBar.maxOHLC && pDw >= fBar.minOHLC) barsFutAnalyzeRes(currBar, Some(fBar), p, "bt")
          else barsFutAnalyzeRes(currBar, None, p, "nn")
        case None => barsFutAnalyzeRes(currBar, None, p, "nn")
      }
    }
  }

  def saveBarsFutAnal(seqFA: Seq[barsResToSaveDB]): Unit = {
    val seqBarsParts = seqFA.grouped(500)
    for (thisPartOfSeq <- seqBarsParts) {
      val builder = BatchStatement.builder(DefaultBatchType.UNLOGGED)
      thisPartOfSeq.foreach {
        t =>
          builder.addStatement(prepSaveFa
            .setInt("p_ticker_id", t.tickerId)
            .setLocalDate("p_ddate", t.dDate)
            .setInt("p_bar_width_sec", t.barWidthSec)
            .setLong("p_ts_end", t.ts_end)
            .setDouble("p_c", t.c)
            .setDouble("p_log_oe", t.log_oe)
            .setLong("p_ts_end_res", t.ts_end_res)
            .setInt("p_dursec_res", t.dursec_res)
            .setLocalDate("p_ddate_res", t.ddate_res)
            .setDouble("p_c_res", t.c_res)
            .setString("p_res_type", t.res_type))
      }
      try {
        val batch = builder.build()
        log.trace("saveBarsFutAnal  before execute batch.size()=" + batch.size())
        sess.execute(batch)
        batch.clear()
      } catch {
        case e: com.datastax.oss.driver.api.core.DriverException => log.error(s"DriverException when save bars ${e.getMessage} ${e.getCause}")
          throw e
      }
    }
  }

  lazy val getAllBarsProperties : () => Seq[BarProperty] = () =>
    sess.execute(prepBCalcProps).all().iterator.asScala.map(
    row => BarProperty(
      row.getInt("ticker_id"),
      row.getInt("bar_width_sec"),
      row.getInt("is_enabled")
    )).toList.filter(bp => bp.isEnabled==1)

  /*
  def getAllBarsProperties : Seq[BarProperty] =
    sess.execute(prepBCalcProps).all().iterator.asScala.map(
      row => BarProperty(
        row.getInt("ticker_id"),
        row.getInt("bar_width_sec"),
        row.getInt("is_enabled")
      )).toList.filter(bp => bp.isEnabled==1)
*/

  def convertFaDataToSaveData(futAnalRes: Seq[barsFutAnalyzeRes]) : Seq[barsResToSaveDB] =
    futAnalRes
      .filter(elm => elm.resAnal.isDefined)
      .map(r => barsResToSaveDB(
        r.srcBar.tickerId,
        r.srcBar.dDate,
        r.srcBar.barWidthSec,
        r.srcBar.ts_end,
        r.srcBar.c,
        r.p,
        r.resAnal match {
          case Some(ri) => ri.ts_end
        },
        r.resAnal match {
          case Some(ri) => Math.round((ri.ts_end - r.srcBar.ts_end)/1000L).toInt
        },
        r.resAnal match {
          case Some(ri) => ri.dDate
        },
        r.resAnal match {
          case Some(ri) => ri.c
        },
        r.resType
      ))

}

