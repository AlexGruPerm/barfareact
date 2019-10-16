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
  private val prepAllDatesByTickBws :BoundStatement = prepareSql(sess,sqlAllDatesByTickBws)
  private val prepMaxDateFa: BoundStatement = prepareSql(sess, queryMaxDateFa)
  private val prepMinDateBar: BoundStatement = prepareSql(sess, queryMinDateBar)
  private val prepReadBarsAll: BoundStatement = prepareSql(sess, queryReadBarsAll)
  private val prepReadBarsOneDate: BoundStatement = prepareSql(sess, queryReadBarsOneDate)
  private val prepSaveFa: BoundStatement = prepareSql(sess, querySaveFa)
  private val prepAllFaDateByTickerBws: BoundStatement = prepareSql(sess, queryAllFaDateByTickerBws)
  private val prepFaCntPerPrimKey: BoundStatement = prepareSql(sess, queryFaCntPerPrimKey)
  private val prepBarCntPerPrimKey: BoundStatement = prepareSql(sess, queryBarCntPerPrimKey)

  implicit def LocalDateToOpt(v :LocalDate) :Option[LocalDate] = Option(v)

  lazy val getAllBarsProperties : () => Seq[BarProperty] = () =>
    sess.execute(prepBCalcProps).all().iterator.asScala.map(
      row => BarProperty(
        row.getInt("ticker_id"),
        row.getInt("bar_width_sec"),
        row.getInt("is_enabled")
      )).toList.filter(bp => bp.isEnabled==1)

  lazy val getAllDates : (Int,Int) => Seq[LocalDate] = (tickerID, barWidthSec) =>
    sess.execute(prepAllDatesByTickBws
      .setInt("pTickerId", tickerID)
      .setInt("pBarWidthSec", barWidthSec))
      .all().iterator.asScala.map(row => row.getLocalDate("ddate")
    ).toList.distinct.sortBy(ld => ld)

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

  lazy val getAllFaDates :(Int,Int) => Seq[LocalDate] = (tickerID, barWidthSec) =>
    sess.execute(prepAllFaDateByTickerBws
      .setInt("pTickerId", tickerID)
      .setInt("pBarWidthSec", barWidthSec))
      .all().iterator.asScala.map(row => row.getLocalDate("ddate")
    ).toList.distinct.sortBy(ld => ld)

  lazy val getFaCntPerPrimKey :(Int,Int,LocalDate) => Long = (tickerID, barWidthSec, thisDate) =>
    Option(sess.execute(prepFaCntPerPrimKey
      .setInt("pTickerId", tickerID)
      .setInt("pBarWidthSec", barWidthSec)
      .setLocalDate("ddate",thisDate))
      .one().getLong("cnt")).getOrElse(0L)

  lazy val getBarCntPerPrimKey :(Int,Int,LocalDate) => Long = (tickerID, barWidthSec, thisDate) =>
    Option(sess.execute(prepBarCntPerPrimKey
      .setInt("pTickerId", tickerID)
      .setInt("pBarWidthSec", barWidthSec)
      .setLocalDate("ddate",thisDate))
      .one().getLong("cnt")).getOrElse(0L)

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

  val rowToBarDataWthoDate : (Row, Int, Int) => barsForFutAnalyze =
    (row: Row, tickerID: Int, barWidthSec: Int) =>
      barsForFutAnalyze(
        tickerID,
        barWidthSec,
        row.getLocalDate("ddate"),
        row.getLong("ts_begin"),
        row.getLong("ts_end"),
        row.getDouble("o"),
        row.getDouble("h"),
        row.getDouble("l"),
        row.getDouble("c")
      )

  /** Read bars from DB beginning from startReadDate if Some,
    * otherwise return empty Seq.
    * We need compare performance
    * 1) read date by date all bars (175 ddate for 11.10.2019)
    * 2) read all bars in one query
    * Tests:
    *
    *
    */
  def readBars(tickerID: Int, barWidthSec: Int, startReadDate: Option[LocalDate], distDates :Seq[LocalDate]): Seq[barsForFutAnalyze] =
    startReadDate match {
      //read all bars from startDate to the last date.
      case Some(startDate)  =>
        distDates
          .filter(dat => dat == startDate || dat.isAfter(startDate))
          .flatMap {
            thisDate =>
              sess.execute(prepReadBarsOneDate
                .setInt("pTickerId", tickerID)
                .setInt("pBarWidthSec", barWidthSec)
                .setLocalDate("pDate", thisDate)).all
                .iterator.asScala.toSeq.map(r => rowToBarData(r, tickerID, barWidthSec, thisDate))
                .sortBy(sr => (sr.dDate, sr.ts_end))
        }
      //read all bars from first date to last date.
      case Some(_)  =>
        sess.execute(prepReadBarsAll
          .setInt("pTickerId", tickerID)
          .setInt("pBarWidthSec", barWidthSec)).all
          .iterator.asScala.toSeq.map(r => rowToBarDataWthoDate(r, tickerID, barWidthSec))
          .sortBy(sr => (sr.dDate, sr.ts_end))
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

  /**
    * For optimal uasge of batch unlogged pwrites.
    * we need divide data into partitions blocks before inserts.
    * OR:
    * Unlogged batch covering XX partitions detected against table [mts_bars.bars_fa].
    * You should use a logged batch for atomicity, or asynchronous writes for performance.
    *
    *  mts_bars.bars_fa  PRIMARY KEY (( ticker_id, ddate, bar_width_sec ), ...
    *
  */
  def saveBarsFutAnal(seqFA: Seq[barsResToSaveDB]): Unit = {
    //Count of seconds in day divide on bws (Bar width in seconds)
    val batchSizeAsFuncFromBws: Int =
      seqFA.headOption match {
        case Some(bar) => 86400 / bar.barWidthSec
        case None => 0
      }
    /**
      * Iterate by each distinct date.
    */
    seqFA.map(p => p.dDate).distinct.collect{
      case thisDate =>
      val seqBarsParts = seqFA.filter(b => b.dDate==thisDate).grouped(batchSizeAsFuncFromBws)

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
  }




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

