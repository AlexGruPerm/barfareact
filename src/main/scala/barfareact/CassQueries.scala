package barfareact

trait CassQueries {

  val sqlBCalcProps = " select * from mts_meta.bars_property "

  val queryMaxDateFa = """ select max(ddate) as faLastDate
                         |   from mts_bars.bars_fa
                         |  where ticker_id     = :pTickerId and
                         |        bar_width_sec = :pBarWidthSec
                         |  allow filtering """.stripMargin

  val queryMinDateBar = """ select min(ddate) as mindate
                          |   from mts_bars.bars_bws_dates
                          |  where ticker_id     = :pTickerId and
                          |        bar_width_sec = :pBarWidthSec """.stripMargin

  val queryReadBarsOneDate = """ select ts_begin,ts_end,o,h,l,c
                               |   from mts_bars.bars
                               |  where ticker_id     = :pTickerId and
                               |        bar_width_sec = :pBarWidthSec and
                               |        ddate         = :pDate
                               |  order by ts_end; """.stripMargin

  val querySaveFa =
    """ insert into mts_bars.bars_fa(
                                      ticker_id,
                                      ddate,
                                      bar_width_sec,
                                      ts_end,
                                      c,
                                      log_oe,
                                      ts_end_res,
                                      dursec_res,
                                      ddate_res,
                                      c_res,
                                      res_type)
                               values(
                                       :p_ticker_id,
                                       :p_ddate,
                                       :p_bar_width_sec,
                                       :p_ts_end,
                                       :p_c,
                                       :p_log_oe,
                                       :p_ts_end_res,
                                       :p_dursec_res,
                                       :p_ddate_res,
                                       :p_c_res,
                                       :p_res_type
                                      ) """

}