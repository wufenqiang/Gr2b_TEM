package com.weather.bigdata.mt.signal

import java.util.Date

import com.weather.bigdata.it.nc_grib.core.SciDatasetCW
import com.weather.bigdata.it.spark.platform.signal.JsonStream
import com.weather.bigdata.mt.basic.mt_commons.business.Fc.FcMainCW
import com.weather.bigdata.mt.basic.mt_commons.business.Ocf.OcfReviseCW
import com.weather.bigdata.mt.basic.mt_commons.business.Wea.WeaMainCW
import com.weather.bigdata.mt.basic.mt_commons.commons.BroadcastUtil.ShareData
import com.weather.bigdata.mt.basic.mt_commons.commons.ReadWriteUtil.ReadFile
import com.weather.bigdata.mt.basic.mt_commons.commons.StreamUtil.{MapStream, SignalStream}
import com.weather.bigdata.mt.basic.mt_commons.commons.WeatherFunUtil.WeatherDate
import com.weather.bigdata.mt.basic.mt_commons.commons.{Constant, PropertiesUtil}
import org.apache.spark.rdd.RDD

import scala.collection.mutable

object Gr2b_TEM {
  private val gr2bFrontOpen: Boolean = PropertiesUtil.gr2bFrontOpen
  private val timeSteps = Constant.TimeSteps
  def main(signalJson:String,splitFile:String): Unit ={
    // /站点数据
    val StnIdGetLatLon: mutable.HashMap[String,String]=ReadFile.ReadStationInfoMap_IdGetlatlon_stainfo_nat
    val (dataType: String, applicationTime: Date, generationFileName: String, timeStamp: Date, attribute: String) = JsonStream.analysisJsonStream(signalJson)

    val fcType: String = dataType.split("_").last
    val fcdate: Date = applicationTime
    val stationInfos_fc: (mutable.HashMap[String, String], String, Date) = (StnIdGetLatLon, PropertiesUtil.stationfc, fcdate)


    //信号收集池
    val typeSetfileName: String = PropertiesUtil.gettypeSetfileName(fcdate, splitFile)
    val TypeMap: mutable.HashMap[(String, Date), (String, Date, String)] = MapStream.readMap(typeSetfileName)
    //对信号池中信号分类
    val (typeMap_Gr2b, typeMap_Ocf1h, typeMap_Ocf12h, typeMap_Obs, typeMap_other) = MapStream.sortTypeMap(TypeMap)
    val containsWea0FcType: Boolean = TypeMap.contains((Constant.PREPathKey, fcdate)) && TypeMap.contains((Constant.CLOUDPathKey, fcdate))

    //加载attribute信息
    PropertiesUtil.putPropertiesHashMap_json(fcType, attribute)
    val ExtremMap = Map( ): Map[String, Map[String, Array[Float]]]

    val fcweaflag :(Boolean,Boolean)= {
      if(typeMap_Ocf1h.isEmpty && typeMap_Ocf12h.isEmpty){
        if(!containsWea0FcType){
          val flag0=FcMainCW.parallelfc_ReturnFlag(splitFile, fcType, fcdate, generationFileName, ExtremMap, stationInfos_fc, gr2bFrontOpen, this.timeSteps)
          (flag0,false)
        }else{
          val rdd0:RDD[SciDatasetCW]=FcMainCW.parallelfc_ReturnRDD(splitFile, fcType, fcdate, generationFileName, ExtremMap, stationInfos_fc, gr2bFrontOpen, this.timeSteps)
          val flag0=WeaMainCW.afterFcWea0_ReturnFlag(rdd0, fcdate, stationInfos_fc)
          (flag0,flag0)
        }
      }else if((!typeMap_Ocf1h.isEmpty) && (!typeMap_Ocf12h.isEmpty)){
        val sciGeo=ShareData.sciGeoCW_ExistOrCreat
        val ocf1hsignal = MapStream.getLastest(typeMap_Ocf1h)
        val ocf12hsignal = MapStream.getLastest(typeMap_Ocf12h)
        val ocfdate1h = ocf1hsignal._1._2
        val ocffile1h = ocf1hsignal._2._1
        val ocfdate12h = ocf12hsignal._1._2
        val ocffile12h = ocf12hsignal._2._1
        val ocffileArr: Array[(Double, Date, String)]={
          timeSteps.map(timeStep=>{
            val ocfdate:Date={
              if(timeStep==1.0d){
                ocfdate1h
              }else if(timeStep==12.0d){
                ocfdate12h
              }else{
                null
              }
            }
            val ocffile:String={
              if(timeStep==1.0d){
                ocffile1h
              }else if(timeStep==12.0d){
                ocffile12h
              }else{
                null
              }
            }
            (timeStep,ocfdate,ocffile)
          })
        }
        //TEM 主流程
        val rdd0:RDD[SciDatasetCW]=FcMainCW.parallelfc_ReturnRDD(splitFile, fcType, fcdate, generationFileName, ExtremMap, stationInfos_fc, gr2bFrontOpen, this.timeSteps)
        //TEM ocf订正
        val rdd1:RDD[SciDatasetCW]=OcfReviseCW.ReturnData_plusStation(rdd0,sciGeo,ocffileArr,stationInfos_fc)
        //TEM wea重算
        val rdd2:RDD[SciDatasetCW]=WeaMainCW.afterFcWea0_ReturnRdd(rdd1,fcdate,stationInfos_fc)
        //Wea ocf订正
        val flag0=WeaMainCW.afterFcOcfWea2(rdd2,sciGeo,ocffileArr,stationInfos_fc)
        (flag0,flag0)
      }else{
        val e:Exception=new Exception("ocf信号出现故障0,请查明原因")
        e.printStackTrace()

        //(typeMap_Ocf1h.isEmpty || typeMap_Ocf12h.isEmpty)
        //上一个预报信号收集池
        val typeSetfileName1: String = PropertiesUtil.gettypeSetfileName(WeatherDate.previousFCDate(fcdate), splitFile)
        val TypeMap1: mutable.HashMap[(String, Date), (String, Date, String)] = MapStream.readMap(typeSetfileName1)
        val (typeMap_Gr2b1, typeMap_Ocf1h1, typeMap_Ocf12h1, typeMap_Obs1, typeMap_other1) = MapStream.sortTypeMap(TypeMap1)
        if(!typeMap_Ocf1h1.isEmpty && !typeMap_Ocf12h1.isEmpty){
          val rdd0:RDD[SciDatasetCW]=FcMainCW.parallelfc_ReturnRDD(splitFile, fcType, fcdate, generationFileName, ExtremMap, stationInfos_fc, gr2bFrontOpen, this.timeSteps)
          val rdd1:RDD[SciDatasetCW]=WeaMainCW.afterFcWea0_ReturnRdd(rdd0,fcdate,stationInfos_fc)
          val sciGeo=ShareData.sciGeoCW_ExistOrCreat

          val ocf1hsignal = MapStream.getLastest(typeMap_Ocf1h1)
          val ocf12hsignal = MapStream.getLastest(typeMap_Ocf12h1)
          val ocfdate1h = ocf1hsignal._1._2
          val ocffile1h = ocf1hsignal._2._1
          val ocfdate12h = ocf12hsignal._1._2
          val ocffile12h = ocf12hsignal._2._1

          val ocffileArr: Array[(Double, Date, String)]={
            timeSteps.map(timeStep=>{
              val ocfdate:Date={
                if(timeStep==1.0d){
                  ocfdate1h
                }else if(timeStep==12.0d){
                  ocfdate12h
                }else{
                  null
                }
              }
              val ocffile:String={
                if(timeStep==1.0d){
                  ocffile1h
                }else if(timeStep==12.0d){
                  ocffile12h
                }else{
                  null
                }
              }
              (timeStep,ocfdate,ocffile)
            })
          }

          val flag0= WeaMainCW.afterFcOcfWea2(rdd1,sciGeo,ocffileArr,stationInfos_fc)
          (flag0,flag0)
        }else{
          val e:Exception=new Exception("ocf信号出现故障1,请查明原因")
          e.printStackTrace()
          val rdd0:RDD[SciDatasetCW]=FcMainCW.parallelfc_ReturnRDD(splitFile, fcType, fcdate, generationFileName, ExtremMap, stationInfos_fc, gr2bFrontOpen, this.timeSteps)
          val flag0= WeaMainCW.afterFcWea0_ReturnFlag(rdd0, fcdate, stationInfos_fc)
          (flag0,flag0)
        }
      }
    }
    if (fcweaflag._1) {
      SignalStream.writeObssignal(splitFile,fcdate, fcType, timeSteps)
      SignalStream.reduceLatLonsignal(splitFile,fcdate,fcType,this.timeSteps)

      if(fcweaflag._2){
        SignalStream.writeObssignal(splitFile,fcdate, Constant.WEATHER, this.timeSteps)
        SignalStream.reduceLatLonsignal(splitFile,fcdate,Constant.WEATHER,timeSteps)
      }

      //更新数据信号收集池
      MapStream.updateMap(signalJson,splitFile)
    }
  }
}
