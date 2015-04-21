package geotrellis.admin.server

import akka.actor.ActorSystem
import com.github.nscala_time.time.Imports._
import com.quantifind.sumac.{ ArgApp, ArgMain }
import geotrellis.proj4._
import geotrellis.raster._
import geotrellis.raster.io.geotiff.Nodata
import geotrellis.raster.render._
import geotrellis.spark._
import geotrellis.spark.cmd.args._
import geotrellis.spark.io._
import geotrellis.spark.io.accumulo._
import geotrellis.spark.io.cassandra._
import com.datastax.spark.connector.cql.CassandraConnector
import geotrellis.spark.io.hadoop._
import geotrellis.spark.io.json._
import geotrellis.spark.tiling._
import geotrellis.spark.utils.SparkUtils
import geotrellis.vector._
import geotrellis.vector.reproject._
import org.apache.accumulo.core.client.security.tokens.PasswordToken
import scala.concurrent._
import scala.concurrent.ExecutionContext.Implicits.global
import spray.http.{MediaTypes }
import spray.httpx.SprayJsonSupport._
import spray.json._
import spray.routing._


class CassandraCatalogArgs extends CassandraArgs

/**
 * Catalog and TMS service for TimeRaster layers only
 * This is intended to exercise the machinery more than being a serious service.
 */
object CassandraCatalogService extends ArgApp[CassandraCatalogArgs] with SimpleRoutingApp with CorsSupport with ZonalSummaryRoutes {
  implicit val system = ActorSystem("spray-system")
  implicit val sparkContext = SparkUtils.createSparkContext("Catalog")

  val sparkConf = sparkContext.getConf
  sparkConf.set("spark.cassandra.connection.host", argHolder.host)
  val connector = CassandraConnector(sparkConf)
  implicit val cassandra = CassandraInstance(connector, argHolder.keyspace)  

  val catalog = CassandraRasterCatalog("metadata", "attributes")

  /** Simple route to test responsiveness of service. */
  val pingPong = path("ping")(complete("pong"))

  val layoutScheme = ZoomedLayoutScheme()

  /** Server out TMS tiles for some layer */
  def tmsRoute =
    pathPrefix(Segment / IntNumber / IntNumber / IntNumber) { (layer, zoom, x, y) =>
      parameters('time.?, 'breaks.?) { (timeOption, breaksOption) =>    
        respondWithMediaType(MediaTypes.`image/png`) {
          complete {
            future {
              val zooms = catalog.layerMetaDataCatalog.zoomLevelsFor(layer)

              val tile = 
                if(zooms.contains(zoom)) {
                  val layerId = LayerId(layer, zoom)

                  timeOption match {
                    case Some(timeStr) =>
                      val time = DateTime.parse(timeStr)
                      catalog.tileReader[SpaceTimeKey](layerId).read(SpaceTimeKey(x, y, time))
                    case None =>
                      catalog.tileReader[SpatialKey](layerId).read(SpatialKey(x, y))
                  }
                } else {
                  val z = zooms.max

                  if(zoom > z) {
                    val layerId = LayerId(layer, z)

                    val meta = catalog.layerMetaDataCatalog.read(layerId)
                    val rmd = meta.rasterMetaData

                    val layoutLevel = layoutScheme.levelFor(zoom)
                    val mapTransform = MapKeyTransform(rmd.crs, layoutLevel.tileLayout.layoutCols, layoutLevel.tileLayout.layoutRows)
                    val targetExtent = mapTransform(x, y)
                    val gb @ GridBounds(nx, ny, _, _) = rmd.mapTransform(targetExtent)
                    val sourceExtent = rmd.mapTransform(nx, ny)
                    println(s"SOURCE TILELAYOUT: ${rmd.tileLayout}")
                    println(s"TARGET TILELAYOUT: ${layoutLevel.tileLayout}")
                    println(s"GRIDBOUNDS: $gb")
                    println(s"SOURCE EXTENT $sourceExtent")
                    println(s"TARGET EXTENT $targetExtent")

                    val largerTile = 
                      timeOption match {
                        case Some(timeStr) =>
                          val time = DateTime.parse(timeStr)
                          catalog.tileReader[SpaceTimeKey](layerId).read(SpaceTimeKey(nx, ny, time))
                        case None =>
                          catalog.tileReader[SpatialKey](layerId).read(SpatialKey(nx, ny))
                      }

                    largerTile.resample(sourceExtent, RasterExtent(targetExtent, 256, 256))
                  } else {
                    sys.error("NOPE!")
                  }
                }


              breaksOption match {
                case Some(breaks) =>
                  if(layer == "diff") tile.renderPng(ColorRamps.LightToDarkGreen, breaks.split(",").map(_.toInt)).bytes
                  else tile.renderPng(ColorRamps.BlueToOrange, breaks.split(",").map(_.toInt)).bytes
                case None =>
                  tile.renderPng.bytes  
              }              
            }
          }
        }
      }
    }

  def catalogRoute = cors {
    path("") {
      get {
        // get the entire catalog
        complete {
          import DefaultJsonProtocol._

          catalog.layerMetaDataCatalog.fetchAll.toSeq.map {
            case (key, CassandraLayerMetaData(md, histogram, _, _)) =>
              println(s"Loading $key")
              val layer = key
              val center = md.extent.reproject(md.crs, LatLng).center
              val breaks = histogram.get.getQuantileBreaks(12)

              JsObject(
                "layer" -> layer.toJson,
                "metaData" -> md.toJson,
                "center" -> List(center.x, center.y).toJson,
                "breaks" -> breaks.toJson
              )
          }
        }
      }
    } ~ 
    pathPrefix(Segment / IntNumber) { (name, zoom) =>      
      val layer = LayerId(name, zoom)                                      
      val Some(CassandraLayerMetaData(md, _, _, _)) = catalog.layerMetaDataCatalog.fetchAll.get(layer)
      (path("bands") & get) { 
        import DefaultJsonProtocol._
        complete{ future {          
          /*val bands = {
            val GridBounds(col, row, _, _) = md.mapTransform(md.extent)
            val filters = new FilterSet[SpaceTimeKey]()// withFilter SpaceFilter(GridBounds(col, row, col, row))
            catalog.reader[SpaceTimeKey].read(layer, filters).map {
              case (key, tile) => key.temporalKey.time.toString
            }
          }.collect*/
          JsObject("time" -> null)
        } }
      } ~ 
      (path("breaks") & get) {
        parameters('num ? "10") { num => 
          import DefaultJsonProtocol._ 
          complete { future {                      
            
            (if (layer.name == "NLCD")
              Histogram(catalog.reader[SpatialKey].read(layer))
            else
              Histogram(catalog.reader[SpaceTimeKey].read(layer))
            ).getQuantileBreaks(num.toInt)
          } }
        }
      }
    }
  }

  def timedCreate[T](startMsg:String,endMsg:String)(f:() => T):T = {
    println(startMsg)
    val s = System.currentTimeMillis
    val result = f()
    val e = System.currentTimeMillis
    val t = "%,d".format(e-s)
    println(s"\t$endMsg (in $t ms)")
    result
  }

  def pixelRoute = cors {
    import DefaultJsonProtocol._
    import org.apache.spark.SparkContext._

    path("pixel") {
      get {
        parameters('name, 'zoom.as[Int], 'x.as[Double], 'y.as[Double]) { (name, zoom, x, y) =>
          val layer = LayerId(name, zoom)
          val CassandraLayerMetaData(md, _, _, _) = catalog.layerMetaDataCatalog.read(layer)
          val crs = md.crs

          val p = Point(x, y).reproject(LatLng, crs)
          val key = md.mapTransform(p)
          val rdd = catalog.reader[SpaceTimeKey].read(layer, FilterSet(SpaceFilter[SpaceTimeKey](key.col, key.row)))
          val bcMetaData = rdd.sparkContext.broadcast(rdd.metaData)

          def createCombiner(value: Double): (Double, Double) =
            (value, 1)
          def mergeValue(acc: (Double, Double), value: Double): (Double, Double) =
            (acc._1 + value, acc._2 + 1)
          def mergeCombiners(acc1: (Double, Double), acc2: (Double, Double)): (Double, Double) =
            (acc1._1 + acc2._1, acc1._2 + acc2._2)

          complete {
            val data: Array[(DateTime, Double)] =
            timedCreate("Starting calculation", "End calculation") {
              rdd
                .mapKeys { key => key.updateTemporalComponent(key.temporalKey.time.withMonthOfYear(1).withDayOfMonth(1).withHourOfDay(0)) }
                .map { case (key, tile) =>
                  val md = bcMetaData.value
                  val (col, row) = RasterExtent(md.mapTransform(key), tile.cols, tile.rows).mapToGrid(p.x, p.y)
                  (key, tile.getDouble(col, row))
                 }
                .combineByKey(createCombiner, mergeValue, mergeCombiners)
                .map { case (key, (sum, count)) =>
                  (key.temporalKey.time, sum / count)
                 }
                .collect
            }

            JsArray(JsObject(
              "model" -> JsString(name),
              "data" -> JsArray(
                data.map { case (year, value) =>
                  JsObject(
                    "year" -> JsString(year.toString),
                    "value" -> JsNumber(value)
                  )
                }: _*
              )
            ))
          }
        }
      }
    }
  }

/*
  def vectorRoute = 
    cors {
      import DefaultJsonProtocol._
      path("") {
        get {
          complete { 
            scala.io.Source.fromFile("/Users/rob/proj/climate/climate-viewer/layer.json").getLines.mkString.parseJson.asJsObject
          }
        }
      }
    }
*/
  def root = {
    pathPrefix("catalog") { catalogRoute } ~
      pathPrefix("tms") { tmsRoute } ~
      pathPrefix("stats") { zonalRoutes(catalog) } ~
      //pathPrefix("vector") { vectorRoute } ~
      pixelRoute
  }

  startServer(interface = "0.0.0.0", port = 8088) {
    pingPong ~ root
  }
}
