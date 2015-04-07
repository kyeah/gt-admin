package geotrellis.admin.ingest

import geotrellis.spark._
import geotrellis.spark.ingest.NetCDFIngestCommand._
import geotrellis.spark.tiling._
import geotrellis.spark.io.accumulo._
import geotrellis.spark.io.cassandra._
import geotrellis.spark.ingest._
import geotrellis.spark.cmd.args._
import geotrellis.spark.io.hadoop._
import geotrellis.spark.utils.SparkUtils
import geotrellis.raster._
import geotrellis.vector._
import geotrellis.proj4._
import org.apache.accumulo.core.client.security.tokens.PasswordToken
import org.apache.spark._
import com.quantifind.sumac.ArgMain
import com.github.nscala_time.time.Imports._

import com.datastax.spark.connector.cql.CassandraConnector

/** Ingests the chunked NEX GeoTIFF data */
object CassandraNEXIngest extends ArgMain[CassandraIngestArgs] with Logging {
  def main(args: CassandraIngestArgs): Unit = {
    System.setProperty("com.sun.media.jai.disableMediaLib", "true")

    implicit val sparkContext = SparkUtils.createSparkContext("Ingest")

    val sparkConf = sparkContext.getConf
    sparkConf.set("spark.cassandra.connection.host", args.host)

    val connector = CassandraConnector(sparkConf)
    val cassandra = CassandraInstance(connector, args.keyspace)
    sparkContext.setCheckpointDir("/home/kyeh/cs/git/climate/")

    implicit val tiler: Tiler[SpaceTimeInputKey, SpaceTimeKey] = {
      val getExtent = (inKey: SpaceTimeInputKey) => inKey.extent
      val createKey = (inKey: SpaceTimeInputKey, spatialComponent: SpatialKey) =>
        SpaceTimeKey(spatialComponent, inKey.time)

      Tiler(getExtent, createKey)
    }

    val layoutScheme = ZoomedLayoutScheme()

    def layerId(zoom: Int) = LayerId(args.layerName, zoom)

    val save = { (rdd: RasterRDD[SpaceTimeKey], level: LayoutLevel) =>
      cassandra.catalog.save(layerId(level.zoom), args.table, rdd, args.clobber)
    }

    // Get source tiles
    val inPath = args.inPath
    val updatedConf =
      sparkContext.hadoopConfiguration.withInputDirectory(inPath)
    val source = 
      sparkContext.newAPIHadoopRDD(
        updatedConf,
        classOf[SourceTileInputFormat],
        classOf[SpaceTimeInputKey],
        classOf[Tile]
      )

    val (level, rdd) =  Ingest[SpaceTimeInputKey, SpaceTimeKey](source, args.destCrs, layoutScheme)

    if (args.pyramid) {
      Pyramid.saveLevels(rdd, level, layoutScheme)(save)
    } else{
      save(rdd, level)
    }
    
    cassandra.close
  }
}
