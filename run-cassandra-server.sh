JAR=/home/kyeh/cs/git/gt-admin/server/target/scala-2.10/gt-admin-server-assembly-0.1.0-SNAPSHOT.jar

zip -d $JAR META-INF/ECLIPSEF.RSA
zip -d $JAR META-INF/ECLIPSEF.SF

spark-submit \
--class geotrellis.admin.server.CassandraCatalogService \
--conf spark.executor.memory=8g --master local[4] --driver-memory=2g \
$JAR \
--host 127.0.0.1 --keyspace ingest
