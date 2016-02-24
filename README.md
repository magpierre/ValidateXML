# ValidateXML

ValidateXML is a small Scala (Spark Driver side) package to do Schema validations of XML files vs a set of XSD's. The small library uses
SAX XSD validation and expect to get to the files using the HDFS-API.

The end-result is a comma separated string of paths to files validated and ready to be loaded with Spark XML.
This might move into the distributed world later but for now it will execute purely in the Driver.

Simple code example:
import com.mapr.xml._

...

val validator = new ValidateXML(sc.hadoopConfiguration)
val input_files = validator.validate("maprfs:///data/landing","maprfs:///data/landing/XSD”)
val base_rdd = sqlContext.read.format("xml").option("rowTag”,”whatever”).load(input_files)


