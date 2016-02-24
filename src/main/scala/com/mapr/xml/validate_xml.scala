
package com.mapr.xml

import javax.xml.validation._
import javax.xml.transform.stream.StreamSource
import javax.xml.parsers.{SAXParser, SAXParserFactory}
import javax.xml.validation.{Schema, ValidatorHandler }
import org.xml.sax.{XMLReader, InputSource}
import javax.xml.parsers.{SAXParser,SAXParserFactory}
import java.io.{FileInputStream, IOException }
import org.apache.hadoop.fs._

class ValidateXML(conf: org.apache.hadoop.conf.Configuration) { 
val fs = org.apache.hadoop.fs.FileSystem.get(conf)
def filesAsString(directory: String, filter: String): Array[String] = {
  
  
  val xsd_list = fs.listFiles(new Path(directory),false)
  var lst = new StringBuilder

  while (xsd_list.hasNext) {
    val x = xsd_list.next.getPath
    val x_str = x.toString.replace(directory,"")
    if(x_str.contains(filter) && x_str.contains("._") == false ) {
      if(lst.length() > 0) {
        lst.append(',')
      }
      lst ++= x_str.toString
    }
  }

  lst.toString().split(",")
}

def validateXmls(xml_directory: String, xsd_directory: String): String = {
  // Get all the xml files in xml_directory
  val target_files = filesAsString(/* conf, */ xml_directory, ".xml")
//  val fs = org.apache.hadoop.fs.FileSystem.get(conf)

  // Get all the xsd files in xsd_directory
  val target_list = filesAsString(/* conf,*/xsd_directory, ".xsd")
  val total_xsds = target_list.length
  var sources = new Array[javax.xml.transform.Source](total_xsds)

  // Populate a source array with Stream Sources
  for (i <- 0 to (target_list.length - 1)) {
    sources.update(i, new javax.xml.transform.stream.StreamSource(new FSDataInputStream(fs.open(new Path(xsd_directory + target_list(i))))))
  }

  // Create a schema factory
  val sf = SchemaFactory.newInstance(javax.xml.XMLConstants.W3C_XML_SCHEMA_NS_URI)
  // Create Schema object
  val schema_obj = sf.newSchema(sources)
  // Create validator
  val validator = schema_obj.newValidator

  // validate locally using sax (expensive but needed currently)
  var filePathStr = ""
  for (i <- 0 to target_files.length - 1) {
    try {
      validator.validate(new javax.xml.transform.stream.StreamSource(new FSDataInputStream(fs.open(new Path(xml_directory + target_files(i))))))
      val sep = if (i != 0 && filePathStr.length > 0 && target_files(i).length > 0) {
        ","
      }
      else {
        ""
      }
      filePathStr = filePathStr + sep + xml_directory + target_files(i).trim
    }
    catch {
      case ex: Exception => {
        target_files.update(i, "")
      }
    }
  }

  filePathStr
}
}
