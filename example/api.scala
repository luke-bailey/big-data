package example

import java.io.InputStream
import java.io.IOException
import java.net.{URL, HttpURLConnection}
import scala.util.Try
//import upickle.default._
import java.io._
import scala.io._

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import java.io.PrintWriter;
import scala.io._

/*object APITest {
    def main(args: Array[String]): Unit = {
        val myAPI = new APIConnect()
        println(myAPI.content)      
        println(myAPI.myApiData("data")("status"))
        val myHDFS = new HdfsDemo()
        myHDFS.createFile(myAPI.content)
    }*/
class APIConnect {
        var content = ""
        //var myApiData: ujson.Value = _
        @throws(classOf[java.io.IOException])
        @throws(classOf[java.net.SocketTimeoutException])
        def get(url: String,
        connectTimeout: Int = 5000,
        readTimeout: Int = 5000,
        requestMethod: String = "GET") =
            {
    
    val connection = (new URL(url)).openConnection.asInstanceOf[HttpURLConnection]
    connection.setConnectTimeout(connectTimeout)
    connection.setReadTimeout(readTimeout)
    connection.setRequestMethod(requestMethod)
    val inputStream = connection.getInputStream
    val content = scala.io.Source.fromInputStream(inputStream).mkString
    if (inputStream != null) inputStream.close
    content
}
try {
    val content2 = get("https://api.nytimes.com/svc/topstories/v2/science.json?api-key=pp0RqO0ygRq2zkctpkUKh8VGUqTw0a18")
    //val data: ujson.Value = ujson.read(content)
    //val myArray = data.arr
    content = content2
    //myApiData = myArray
} catch {
    case ioe: java.io.IOException =>  // handle this
    case ste: java.net.SocketTimeoutException => // handle this
}
    }

    class HdfsDemo {
  
  val path = "hdfs://sandbox-hdp.hortonworks.com:8020/user/maria_dev/"


  def createFile(json: String): Unit = {
    val hdfsFile = new APIConnect()
    val filename = path + "ApiData.txt"
    println(s"Creating file $filename ...")
    
    val conf = new Configuration()
    val fs = FileSystem.get(conf)
    
    // Check if file exists. If yes, delete it.
    println("Checking if it already exists...")
    val filepath = new Path( filename)
    val isExisting = fs.exists(filepath)
    if(isExisting) {
      println("Yes it does exist. Deleting it...")
      fs.delete(filepath, false)
    }

    val output = fs.create(new Path(filename))
    
    val writer = new PrintWriter(output)
    writer.write(json)
    writer.close()
    
    println(s"Done creating file $filename ...")
  }
}