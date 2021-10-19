package example

import scala.io._
import upickle.default._
import scala.util.Try
import org.mongodb.scala._

object MyTest {
    def main(args: Array[String]) = {
        println("hello")
    }
}

class readFiles (val fileName: String) {
    val fileType = fileName
    var myJsonFile: ujson.Value = _
    var myCsvFile = Array[Array[String]]()
    def findType (file: String = fileType) = {
        if (file.contains(".json")) {
            println("found a json file!")
            val jsonFile = os.read(os.pwd/s"$file")
            val data: ujson.Value = ujson.read(jsonFile)
            val myArray = data.arr
            //val jsonArray = data.arr
            //println(data(0)("first_name").str)
            //myJsonArray = jsonArray
            myJsonFile = myArray
    } 
    

}
}