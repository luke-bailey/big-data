package example


import scala.io._
import scala.util.Try
import java.io._
import scala.util._

/*object Test2 {
    def main(args: Array[String]) = {
        println("hello")
    }
}*/

class InputCheck {

def checkString():String = {
    val userAnswer = scala.io.StdIn.readLine()
    userAnswer
}

def checkInt():Int = {
                var userAnswer = 0
                var isInt = false
                try {
                userAnswer = scala.io.StdIn.readInt()
                isInt = Try(userAnswer.toInt).isSuccess
            } catch {
                case _: NumberFormatException => {
                    //println("This is not a valid input")
                    //100
                }
            }
            if (isInt) {
                    userAnswer
                } else {
                    100
                }
        }
        def checkDouble():Double = {
                var userResponse = 0.00
                var isDouble = false
                try {
                userResponse = scala.io.StdIn.readDouble()
                isDouble = Try(userResponse.toDouble).isSuccess
            } catch {
                case _: NumberFormatException => {
                    println("This is not a valid input")
                    //100.00
                }
            }
            if (isDouble) {
                    userResponse
            } else {
                0
            }
        }
    }