package example

import java.io.IOException
import java.security.MessageDigest
import scala.util.Try

import java.sql.SQLException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

import org.apache.hadoop.hive.cli.CliSessionState
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.ql.Driver
import org.apache.hadoop.hive.ql.session.SessionState
import java.sql.Driver
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.ArrayBuffer


object HiveCLI {

var con: java.sql.Connection = null;
var driverName = "org.apache.hive.jdbc.HiveDriver"
val conStr = "jdbc:hive2://sandbox-hdp.hortonworks.com:10000/default";

val insertSql = """
                |INSERT INTO users (id,username,password,clearance)
                |VALUES (?,?,?,?)
                """.stripMargin
val userQuery = """
                SELECT username FROM users WHERE username = ?
                """.stripMargin
val passQuery = """
                SELECT clearance, username FROM users WHERE password = ? AND username = ?
                """.stripMargin
val updateQuery = """
                UPDATE users SET username = ? WHERE password = ? AND username = ?
                """.stripMargin
val updatePassQuery = """
                UPDATE users SET password = ? WHERE password = ? AND username = ?
                """.stripMargin


    def main(args: Array[String]): Unit = {
        val myCheck = new InputCheck()
        var counter = 0
        val path = "hdfs://sandbox-hdp.hortonworks.com:8020/user/maria_dev/"
        val userList = new ArrayBuffer[String]()
        while (counter == 0) {
            println("What would you like to do :\n 1: Create User \n 2: Sign In \n 3: Update Username and Password \n 4: Scrape Data \n " +
                    "5: Upload Data \n 6: Perform Queries \n 7: Log Out \n 8: Exit")
            
        //KeyID pp0RqO0ygRq2zkctpkUKh8VGUqTw0a18
        val userResponse = myCheck.checkInt()
        try {    
        def response (answer: Int): Unit = {
            answer match {
                case 1 => {
                    val userName = scala.io.StdIn.readLine("What would you like your username to be?\nMust be between 4-10 Characters\n")
                    if (userName.length() >= 4 & userName.length() <= 10) {
                        Class.forName(driverName);

                        con = DriverManager.getConnection(conStr, "", "");

                        val preparedStatement: PreparedStatement = con.prepareStatement(userQuery)
                        preparedStatement.setString(1, userName)
                        val res = preparedStatement.executeQuery
                        if (res.next()) {
                            println("this username is already taken, please choose another")
                        
                        } else {
                            val password = scala.io.StdIn.readLine("What would you like your password to be?\nMust be between 4-10 Characters\n")
                            if (password.length() >= 4 & password.length() <= 10) {
                                
                                val stmt = con.createStatement();
                                val userQuery = stmt.execute("set hive.enforce.bucketing = true");
                                if (!userQuery) {
                                println("creating account...")
                                val digest = MessageDigest.getInstance("MD5")
                                val newPass = digest.digest(password.getBytes).map("%02x".format(_)).mkString
                                val count = stmt.executeQuery("select count(*) from users")
                                var userCount = 0
                                while (count.next()){
                                userCount = count.getInt(1) + 1
                                }
                                val preparedStmt: PreparedStatement = con.prepareStatement(insertSql)
                                preparedStmt.setInt(1, userCount)
                                preparedStmt.setString (2, userName)
                                preparedStmt.setString (3, newPass)
                                preparedStmt.setString (4, "user")

                                val insert = preparedStmt.execute
                                if (!insert) {
                                    println("info successfully inserted")
                                } else {
                                    println("there was an issue with the transaction")
                                }
                                //do hive insert
                                }
                             } else {
                                    println("password must be between 4 and 10 characters")
                                }
                             } 
                           } else {
                                println("username must be between 4 and 10 characters")
                            }
                    }
                case 2 => {
                    val userName = scala.io.StdIn.readLine("Username: ")
                    val password = scala.io.StdIn.readLine("Password: ")
                    /*val standardIn = System.console()
                    val otherPass = standardIn.readPassword()
                    println(otherPass)*/
                    Class.forName(driverName);

                    con = DriverManager.getConnection(conStr, "", "");
                    val stmt = con.createStatement();
                    val digest = MessageDigest.getInstance("MD5")
                    val newPass = digest.digest(password.getBytes).map("%02x".format(_)).mkString
                    val preparedStatement: PreparedStatement = con.prepareStatement(passQuery)
                    preparedStatement.setString(1, newPass)
                    preparedStatement.setString(2, userName)
                    val res = preparedStatement.executeQuery
                        if (res.next()) {
                        println(s"you are now logged in as a ${res.getString(1)}")
                        if (userList.isEmpty) {
                        userList += res.getString(1)
                        } else {
                            userList.remove(0)
                            userList += res.getString(1)
                        }
                    } else {
                        println("username or password is incorrect")
                    }
                    } 
                case 3 => {
                    println("What would you like to update?\n 1: Username \n 2: Password")
                    val answer = myCheck.checkInt()
                    answer match {
                        case 1 => {
                            val userName = scala.io.StdIn.readLine("What is your username?")
                            val password = scala.io.StdIn.readLine("Password: ")
                            Class.forName(driverName);

                                con = DriverManager.getConnection(conStr, "", "");
                                    val digest = MessageDigest.getInstance("MD5")
                                    val newPass = digest.digest(password.getBytes).map("%02x".format(_)).mkString
                                    val preparedStatement: PreparedStatement = con.prepareStatement(passQuery)
                                    preparedStatement.setString(1, newPass)
                                    preparedStatement.setString(2, userName)
                                    val res = preparedStatement.executeQuery
                                    if (res.next()) {
                                        val newUserName = scala.io.StdIn.readLine("What would you like to change your username to?")
                                        if (newUserName.length() >= 4 && newUserName.length() <= 10) {
                                            Class.forName(driverName);

                                            con = DriverManager.getConnection(conStr, "", "");
                                            val stmt = con.createStatement();
                                            val userQuery = stmt.execute("set hive.enforce.bucketing = true");
                                            if (!userQuery) {
                                            println("updating username...")
                                            val preparedStatement: PreparedStatement = con.prepareStatement(updateQuery)
                                            preparedStatement.setString(1, newUserName)
                                            preparedStatement.setString(2, newPass)
                                            preparedStatement.setString(3, userName)
                                            val res = preparedStatement.execute
                                            if (!res) {
                                                println("you have updated your username")
                                            } else {
                                                println("something went wrong")
                                            }
                                        } else {
                                            println("username must between 4-10 characters")
                                        }
                                    }
                                    } else {
                                        println("incorrect username or password")
                                    }
                                }
                        case 2 => {
                            val userName = scala.io.StdIn.readLine("What is your Username? ")
                            val password = scala.io.StdIn.readLine("Password: ")
                            Class.forName(driverName);

                                con = DriverManager.getConnection(conStr, "", "");
                                    val digest = MessageDigest.getInstance("MD5")
                                    val newPass = digest.digest(password.getBytes).map("%02x".format(_)).mkString
                                    val preparedStatement: PreparedStatement = con.prepareStatement(passQuery)
                                    preparedStatement.setString(1, newPass)
                                    preparedStatement.setString(2, userName)
                                    val res = preparedStatement.executeQuery
                                    if (res.next()) {
                                        val newPass2 = scala.io.StdIn.readLine("What would you like to change your password to?")
                                        if (newPass2.length() >= 4 && newPass2.length() <= 10) {
                                            val stmt = con.createStatement();
                                            val userQuery = stmt.execute("set hive.enforce.bucketing = true");
                                            if (!userQuery) {
                                            println("updating password...")
                                            val digest = MessageDigest.getInstance("MD5")
                                            val newPass3 = digest.digest(newPass2.getBytes).map("%02x".format(_)).mkString
                                            val preparedStatement: PreparedStatement = con.prepareStatement(updatePassQuery)
                                            preparedStatement.setString(1, newPass3)
                                            preparedStatement.setString(2, newPass)
                                            preparedStatement.setString(3, userName)
                                            val res = preparedStatement.execute
                                            if (!res) {
                                                println("you have updated your password")
                                            } else {
                                                println("something went wrong")
                                            }
                                        }
                                     } else {
                                            println("username must between 4-10 characters")
                                        }
                                    } else {
                                        println("incorrect username or password")
                                    }
                            }
                            case _ => {
                                println("this is not a valid response")
                            }
                        }
                    }
                case 4 => {
                    val apiData = new APIConnect()
                    val hdfs = new HdfsDemo()
                    hdfs.createFile(apiData.content)
                }
                case 5 => {
                    Class.forName(driverName);

                    con = DriverManager.getConnection(conStr, "", "");
                    val stmt = con.createStatement();
                    val addJar = stmt.execute("add jar hdfs:///user/maria_dev/.hiveJars/json-udf-1.3.8-jar-with-dependencies.jar")
                    if (!addJar) {
                        println("adding udf jar file...")
                        val addSecondJar = stmt.execute("add jar hdfs:///user/maria_dev/.hiveJars/json-serde-1.3.8-jar-with-dependencies.jar")
                        if (!addSecondJar) {
                            println("adding serde jar file...")
                            val filepath = "hdfs:///user/maria_dev/NYTjson2.txt";
                            val tableName = "politics"
                            val sqlQuery = "load data inpath '" + filepath + "' overwrite into table " + tableName;
                            System.out.println("Running: " + sqlQuery);
                            val tableLoad = stmt.execute(sqlQuery);
                            if (!tableLoad) {
                                println("successfully uploaded")
                            } else {
                                println("upload failed")
                            }
                                } else {
                                    println("something went wrong")
                                }
                            } else {
                                println("something went wrong")
                            }
                    
                    //upload data to Hive
                }
                case 6 => {
                    if (!userList.isEmpty) {
                        if (userList(0) == "admin") {
                    println("What query would you like to perform? \n 1: Most Common Topic \n 2: How Many Articles \n" +
                      " 3: Joe Biden \n 4: Covid \n 5: Climate \n 6: Donald Trump")
                      val answer = myCheck.checkInt()
                      answer match {
                          case 1 => {
                              con = DriverManager.getConnection(conStr, "", "");
                              val stmt = con.createStatement();
                              val addJar = stmt.execute("add jar hdfs:///user/maria_dev/.hiveJars/json-udf-1.3.8-jar-with-dependencies.jar")
                              if (!addJar) {
                                println("adding udf jar file...")
                                val addSecondJar = stmt.execute("add jar hdfs:///user/maria_dev/.hiveJars/json-serde-1.3.8-jar-with-dependencies.jar")
                                if (!addSecondJar) {
                                    println("adding serde file...")
                                    val myLoop = stmt.executeQuery("select des, count(1) as cnt from politics lateral view explode(des_facet) polTable as des group by des order by cnt desc")
                                    while (myLoop.next()) {
                                        println(myLoop.getString(1), myLoop.getString(2))
                                    }
                          }
                        }
                    }
                      
                          case 2 => {
                              con = DriverManager.getConnection(conStr, "", "");
                              val stmt = con.createStatement();
                              val addJar = stmt.execute("add jar hdfs:///user/maria_dev/.hiveJars/json-udf-1.3.8-jar-with-dependencies.jar")
                              if (!addJar) {
                                println("adding udf jar file...")
                                val addSecondJar = stmt.execute("add jar hdfs:///user/maria_dev/.hiveJars/json-serde-1.3.8-jar-with-dependencies.jar")
                                if (!addSecondJar) {
                                    println("adding serde file...")
                                    println("counting articles...")
                                    val myLoop = stmt.executeQuery("select count(*) from politics")
                                    while (myLoop.next()) {
                                        println(myLoop.getString(1))
                                    }
                                }
                            }
                              //how many articles hive query
                          }
                          case 3 => {
                              println(" 1: Check how many articles are related to Biden \n 2: The article titles and their abstracts")
                              val biden = "%Biden%"
                              val lowercase = "%biden%"
                              val answer = myCheck.checkInt()
                              answer match { 
                              case 1 => { 
                              con = DriverManager.getConnection(conStr, "", "");
                              val stmt = con.createStatement();
                              val addJar = stmt.execute("add jar hdfs:///user/maria_dev/.hiveJars/json-udf-1.3.8-jar-with-dependencies.jar")
                              if (!addJar) {
                                println("adding udf jar file...")
                                val addSecondJar = stmt.execute("add jar hdfs:///user/maria_dev/.hiveJars/json-serde-1.3.8-jar-with-dependencies.jar")
                                if (!addSecondJar) {
                                    println("adding serde file...")
                                    println("counting articles...")
                                        val myLoop = stmt.executeQuery(s"select count(*) from politics where title like '" + biden + "' or abstract like '" + biden + "' or byline like '" + biden + "' or des_facet[0] like '" + biden + "' or url like '" + lowercase + "'")
                                        while (myLoop.next()) {
                                        println(myLoop.getString(1))
                                    }
                            }
                        }
                    }
                            case 2 => {
                                con = DriverManager.getConnection(conStr, "", "");
                              val stmt = con.createStatement();
                              val addJar = stmt.execute("add jar hdfs:///user/maria_dev/.hiveJars/json-udf-1.3.8-jar-with-dependencies.jar")
                              if (!addJar) {
                                println("adding udf jar file...")
                                val addSecondJar = stmt.execute("add jar hdfs:///user/maria_dev/.hiveJars/json-serde-1.3.8-jar-with-dependencies.jar")
                                if (!addSecondJar) {
                                    println("adding serde file...")
                                        val myLoop = stmt.executeQuery("select title, abstract from politics where title like '" + biden + "' or abstract like '" + biden + "' or byline like '" + biden + "' or des_facet[0] like '" + biden + "' or url like '" + lowercase + "'")
                                
                                        while (myLoop.next()) {
                                        println(s"${myLoop.getString(1)}, ${myLoop.getString(2)}")
    
                                    }
                            }
                        }
                            }
                            case _ => {
                              println("That is not a valid response")
                }
            }
                
                              //joe biden articles
                          }
                          case 4 => {
                              println(" 1: Check how many articles are related to Covid \n 2: The article titles and their abstracts")
                              val answer = myCheck.checkInt()
                              answer match {
                                  case 1 => {
                              val biden = "%Covid%"
                              val lowercase = "%covid%"
                              con = DriverManager.getConnection(conStr, "", "");
                              val stmt = con.createStatement();
                              val addJar = stmt.execute("add jar hdfs:///user/maria_dev/.hiveJars/json-udf-1.3.8-jar-with-dependencies.jar")
                              if (!addJar) {
                                println("adding udf jar file...")
                                val addSecondJar = stmt.execute("add jar hdfs:///user/maria_dev/.hiveJars/json-serde-1.3.8-jar-with-dependencies.jar")
                                if (!addSecondJar) {
                                    println("adding serde file...")
                                    println("counting articles...")
                                    val myLoop = stmt.executeQuery(s"select count(*) from politics where title like '" + biden + "' or abstract like '" + biden + "' or byline like '" + biden + "' or des_facet[0] like '" + biden + "' or url like '" + lowercase + "'")
                                    while (myLoop.next()) {
                                        println(myLoop.getString(1))
                                    }
                                }
                            }
                        }
                            case 2 => {
                            val biden = "%Covid%"
                              val lowercase = "%covid%"
                              con = DriverManager.getConnection(conStr, "", "");
                              val stmt = con.createStatement();
                              val addJar = stmt.execute("add jar hdfs:///user/maria_dev/.hiveJars/json-udf-1.3.8-jar-with-dependencies.jar")
                              if (!addJar) {
                                println("adding udf jar file...")
                                val addSecondJar = stmt.execute("add jar hdfs:///user/maria_dev/.hiveJars/json-serde-1.3.8-jar-with-dependencies.jar")
                                if (!addSecondJar) {
                                    println("adding serde file...")
                                    val myLoop = stmt.executeQuery(s"select title, abstract from politics where title like '" + biden + "' or abstract like '" + biden + "' or byline like '" + biden + "' or des_facet[0] like '" + biden + "' or url like '" + lowercase + "'")
                                    while (myLoop.next()) {
                                        println(myLoop.getString(1), myLoop.getString(2))
                                    }
                                }
                            }
                              //covid hive query
                          }
                          case _ => {
                              println("That is not a valid response")
                          }
                        }
                    }
                          case 5 => {
                              println(" 1: Climate article count \n 2: Titles and abstracts")
                              val answer = myCheck.checkInt()
                              answer match {
                                  case 1 => { 
                              val biden = "%Climate%"
                              val lowercase = "%climate%"
                              con = DriverManager.getConnection(conStr, "", "");
                              val stmt = con.createStatement();
                              val addJar = stmt.execute("add jar hdfs:///user/maria_dev/.hiveJars/json-udf-1.3.8-jar-with-dependencies.jar")
                              if (!addJar) {
                                println("adding udf jar file...")
                                val addSecondJar = stmt.execute("add jar hdfs:///user/maria_dev/.hiveJars/json-serde-1.3.8-jar-with-dependencies.jar")
                                if (!addSecondJar) {
                                    println("adding serde file...")
                                    println("counting articles...")
                                    val myLoop = stmt.executeQuery(s"select count(*) from politics where title like '" + biden + "' or abstract like '" + lowercase + "' or byline like '" + lowercase + "' or des_facet[0] like '" + biden + "' or url like '" + lowercase + "' or section = '" + lowercase + "'")
                                    while (myLoop.next()) {
                                        println(myLoop.getString(1))
                                    }
                            }
                        } 
                    }
                        case 2 => {
                            val biden = "%Climate%"
                              val lowercase = "%climate%"
                              con = DriverManager.getConnection(conStr, "", "");
                              val stmt = con.createStatement();
                              val addJar = stmt.execute("add jar hdfs:///user/maria_dev/.hiveJars/json-udf-1.3.8-jar-with-dependencies.jar")
                              if (!addJar) {
                                println("adding udf jar file...")
                                val addSecondJar = stmt.execute("add jar hdfs:///user/maria_dev/.hiveJars/json-serde-1.3.8-jar-with-dependencies.jar")
                                if (!addSecondJar) {
                                    println("adding serde file...")
                                    val myLoop = stmt.executeQuery(s"select title, abstract from politics where title like '" + biden + "' or abstract like '" + lowercase + "' or byline like '" + lowercase + "' or des_facet[0] like '" + biden + "' or url like '" + lowercase + "' or section = '" + lowercase + "'")
                                    while (myLoop.next()) {
                                        println(s"${myLoop.getString(1)}, ${myLoop.getString(2)}")
                                    }
                            }
                        }
                        }
                        case _ => {
                            println("that is not a valid response")
                        }
                    }
                              //reconciliation hive query
                          }
                          case 6 => {
                              println(" 1: Trump article count \n 2: Titles and abstracts")
                              val answer = myCheck.checkInt()
                              answer match {
                                case 1 => {
                              val biden = "%Trump%"
                              val lowercase = "%trump%"
                              con = DriverManager.getConnection(conStr, "", "");
                              val stmt = con.createStatement();
                              val addJar = stmt.execute("add jar hdfs:///user/maria_dev/.hiveJars/json-udf-1.3.8-jar-with-dependencies.jar")
                              if (!addJar) {
                                println("adding udf jar file...")
                                val addSecondJar = stmt.execute("add jar hdfs:///user/maria_dev/.hiveJars/json-serde-1.3.8-jar-with-dependencies.jar")
                                if (!addSecondJar) {
                                    println("adding serde file...")
                                    println("counting articles...")
                                    val myLoop = stmt.executeQuery(s"select count(*) from politics where title like '" + biden + "' or abstract like '" + biden + "' or byline like '" + biden + "' or des_facet[0] like '" + biden + "' or url like '" + lowercase + "' or section = '" + lowercase + "'")
                                    while (myLoop.next()) {
                                        println(myLoop.getString(1))
                                       }
                                }
                                }
                            }
                            case 2 => {
                                val biden = "%Trump%"
                              val lowercase = "%trump%"
                              con = DriverManager.getConnection(conStr, "", "");
                              val stmt = con.createStatement();
                              val addJar = stmt.execute("add jar hdfs:///user/maria_dev/.hiveJars/json-udf-1.3.8-jar-with-dependencies.jar")
                              if (!addJar) {
                                println("adding udf jar file...")
                                val addSecondJar = stmt.execute("add jar hdfs:///user/maria_dev/.hiveJars/json-serde-1.3.8-jar-with-dependencies.jar")
                                if (!addSecondJar) {
                                    println("adding serde file...")
                                    val myLoop = stmt.executeQuery(s"select title, abstract from politics where title like '" + biden + "' or abstract like '" + biden + "' or byline like '" + biden + "' or des_facet[0] like '" + biden + "' or url like '" + lowercase + "' or section = '" + lowercase + "'")
                                    while (myLoop.next()) {
                                        println(myLoop.getString(1), myLoop.getString(2))
                                       }
                                }
                                }
                            }
                        }
                          }
                          case _ => {
                              println("That is not a valid response")
                          }
                      }
                    } else {
                        println("users can not perform queries")
                    }
                    } else {
                        println("only an admin can run queries")
                    }
                    //Hive queries
                }
                case 7 => {
                    if (!userList.isEmpty) {
                    userList.remove(0)
                    println("logging out...")
                    } else {
                        println("you are not logged in")
                    }
                }
                case 8 => {
                    println("exiting...")
                    counter += 1
                }
                case _ => {
                    println("that is not a valid input")
                }
            }
        }
        response(userResponse)
    }  catch {
      case ex: Throwable => {
        ex.printStackTrace();
        throw new Exception(s"${ex.getMessage}")
      }
    } finally {
      try {
        if (con != null)
          con.close();
      } catch {
        case ex: Throwable => {
          ex.printStackTrace();
          throw new Exception(s"${ex.getMessage}")
        }
      }
    }
  
    }
}
}
