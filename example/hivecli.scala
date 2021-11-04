package example

import java.io.IOException

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
val conStr = "jdbc:hive2://sandbox-hdp.hortonworks.com:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2";

val insertSql = """
                |INSERT INTO users (username,password,clearance)
                |VALUES (?,?,?)
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
            if (userList.isEmpty) {
            println("What would you like to do:\n 1: Create User \n 2: Sign In \n 3: Update Username and Password \n 4: Scrape Data \n " +
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
                            val password = scala.io.StdIn.readLine("What would you like your password to be?\n Must be between 4-10 Characters\n")
                            if (password.length() >= 4 & password.length() <= 10) {
                                val stmt = con.createStatement();
                                val userQuery = stmt.execute("set hive.enforce.bucketing = true");
                                if (!userQuery) {
                                println("creating account...")
                                val preparedStmt: PreparedStatement = con.prepareStatement(insertSql)

                                preparedStmt.setString (1, userName)
                                preparedStmt.setString (2, password)
                                preparedStmt.setString (3, "user")

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

                    val preparedStatement: PreparedStatement = con.prepareStatement(passQuery)
                    preparedStatement.setString(1, password)
                    preparedStatement.setString(2, userName)
                    val res = preparedStatement.executeQuery
                    if (res.next()) {
                    while (res.next()) {
                        
                        println(s"you are now logged in as a ${res.getString(1)}")
                        if (userList.isEmpty) {
                        userList += (res.getString(1), res.getString(2))   
                        } else {
                            userList.remove(0,1)
                            userList += (res.getString(1), res.getString(2))
                        }
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

                                    val preparedStatement: PreparedStatement = con.prepareStatement(passQuery)
                                    preparedStatement.setString(1, password)
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
                                            preparedStatement.setString(2, password)
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

                                    val preparedStatement: PreparedStatement = con.prepareStatement(passQuery)
                                    preparedStatement.setString(1, password)
                                    preparedStatement.setString(2, userName)
                                    val res = preparedStatement.executeQuery
                                    if (res.next()) {
                                        val newPass = scala.io.StdIn.readLine("What would you like to change your password to?")
                                        if (newPass.length() >= 4 && newPass.length() <= 10) {
                                            val stmt = con.createStatement();
                                            val userQuery = stmt.execute("set hive.enforce.bucketing = true");
                                            if (!userQuery) {
                                            println("updating password...")
                                            val preparedStatement: PreparedStatement = con.prepareStatement(updatePassQuery)
                                            preparedStatement.setString(1, newPass)
                                            preparedStatement.setString(2, password)
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
                      " 3: Joe Biden \n 4: Covid \n 5: Reconciliation \n 6: Donald Trump")
                      val answer = myCheck.checkInt()
                      answer match {
                          case 1 => {
                              //most common topic hive query
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
                                    val myLoop = stmt.executeQuery("select count(*) from test2")
                                    while (myLoop.next()) {
                                        println(myLoop.getString(1))
                                    }
                                }
                            }
                              //how many articles hive query
                          }
                          case 3 => {
                              val biden = "%Biden%"
                              con = DriverManager.getConnection(conStr, "", "");
                              val stmt = con.createStatement();
                              val addJar = stmt.execute("add jar hdfs:///user/maria_dev/.hiveJars/json-udf-1.3.8-jar-with-dependencies.jar")
                              if (!addJar) {
                                println("adding udf jar file...")
                                val addSecondJar = stmt.execute("add jar hdfs:///user/maria_dev/.hiveJars/json-serde-1.3.8-jar-with-dependencies.jar")
                                if (!addSecondJar) {
                                    println("adding serde file...")
                                    for (x <- 0 until 6) {
                                        val myLoop = stmt.executeQuery(s"select title from test2 where per_facet[$x] like '" + biden + "' or title like '" + biden + "'")
                                        while (myLoop.next()) {
                                        println(myLoop.getString(1))
                                    }
                                }
                            }
                        }
                              //joe biden articles
                          }
                          case 4 => {
                              val biden = "%Covid%"
                              con = DriverManager.getConnection(conStr, "", "");
                              val stmt = con.createStatement();
                              val addJar = stmt.execute("add jar hdfs:///user/maria_dev/.hiveJars/json-udf-1.3.8-jar-with-dependencies.jar")
                              if (!addJar) {
                                println("adding udf jar file...")
                                val addSecondJar = stmt.execute("add jar hdfs:///user/maria_dev/.hiveJars/json-serde-1.3.8-jar-with-dependencies.jar")
                                if (!addSecondJar) {
                                    println("adding serde file...")
                                    val myLoop = stmt.executeQuery("select title from test2 where title like '" + biden + "'")
                                    while (myLoop.next()) {
                                        println(myLoop.getString(1))
                                    }
                                }
                            }
                              //covid hive query
                          }
                          case 5 => {
                              val biden = "%Climate%"
                              con = DriverManager.getConnection(conStr, "", "");
                              val stmt = con.createStatement();
                              val addJar = stmt.execute("add jar hdfs:///user/maria_dev/.hiveJars/json-udf-1.3.8-jar-with-dependencies.jar")
                              if (!addJar) {
                                println("adding udf jar file...")
                                val addSecondJar = stmt.execute("add jar hdfs:///user/maria_dev/.hiveJars/json-serde-1.3.8-jar-with-dependencies.jar")
                                if (!addSecondJar) {
                                    println("adding serde file...")
                                    val myLoop = stmt.executeQuery("select title from test2 where title like '" + biden + "'")
                                    while (myLoop.next()) {
                                        println(myLoop.getString(1))
                                    }
                                }
                            }
                              //reconciliation hive query
                          }
                          case 6 => {
                              val biden = "%Trump%"
                              con = DriverManager.getConnection(conStr, "", "");
                              val stmt = con.createStatement();
                              val addJar = stmt.execute("add jar hdfs:///user/maria_dev/.hiveJars/json-udf-1.3.8-jar-with-dependencies.jar")
                              if (!addJar) {
                                println("adding udf jar file...")
                                val addSecondJar = stmt.execute("add jar hdfs:///user/maria_dev/.hiveJars/json-serde-1.3.8-jar-with-dependencies.jar")
                                if (!addSecondJar) {
                                    println("adding serde file...")
                                    for (x <- 0 until 6) {
                                    val myLoop = stmt.executeQuery(s"select title from test2 where per_facet[$x] like '" + biden + "' or title like '" + biden + "'")
                                    while (myLoop.next()) {
                                        println(myLoop.getString(1))
                                       }
                                    }
                                }
                            }
                              //donald trump hive query
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
                    userList.remove(0)
                    println("logging out...")
                }
                case 8 => {
                    println(("exiting..."))
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
  } else {
      println(s"What would you like to do ${userList(1)}:\n 1: Create User \n 2: Sign In \n 3: Update Username and Password \n 4: Scrape Data \n " +
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
                            val password = scala.io.StdIn.readLine("What would you like your password to be?\n Must be between 4-10 Characters\n")
                            if (password.length() >= 4 & password.length() <= 10) {
                                val stmt = con.createStatement();
                                val userQuery = stmt.execute("set hive.enforce.bucketing = true");
                                if (!userQuery) {
                                println("creating account...")
                                val preparedStmt: PreparedStatement = con.prepareStatement(insertSql)

                                preparedStmt.setString (1, userName)
                                preparedStmt.setString (2, password)
                                preparedStmt.setString (3, "user")

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

                    val preparedStatement: PreparedStatement = con.prepareStatement(passQuery)
                    preparedStatement.setString(1, password)
                    preparedStatement.setString(2, userName)
                    val res = preparedStatement.executeQuery
                    if (res.next()) {
                        
                        println(s"you are now logged in as a ${res.getString(1)}")
                        if (userList.isEmpty) {
                        userList += res.getString(1)   
                        } else {
                            userList.remove(0)
                            userList += res.getString(1)
                            println(userList(0))
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

                                    val preparedStatement: PreparedStatement = con.prepareStatement(passQuery)
                                    preparedStatement.setString(1, password)
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
                                            preparedStatement.setString(2, password)
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

                                    val preparedStatement: PreparedStatement = con.prepareStatement(passQuery)
                                    preparedStatement.setString(1, password)
                                    preparedStatement.setString(2, userName)
                                    val res = preparedStatement.executeQuery
                                    if (res.next()) {
                                        val newPass = scala.io.StdIn.readLine("What would you like to change your password to?")
                                        if (newPass.length() >= 4 && newPass.length() <= 10) {
                                            val stmt = con.createStatement();
                                            val userQuery = stmt.execute("set hive.enforce.bucketing = true");
                                            if (!userQuery) {
                                            println("updating password...")
                                            val preparedStatement: PreparedStatement = con.prepareStatement(updatePassQuery)
                                            preparedStatement.setString(1, newPass)
                                            preparedStatement.setString(2, password)
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
                      " 3: Joe Biden \n 4: Covid \n 5: Reconciliation \n 6: Donald Trump")
                      val answer = myCheck.checkInt()
                      answer match {
                          case 1 => {
                              //most common topic hive query
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
                                    val myLoop = stmt.executeQuery("select count(*) from test2")
                                    while (myLoop.next()) {
                                        println(myLoop.getString(1))
                                    }
                                }
                            }
                              //how many articles hive query
                          }
                          case 3 => {
                              val biden = "%Biden%"
                              con = DriverManager.getConnection(conStr, "", "");
                              val stmt = con.createStatement();
                              val addJar = stmt.execute("add jar hdfs:///user/maria_dev/.hiveJars/json-udf-1.3.8-jar-with-dependencies.jar")
                              if (!addJar) {
                                println("adding udf jar file...")
                                val addSecondJar = stmt.execute("add jar hdfs:///user/maria_dev/.hiveJars/json-serde-1.3.8-jar-with-dependencies.jar")
                                if (!addSecondJar) {
                                    println("adding serde file...")
                                    for (x <- 0 until 6) {
                                        val myLoop = stmt.executeQuery(s"select title from test2 where per_facet[$x] like '" + biden + "' or title like '" + biden + "'")
                                        while (myLoop.next()) {
                                        println(myLoop.getString(1))
                                    }
                                }
                            }
                        }
                              //joe biden articles
                          }
                          case 4 => {
                              val biden = "%Covid%"
                              con = DriverManager.getConnection(conStr, "", "");
                              val stmt = con.createStatement();
                              val addJar = stmt.execute("add jar hdfs:///user/maria_dev/.hiveJars/json-udf-1.3.8-jar-with-dependencies.jar")
                              if (!addJar) {
                                println("adding udf jar file...")
                                val addSecondJar = stmt.execute("add jar hdfs:///user/maria_dev/.hiveJars/json-serde-1.3.8-jar-with-dependencies.jar")
                                if (!addSecondJar) {
                                    println("adding serde file...")
                                    val myLoop = stmt.executeQuery("select title from test2 where title like '" + biden + "'")
                                    while (myLoop.next()) {
                                        println(myLoop.getString(1))
                                    }
                                }
                            }
                              //covid hive query
                          }
                          case 5 => {
                              val biden = "%Climate%"
                              con = DriverManager.getConnection(conStr, "", "");
                              val stmt = con.createStatement();
                              val addJar = stmt.execute("add jar hdfs:///user/maria_dev/.hiveJars/json-udf-1.3.8-jar-with-dependencies.jar")
                              if (!addJar) {
                                println("adding udf jar file...")
                                val addSecondJar = stmt.execute("add jar hdfs:///user/maria_dev/.hiveJars/json-serde-1.3.8-jar-with-dependencies.jar")
                                if (!addSecondJar) {
                                    println("adding serde file...")
                                    val myLoop = stmt.executeQuery("select title from test2 where title like '" + biden + "'")
                                    while (myLoop.next()) {
                                        println(myLoop.getString(1))
                                    }
                                }
                            }
                              //reconciliation hive query
                          }
                          case 6 => {
                              val biden = "%Trump%"
                              con = DriverManager.getConnection(conStr, "", "");
                              val stmt = con.createStatement();
                              val addJar = stmt.execute("add jar hdfs:///user/maria_dev/.hiveJars/json-udf-1.3.8-jar-with-dependencies.jar")
                              if (!addJar) {
                                println("adding udf jar file...")
                                val addSecondJar = stmt.execute("add jar hdfs:///user/maria_dev/.hiveJars/json-serde-1.3.8-jar-with-dependencies.jar")
                                if (!addSecondJar) {
                                    println("adding serde file...")
                                    for (x <- 0 until 6) {
                                    val myLoop = stmt.executeQuery(s"select title from test2 where per_facet[$x] like '" + biden + "' or title like '" + biden + "'")
                                    while (myLoop.next()) {
                                        println(myLoop.getString(1))
                                       }
                                    }
                                }
                            }
                              //donald trump hive query
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
                    userList.remove(0)
                    println("logging out...")
                }
                case 8 => {
                    println(("exiting..."))
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
}
