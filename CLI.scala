package example

import scala.io._
import upickle.default._
import scala.util.Try
import org.mongodb.scala._
import org.mongodb.scala.model.Filters._
import com.mongodb.async.client.Observable
import org.mongodb.scala.Completed
import org.mongodb.scala.model.Sorts._
import org.mongodb.scala.model.Projections._
import org.mongodb.scala.model.Aggregates._
import org.mongodb.scala.model.Updates._
import org.mongodb.scala.model.Accumulators._
import com.mongodb.client.result.UpdateResult
import org.mongodb.scala.bson.BsonDateTime
import org.mongodb.scala.bson.DefaultBsonTransformers


object ComLineInt {

    def main(args: Array[String]) {
        val myCheck = new InputCheck()
        var counter = 0
        val myConn = new MongoConn()
        val client: MongoClient = MongoClient()
        val database: MongoDatabase = client.getDatabase("test")
        val collection: MongoCollection[Document] = database.getCollection("meteors")

        while (counter == 0) {
            println("What would you like to do:\n 1: Input info \n 2: Sort Data \n 3: Compute Data \n 4: Update Documents \n 5: Other Queries \n 6: Drop Collection \n 7: Exit")
            
        
        val userResponse = myCheck.checkInt()


            def response (answer: Int) = {
                answer match {
                    case 1 => {
                        val newReader = new readFiles("meteors1.json")
                        newReader.findType()         
                        val document = (0 to 499) map { i: Int => Document(
                            "_id" -> i,
                            "name" -> newReader.myJsonFile(i)("name").str,
                            "id" -> newReader.myJsonFile(i)("id").str,
                            "metclass" -> newReader.myJsonFile(i)("recclass").str,
                            "nametype" -> newReader.myJsonFile(i)("nametype").str,
                            "mass" -> newReader.myJsonFile(i)("mass").num,
                            "year"-> newReader.myJsonFile(i)("year").str,
                            "metlat" -> newReader.myJsonFile(i)("reclat").num,
                            "metlong" -> newReader.myJsonFile(i)("reclong").num,
                            "geolocation" -> Seq(newReader.myJsonFile(i)("geolocation")("coordinates")(0).num,newReader.myJsonFile(i)("geolocation")("coordinates")(1).num))
                        }
                        val observable: Observable[Completed] = collection.insertMany(document)

                        //Only when an Observable is subscribed to and data requested will the operation happen
                        //Explicitly subscribe:
                        observable.subscribe(new Observer[Completed] {
                        override def onNext(result: Completed): Unit = println("Inserted")

                        override def onError(e: Throwable): Unit = println("Failed")

                        override def onComplete(): Unit = println("Completed")
                    })
                        
                    }

                    case 2 => {
                        println("How would you like to sort the data? \n 1: Name \n 2: ID \n 3: Class \n 4: Mass \n 5: Year \n 6: Coordinates")
                        val answer = myCheck.checkInt()
                        answer match {
                            case 1 => {
                            val observable: Observable[Document] = collection.find(exists("name")).sort(ascending("name")).projection(fields(include("id", "name", "metclass", "mass", "year", "geolocation"), excludeId()))
//Only when an Observable is subscribed to and data requested will the operation happen
//Explicitly subscribe:
                            observable.subscribe(new Observer[Document] {
                            override def onNext(result: Document): Unit = println(result.toJson())

                            override def onError(e: Throwable): Unit = println("Failed")

                            override def onComplete(): Unit = println("Completed")
                            })
                        }
                        case 2 => {
                            val observable: Observable[Document] = collection.find(exists("id")).sort(ascending("id")).projection(fields(include("id", "name", "metclass", "mass", "year", "geolocation"), excludeId()))
//Only when an Observable is subscribed to and data requested will the operation happen
//Explicitly subscribe:
                            observable.subscribe(new Observer[Document] {
                            override def onNext(result: Document): Unit = println(result.toJson())

                            override def onError(e: Throwable): Unit = println("Failed")

                            override def onComplete(): Unit = println("Completed")
                            })
                        }
                        case 3 => {
                            val observable: Observable[Document] = collection.find(exists("class")).sort(ascending("class")).projection(fields(include("id", "name", "metclass", "mass", "year", "geolocation"), excludeId()))
//Only when an Observable is subscribed to and data requested will the operation happen
//Explicitly subscribe:
                            observable.subscribe(new Observer[Document] {
                            override def onNext(result: Document): Unit = println(result.toJson())

                            override def onError(e: Throwable): Unit = println("Failed")

                            override def onComplete(): Unit = println("Completed")
                            })
                        }
                        case 4 => {
                            val observable: Observable[Document] = collection.find(exists("mass")).sort(ascending("mass")).projection(fields(include("id", "name", "metclass", "mass", "year", "geolocation"), excludeId()))
//Only when an Observable is subscribed to and data requested will the operation happen
//Explicitly subscribe:
                            observable.subscribe(new Observer[Document] {
                            override def onNext(result: Document): Unit = println(result.toJson())

                            override def onError(e: Throwable): Unit = println("Failed")

                            override def onComplete(): Unit = println("Completed")
                            })
                        }
                        case 5 => {
                            val observable: Observable[Document] = collection.find(exists("year")).sort(ascending("year")).projection(fields(include("id", "name", "metclass", "mass", "year", "geolocation"), excludeId()))
//Only when an Observable is subscribed to and data requested will the operation happen
//Explicitly subscribe:
                            observable.subscribe(new Observer[Document] {
                            override def onNext(result: Document): Unit = println(result.toJson())

                            override def onError(e: Throwable): Unit = println("Failed")

                            override def onComplete(): Unit = println("Completed")
                            })
                        }
                        case 6 => {
                            val observable: Observable[Document] = collection.find(exists("geolocation")).sort(ascending("geolocation")).projection(fields(include("id", "name", "metclass", "mass", "year", "geolocation"), excludeId()))
//Only when an Observable is subscribed to and data requested will the operation happen
//Explicitly subscribe:
                            observable.subscribe(new Observer[Document] {
                            override def onNext(result: Document): Unit = println(result.toJson())

                            override def onError(e: Throwable): Unit = println("Failed")

                            override def onComplete(): Unit = println("Completed")
                            })
                        }
                        case _ => {
                            println("This is not a valid input")
                        }
                    }
                }
//}
                    case 3 => {
                        println("1: Total Mass of Meteorites \n2: Average Mass of Meteorites")
                        val answer = myCheck.checkInt()
                        answer match {
                            case 1 => {val observable = collection.aggregate(List( group("$nametype", sum("totalMass", "$mass")), project(excludeId())))
                        observable.subscribe(new Observer[Document] {
                            override def onNext(result: Document): Unit = println(result.toJson())

                            override def onError(e: Throwable): Unit = println("Failed")

                            override def onComplete(): Unit = println("Completed")
                            })
                        }
                        case 2 => {
                            val observable = collection.aggregate(List( group("$nametype", avg("avgMass", "$mass")), project(excludeId())))
                        observable.subscribe(new Observer[Document] {
                            override def onNext(result: Document): Unit = println(result.toJson())

                            override def onError(e: Throwable): Unit = println("Failed")

                            override def onComplete(): Unit = println("Completed")
                            })
                          } 
                        }
                    }
                    case 4 => {
                        println("Update:\n1: Name \n2: ID\n3: Mass\n4: Year\n5: Class\n6: Coordinates")
                        val answer = myCheck.checkInt()
                        answer match {
                            case 1 => {
                                println("What name would you like to update?")
                                val userAnswer = myCheck.checkString()
                                println(userAnswer)
                                println("And what would you like to change it to?")
                                val secondAnswer = myCheck.checkString()
                                println(secondAnswer)
                                val observable: Observable[UpdateResult] = collection.updateOne(equal("name", userAnswer), set("name", secondAnswer))

//Only when an Observable is subscribed to and data requested will the operation happen
//Explicitly subscribe:
                                observable.subscribe(new Observer[UpdateResult] {
                                override def onNext(result: UpdateResult): Unit = println("Updated")

                                override def onError(e: Throwable): Unit = println("Failed")

                                override def onComplete(): Unit = println("Completed")
                                })
                            }
                            case 2 => {
                                println("What ID would you like to update?")
                                val userAnswer = myCheck.checkInt()
                                println(userAnswer)
                                println("And what would you like to change it to?")
                                val secondAnswer = myCheck.checkInt()
                                println(secondAnswer)
                                val observable: Observable[UpdateResult] = collection.updateOne(equal("id", userAnswer), set("id", secondAnswer))

//Only when an Observable is subscribed to and data requested will the operation happen
//Explicitly subscribe:
                                observable.subscribe(new Observer[UpdateResult] {
                                override def onNext(result: UpdateResult): Unit = println("Updated")

                                override def onError(e: Throwable): Unit = println("Failed")

                                override def onComplete(): Unit = println("Completed")
                                })
                            }
                            case 3 => {
                                println("What is the name of the meteorite that you want to update mass for?")
                                val userAnswer = myCheck.checkString()
                                println(userAnswer)
                                println("And what would you like to change the mass to?")
                                val secondAnswer = myCheck.checkInt()
                                println(secondAnswer)
                                val observable: Observable[UpdateResult] = collection.updateOne(equal("name", userAnswer), set("mass", secondAnswer))

//Only when an Observable is subscribed to and data requested will the operation happen
//Explicitly subscribe:
                                observable.subscribe(new Observer[UpdateResult] {
                                override def onNext(result: UpdateResult): Unit = println("Updated")

                                override def onError(e: Throwable): Unit = println("Failed")

                                override def onComplete(): Unit = println("Completed")
                                })
                            }
                            case 4 => {
                                println("What is the name of the meteorite that you want to update the year for?")
                                val userAnswer = myCheck.checkString()
                                println(userAnswer)
                                println("And what would you like to change the year to?")
                                val secondAnswer = myCheck.checkString()
                                println(secondAnswer)
                                val observable: Observable[UpdateResult] = collection.updateOne(equal("name", userAnswer), set("year", secondAnswer))

//Only when an Observable is subscribed to and data requested will the operation happen
//Explicitly subscribe:
                                observable.subscribe(new Observer[UpdateResult] {
                                override def onNext(result: UpdateResult): Unit = println("Updated")

                                override def onError(e: Throwable): Unit = println("Failed")

                                override def onComplete(): Unit = println("Completed")
                                })
                            }
                            case 5 => {
                                println("What is the name of the meteorite that you want to update the class for?")
                                val userAnswer = myCheck.checkString()
                                println(userAnswer)
                                println("And what would you like to change the year to?")
                                val secondAnswer = myCheck.checkString()
                                println(secondAnswer)
                                val observable: Observable[UpdateResult] = collection.updateOne(equal("name", userAnswer), set("metclass", secondAnswer))

//Only when an Observable is subscribed to and data requested will the operation happen
//Explicitly subscribe:
                                observable.subscribe(new Observer[UpdateResult] {
                                override def onNext(result: UpdateResult): Unit = println("Updated")

                                override def onError(e: Throwable): Unit = println("Failed")

                                override def onComplete(): Unit = println("Completed")
                                })
                            }
                            case 6 => {
                                println("What is the name of the meteorite that you want to update the coordinates for?")
                                val userAnswer = myCheck.checkString()
                                println(userAnswer)
                                println("And what would you like to change the coordinates to?")
                                println("Please enter longitude:")
                                val secondAnswer = myCheck.checkDouble()
                                println(secondAnswer)
                                println("Please enter latitude:")
                                val thirdAnswer = myCheck.checkDouble()
                                print(thirdAnswer)
                                val observable: Observable[UpdateResult] = collection.updateOne(equal("name", userAnswer), set("geolocation", List(secondAnswer, thirdAnswer)))

//Only when an Observable is subscribed to and data requested will the operation happen
//Explicitly subscribe:
                                observable.subscribe(new Observer[UpdateResult] {
                                override def onNext(result: UpdateResult): Unit = println("Updated")

                                override def onError(e: Throwable): Unit = println("Failed")

                                override def onComplete(): Unit = println("Completed")
                                })
                            }
                        }
                    }

                    case 5 => {
                        println("Other Queries: \n 1: Check the total mass of a class of Meteors \n 2: Average Mass of a class of Meteors \n 3: Smallest Meteor \n 4: Largest Meteor \n 5: Search for meteors above a certain mass \n 6: Meteors that landed between the years 1800-1900")
                        val answer = myCheck.checkInt()
                        answer match {
                            case 1 => {
                                println("What class of meteor would you like to check the total mass of?")
                                val userAnswer = myCheck.checkString()
                                val observable = collection.aggregate(Seq(model.Aggregates.filter(model.Filters.equal("metclass", userAnswer)),
                                model.Aggregates.group("$metclass", model.Accumulators.sum("totalMass", "$mass"))))

                                observable.subscribe(new Observer[Document] {
                                override def onNext(result: Document): Unit = println(result.toJson())

                                override def onError(e: Throwable): Unit = println("Failed")

                                override def onComplete(): Unit = println("Completed")
                            })
                            }
                            case 2 => {
                                println("What class of meteor would you like to check the average mass of?")
                                val userAnswer = myCheck.checkString()
                                val observable = collection.aggregate(Seq(model.Aggregates.filter(model.Filters.equal("metclass", userAnswer)),
                                model.Aggregates.group("$metclass", model.Accumulators.avg("avgMass", "$mass"))))

                                observable.subscribe(new Observer[Document] {
                                override def onNext(result: Document): Unit = println(result.toJson())

                                override def onError(e: Throwable): Unit = println("Failed")

                                override def onComplete(): Unit = println("Completed")
                            })
                            }
                            case 3 => {
                                println("The smallest meteor is: ")
                                val observable = collection.find(exists("mass")).sort(ascending("mass")).projection(fields(include("id", "name", "metclass", "mass", "year", "geolocation"), excludeId())).first()

                                observable.subscribe(new Observer[Document] {
                                override def onNext(result: Document): Unit = println(result.toJson())

                                override def onError(e: Throwable): Unit = println("Failed")

                                override def onComplete(): Unit = println("Completed")
                            })   
                            }
                            case 4 => {
                                println("The largest meteor is: ")
                                val observable = collection.find(exists("mass")).sort(descending("mass")).projection(fields(include("id", "name", "metclass", "mass", "year", "geolocation"), excludeId())).first()

                                observable.subscribe(new Observer[Document] {
                                override def onNext(result: Document): Unit = println(result.toJson())

                                override def onError(e: Throwable): Unit = println("Failed")

                                override def onComplete(): Unit = println("Completed")
                            })
                            }
                            case 5 => {
                                println("What class would you like to search for?")
                                val secondAnswer = myCheck.checkString()
                                println(secondAnswer)
                                println("What mass would you like to search for?")
                                val answer = myCheck.checkDouble()
                                println(answer)
                                val observable = collection.find(and(gte("mass", answer), equal("metclass", secondAnswer))).sort(ascending("mass")).projection(fields(include("id", "name", "metclass", "mass", "year", "geolocation"), excludeId()))

                                observable.subscribe(new Observer[Document] {
                                override def onNext(result: Document): Unit = println(result.toJson())

                                override def onError(e: Throwable): Unit = println("Failed")

                                override def onComplete(): Unit = println("Completed")
                            })
                            }
                            case 6 => {
                                val observable = collection.aggregate(Seq(model.Aggregates.filter(model.Filters.and(gte("year", "1800-01-01T00:00:00.000Z"), lte("year", "1900-01-01T00:00:00.000Z"))), sort(ascending("year")), project(fields(include("id", "name", "metclass", "mass", "year", "geolocation"), excludeId))))
                                
                                observable.subscribe(new Observer[Document] {
                                override def onNext(result: Document): Unit = println(result.toJson())

                                override def onError(e: Throwable): Unit = println("Failed")

                                override def onComplete(): Unit = println("Completed")
                            })
                            } 
                        }

                    }

                    case 6 => {
                            val observable: Observable[Completed] = collection.drop()

//Only when an Observable is subscribed to and data requested will the operation happen
//Explicitly subscribe:
                                observable.subscribe(new Observer[Completed] {
                                override def onNext(result: Completed): Unit = println("Updated")

                                override def onError(e: Throwable): Unit = println("Failed")

                                override def onComplete(): Unit = println("Completed")
                                })
                    }
                    case 7 => {
                        println("exiting...")
                        counter += 1
                    }
                    case _ => {
                        println("this is not a valid input")
                    }
                }
            }
            response(userResponse)
        }
    }
}





class MongoConn {
    val client: MongoClient = MongoClient()
    val database: MongoDatabase = client.getDatabase("test")
    val collection: MongoCollection[Document] = database.getCollection("meteors")

}