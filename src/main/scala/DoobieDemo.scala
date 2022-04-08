import cats.effect.{ExitCode, IO, IOApp}
import doobie.{Fragment, HC, HPS}
import doobie.util.transactor.Transactor
import doobie.implicits._
object DoobieDemo extends IOApp{

  case class Actor(id: Int, name: String)

  val xa = Transactor.fromDriverManager[IO](
    "org.postgresql.Driver",
    "jdbc:postgresql://10.0.0.40:5432/ifrm", // connect URL
    "ifrm", // username
    "ifrm",                         // password
  )

  def findAllActorsNamesProgram: IO[List[String]] = {
    val findAllActorsQuery: doobie.Query0[String] = sql"""select "NAME" from "DEMO"."ACTORS"""".query[String]
    val findAllActors: doobie.ConnectionIO[List[String]] = findAllActorsQuery.to[List]
    findAllActors.transact(xa)
  }

 // single row
  def findActorByIdProgram(id: Int): IO[Actor] = {
    val findActorById: doobie.ConnectionIO[Actor] =
      sql"select id, name from actors where id = $id".query[Actor].unique
    findActorById.transact(xa)
  }

  // multiple rows
  def findAllActorsIdsAndNamesProgram: IO[List[(Int, String)]] = {
    val query: doobie.Query0[(Int, String)] = sql"select id, name from actors".query[(Int, String)]
    val findAllActors: doobie.ConnectionIO[List[(Int, String)]] = query.to[List]
    findAllActors.transact(xa)
  }

// option
  def findActorByIdProgramOption(id: Int): IO[Option[Actor]] = {
    val findActorById: doobie.ConnectionIO[Option[Actor]] =
      sql"select id, name from actors where id = $id".query[Actor].option
    findActorById.transact(xa)
  }

  // HC
  def findActorByNameUsingHCProgram(actorName: String): IO[Option[Actor]] = {
    val query = "select id, name from actors where name = ?"
    HC.stream[Actor](
      query,
      HPS.set(actorName),   // Parameters start from index 1 by default
      512
    ).compile
      .toList
      .map(_.headOption)
      .transact(xa)
  }

 // fragment

  def findActorsByInitialLetterUsingFragmentsProgram(initialLetter: String): IO[List[Actor]] = {
    val select: Fragment = fr"select id, name"
    val from: Fragment = fr"from actors"
    val where: Fragment = fr"where LEFT(name, 1) = $initialLetter"

    val statement = select ++ from ++ where

    statement.query[Actor].to[List].transact(xa)
  }

 // insert a row
  def saveActorProgram(name: String): IO[Int] = {
    val saveActor: doobie.ConnectionIO[Int] =
      sql"insert into actors (name) values ($name)".update.run
    saveActor.transact(xa)
  }

  // for comprehension
  def saveAndGetActorProgram(name: String): IO[Actor] = {
    val retrievedActor = for {
      id <- sql"insert into actors (name) values ($name)".update.withUniqueGeneratedKeys[Int]("id")
      actor <- sql"select * from actors where id = $id".query[Actor].unique
    } yield actor
    retrievedActor.transact(xa)
  }


  override def run(args: List[String]): IO[ExitCode] = {
    findAllActorsNamesProgram.map(res => println("All actors  --- "+res))
      .as(ExitCode.Success)

    findActorByIdProgram(1).map(res => println("actor by id  --- "+res))
      .as(ExitCode.Success)

    findActorByIdProgramOption(10).map(res => println("actor by id option --- "+res))
      .as(ExitCode.Success)

    saveActorProgram("Utkarsha").map(res => println("save actor  --- "+res))
      .as(ExitCode.Success)



  }
}
