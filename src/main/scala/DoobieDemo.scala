import cats.effect.{ExitCode, IO, IOApp}
import doobie.util.transactor.Transactor
import doobie.implicits._
object DoobieDemo extends IOApp{

  val xa = Transactor.fromDriverManager[IO](
    "org.postgresql.Driver",
    "jdbc:postgresql://10.0.0.40:5432/ifrm", // connect URL
    "ifrm", // username
    "ifrm",                         // password
  )
  def findAllActorsNamesProgram: IO[List[String]] = {
    val findAllActorsQuery: doobie.Query0[String] = sql"select name from actors".query[String]
    val findAllActors: doobie.ConnectionIO[List[String]] = findAllActorsQuery.to[List]
    findAllActors.transact(xa)
  }
  override def run(args: List[String]): IO[ExitCode] = {
    findAllActorsNamesProgram.map(println)
      .as(ExitCode.Success)
  }
}
