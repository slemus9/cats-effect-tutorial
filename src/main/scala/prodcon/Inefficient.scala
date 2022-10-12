package prodcon

import cats.syntax.all._
import cats.effect.{IO, IOApp, ExitCode, Sync, Ref}
import cats.effect.std.Console
import collection.immutable.Queue

object Inefficient extends IOApp {

  def producer [F[_]: Sync: Console] (
    queueR: Ref[F, Queue[Int]], 
    counter: Int
  ): F[Unit] = {
    val checkpoint = // to see if it's still alive
      if (counter % 10000 == 0) Console[F].println(s"Produced $counter items")
      else Sync[F].unit

    checkpoint >> 
    queueR.getAndUpdate(_.enqueue(counter + 1)) >>
    producer(queueR, counter + 1)
  }

  def consumer [F[_]: Sync: Console] (
    queueR: Ref[F, Queue[Int]]
  ): F[Unit] = {
    val take = queueR.modify(queue => queue.dequeueOption match {
      case None             => (queue, none)
      case Some(i -> queue) => (queue, i.some)
    })

    val checkpoint: Option[Int] => F[Unit] = {
      case Some(i) if i % 10000 == 0 => Console[F].println(s"Consumed $i items")
      case _                         => Sync[F].unit
    }

    (take >>= checkpoint) >> consumer(queueR)
  }

  def run (args: List[String]): IO[ExitCode] = {

    val makeQueue = Ref.of[IO, Queue[Int]](Queue.empty)
    makeQueue.flatMap { queueR => 
      (consumer(queueR), producer(queueR, 0))
        .parTupled
        .as(ExitCode.Success)
        .handleErrorWith { t => 
          Console[IO]
            .errorln(s"Error caught: ${t.getMessage}")  
            .as(ExitCode.Error)
        }
    }
  }
}