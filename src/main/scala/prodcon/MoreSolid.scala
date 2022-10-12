package prodcon

import cats.syntax.all._
import cats.effect.{IO, IOApp, ExitCode, Async, Sync, Ref, Deferred}
import cats.effect.std.Console
import collection.immutable.Queue

object MoreSolid extends IOApp {

  final case class State [F[_], A] (
    queue: Queue[A], takers: Queue[Deferred[F, A]]
  )

  object State {
    def empty [F[_], A]: State[F, A] = State(Queue.empty, Queue.empty)
  }

  def consumer [F[_]: Async: Console] (
    id: Int, stateR: Ref[F, State[F, Int]]
  ): F[Unit] = {

    val take: F[Int] = Deferred[F, Int].flatMap { taker => 
      stateR.modify {
        case State(queue, takers) if queue.nonEmpty => 
          val (i, rest) = queue.dequeue
          State(rest, takers) -> Async[F].pure(i)
        
        case State(queue, takers) => 
          State(queue, takers.enqueue(taker)) -> taker.get // Block the caller until an element is available
      }.flatten
    }

    val checkpoint: Int => F[Unit] = { i => 
      if (i % 10000 == 0) Console[F].println(s"Consumer $id has reached $i items")
      else Async[F].unit  
    }

    (take >>= checkpoint) >> consumer(id, stateR)
  }

  def producer [F[_]: Sync: Console] (
    id: Int, 
    counterR: Ref[F, Int],
    stateR: Ref[F, State[F, Int]]
  ): F[Unit] = {

    def offer (i: Int): F[Unit] = stateR.modify {
      case State(queue, takers) if takers.nonEmpty => 
        val (taker, rest) = takers.dequeue
        State(queue, rest) -> taker.complete(i).void

      case State(queue, takers) => 
        State(queue.enqueue(i), takers) -> Sync[F].unit
    }.flatten

    val checkpoint: Int => F[Unit] = { i => 
      if (i % 10000 == 0) Console[F].println(s"Producer $id has reached $i items")
      else Sync[F].unit  
    }

    counterR
      .getAndUpdate(_ + 1)
      .flatTap(offer)
      .flatMap(checkpoint)
      .flatMap(_ => producer(id, counterR, stateR))
  }

  def run (args: List[String]): IO[ExitCode] = 
    for {
      stateR <- Ref.of[IO, State[IO, Int]](State.empty)
      counterR <- Ref.of[IO, Int](1)
      producers = List.range(1, 11).map(
        producer(_, counterR, stateR)
      )
      consumers = List.range(1, 11).map(
        consumer(_, stateR)
      )
      res <- (producers ++ consumers)
        .parSequence
        .as(ExitCode.Success)
        .handleErrorWith { t => 
          Console[IO]
            .errorln(s"Error caught: ${t.getMessage}")
            .as(ExitCode.Error)
        }
    } yield res
}