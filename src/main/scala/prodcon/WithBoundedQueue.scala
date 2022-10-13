package prodcon

import cats.syntax.all._
import cats.effect.{IO, IOApp, ExitCode, Async, Sync, Ref, Deferred}
import cats.effect.std.Console
import collection.immutable.Queue

object WithBoundedQueue extends IOApp {

  final case class State [F[_], A] (
    queue: Queue[A],
    capacity: Int,
    takers: Queue[Deferred[F, A]],
    offerers: Queue[(A, Deferred[F, Unit])]
  )

  object State {
    def empty [F[_], A] (capacity: Int): State[F, A] =
      State(Queue.empty, capacity, Queue.empty, Queue.empty)
  }

  def consumer [F[_]: Async: Console] (
    id: Int, stateR: Ref[F, State[F, Int]]
  ): F[Unit] = {

    val take: F[Int] = Deferred[F, Int].flatMap { taker => 
      Async[F].uncancelable(poll => 
        stateR.modify { case s @ State(queue, _, takers, offerers) =>
          if (queue.nonEmpty && offerers.isEmpty) {
            val (i, rest) = queue.dequeue
            s.copy(queue = rest) -> Async[F].pure(i)
          }
  
          else if (queue.nonEmpty) {
            val (i, rest) = queue.dequeue
            val ((j, release), tail) = offerers.dequeue // j is the element that the first offerer is offering
            s.copy(queue = rest.enqueue(j), offerers = tail) -> release.complete(()).as(i) // enqueue j and return i to the consumer
          }
  
          else if (offerers.nonEmpty) {
            val ((i, release), tail) = offerers.dequeue
            s.copy(offerers = tail) -> release.complete(()).as(i)
          }
  
          else 
            s.copy(takers = takers.enqueue(taker)) -> poll(taker.get)
        }.flatten
      )
    }

    val checkpoint: Int => F[Unit] = i => 
      if (i % 10000 == 0) Console[F].println(s"Consumer $id has reached $i items")
      else Sync[F].unit

    (take >>= checkpoint) >> consumer(id, stateR)
  }

  def producer [F[_]: Async: Console] (
    id: Int,
    counterR: Ref[F, Int],
    stateR: Ref[F, State[F, Int]]
  ): F[Unit] = {

    val offer: Int => F[Unit] = i => Deferred[F, Unit].flatMap { offerer => 
      Async[F].uncancelable(poll => stateR.modify {
        case s @ State(queue, capacity, takers, offerers) =>
          if (takers.nonEmpty) {
            val (taker, rest) = takers.dequeue
            s.copy(takers = rest) -> taker.complete(i).void
          }

          else if (queue.size < capacity)
            s.copy(queue = queue.enqueue(i)) -> Async[F].unit

          else
            // offerer.get blocks the fiber, so we should place the call inside poll to ensure that it can be canceled
            s.copy(offerers = offerers.enqueue(i -> offerer)) -> poll(offerer.get) 
      }.flatten)
    }

    val checkpoint: Int => F[Unit] = i => 
      if (i % 10000 == 0) Console[F].println(s"Producer $id has reached $i items")
      else Sync[F].unit

    counterR.getAndUpdate(_ + 1)
      .flatTap(offer)
      .flatMap(checkpoint)
      .flatMap(_ => producer(id, counterR, stateR))
  }

  def run (args: List[String]): IO[ExitCode] = for {
    stateR    <- Ref.of[IO, State[IO, Int]](State.empty(100))
    counterR  <- Ref.of[IO, Int](1)
    producers =  List.range(1, 11).map(
      producer(_, counterR, stateR)
    )
    consumers =  List.range(1, 11).map(
      consumer(_, stateR)
    )
    res       <- (producers ++ consumers)
      .parSequence
      .as(ExitCode.Success)
      .handleErrorWith { t => 
        Console[IO]
          .errorln(s"Error caught: ${t.getMessage}")
          .as(ExitCode.Error)  
      }
  } yield res
}