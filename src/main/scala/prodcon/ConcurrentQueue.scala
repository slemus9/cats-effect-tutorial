package prodcon

import cats.syntax.all._
import cats.effect.{IO, IOApp, ExitCode, Async, Sync, Ref, Deferred}
import cats.effect.std.Console
import collection.immutable.Queue

trait ConcurrentQueue [F[_], A] {

  def offer (a: A): F[Unit]

  def take: F[A]
}

object ConcurrentQueue {

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

  private def offering [F[_]: Async, A] (
    stateR: Ref[F, State[F, A]],
    a: A
  ): F[Unit] = Deferred[F, Unit].flatMap { offerer => 
    Async[F].uncancelable(poll => stateR.modify {
      case s @ State(queue, capacity, takers, offerers) =>
        if (takers.nonEmpty) {
          val (taker, rest) = takers.dequeue
          s.copy(takers = rest) -> taker.complete(a).void
        }

        else if (queue.size < capacity)
          s.copy(queue = queue.enqueue(a)) -> Async[F].unit

        else
          s.copy(offerers = offerers.enqueue(a -> offerer)) -> poll(offerer.get) 
    }.flatten)  
  }

  private def taking [F[_]: Async, A] (
    stateR: Ref[F, State[F, A]]
  ): F[A] = Deferred[F, A].flatMap { taker => 
    Async[F].uncancelable(poll => stateR.modify { case s @ State(queue, _, takers, offerers) =>
      if (queue.nonEmpty && offerers.isEmpty) {
        val (a, rest) = queue.dequeue
        s.copy(queue = rest) -> Async[F].pure(a)
      }

      else if (queue.nonEmpty) {
        val (a1, rest) = queue.dequeue
        val ((a2, release), tail) = offerers.dequeue 
        s.copy(queue = rest.enqueue(a2), offerers = tail) -> release.complete(()).as(a1)
      }

      else if (offerers.nonEmpty) {
        val ((a, release), tail) = offerers.dequeue
        s.copy(offerers = tail) -> release.complete(()).as(a)
      }

      else 
        s.copy(takers = takers.enqueue(taker)) -> poll(taker.get)
    }.flatten)
  }

  def bounded [F[_]: Async, A] (capacity: Int): F[ConcurrentQueue[F, A]] = 
    Ref.of[F, State[F, A]](State.empty(capacity)).map { stateR =>       
      new ConcurrentQueue[F, A] {

        def offer (a: A): F[Unit] = offering(stateR, a)

        def take: F[A] = taking(stateR)
      }
    }
}

object ConcurrentQueueExample extends IOApp {

  def checkpoint [F[_]: Sync: Console] (
    name: String, id: Int
  ): Int => F[Unit] = i => 
    if (i % 10000 == 0) Console[F].println(s"$name $id reached $i items")
    else Sync[F].unit

  def producer [F[_]: Async: Console] (
    id: Int,
    counterR: Ref[F, Int],
    queue: ConcurrentQueue[F, Int]
  ): F[Unit] = 
    counterR
      .getAndUpdate(_ + 1)
      .flatTap(queue.offer)
      .flatMap(checkpoint("Producer", id))
      .foreverM

  def consumer [F[_]: Sync: Console] (
    id: Int,
    queue: ConcurrentQueue[F, Int]
  ): F[Unit] =
    queue
      .take
      .flatMap(checkpoint("Consumer", id))
      .foreverM

  def run(args: List[String]): IO[ExitCode] = for {
    queue     <- ConcurrentQueue.bounded[IO, Int](100)
    counterR  <- Ref.of[IO, Int](1)
    producers =  List.range(1, 11).map(
      producer(_, counterR, queue)
    )
    consumers =  List.range(1, 11).map(
      consumer(_, queue)
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