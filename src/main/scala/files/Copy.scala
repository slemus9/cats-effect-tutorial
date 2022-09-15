package files

import cats.effect.{IOApp, IO, ExitCode, Resource}
import cats.syntax.all._
import java.io._
import scala.util.Try

object Copy extends IOApp {

  def inputStream (f: File): Resource[IO, FileInputStream] =
    Resource.make {
      IO.blocking(new FileInputStream(f)) // acquire
    } { in => 
      IO.blocking(in.close)               // release
        .handleErrorWith(IO.println)
    }
  
  // Alternative definition for classes that implement the java.lang.AutoCloseable interface
  // We are not able to specify actions when closing the resource
  def inputStream1 (f: File): Resource[IO, FileInputStream] =
    Resource.fromAutoCloseable(
      IO { new FileInputStream(f) }
    )

  def outputStream (f: File): Resource[IO, FileOutputStream] =
    Resource.make {
      IO.blocking(new FileOutputStream(f))
    } { out =>
      IO.blocking(out.close)
        .handleErrorWith(IO.println)  
    }

  def inputOutputStreams (in: File, out: File): Resource[IO, (InputStream, OutputStream)] =
    inputStream(in) both outputStream(out)

  def transmit (
    origin: InputStream, 
    destination: OutputStream, 
    buffer: Array[Byte], 
    acc: Long
  ): IO[Long] = {

    val readChunk = IO.blocking { origin.read(buffer, 0, buffer.size) }
    def writeChunk (amount: Int) = IO.blocking { destination.write(buffer, 0, amount) }
    def writeNextChunk (amount: Int) = transmit(origin, destination, buffer, acc + amount)

    readChunk.flatMap { amount =>
      if (amount > -1) writeChunk(amount) >> writeNextChunk(amount)
      else IO.pure(acc)
    }
  }


  def transfer (origin: InputStream, destination: OutputStream): IO[Long] = 
    transmit(origin, destination, new Array(1024 * 10), 0L)

  def copy (origin: File, destination: File): IO[Long] = 
    inputOutputStreams(origin, destination).use {
      case (in, out) => transfer(in, out)
    }

  def run(args: List[String]): IO[ExitCode] = {
    
    val getArgs = 
      IO.fromTry(Try(
          new File(args(0)) -> new File(args(1))
        ))
        .orRaise { 
          new IllegalArgumentException("Need origin and destination files") 
        }

    for {
      args  <- getArgs
      (origin, destination) = args
      count <- copy(origin, destination)
      _     <- IO.println(s"$count bytes copied from ${origin.getPath} to ${destination.getPath}")
    } yield ExitCode.Success
  }
}