package files

import cats.effect.{
  IOApp, IO, ExitCode, 
  Resource, Sync, Concurrent,
  Async
}
import cats.syntax.all._
import cats.{Applicative, Monad, MonadError, Parallel}
import cats.effect.std.Console
import java.io._
import java.nio.file.{Paths, Path, Files}
import scala.jdk.StreamConverters._
import scala.util.Try


object CopyPoly extends IOApp {

  def inputStream [F[_]: Sync] (f: File): Resource[F, FileInputStream] =
    Resource.make {
      Sync[F].blocking(new FileInputStream(f)) // acquire
    } { in => 
      Sync[F]
        .blocking(in.close)               // release
        .handleErrorWith(_ => Sync[F].unit)
    }

  def outputStream [F[_]: Sync] (f: File): Resource[F, FileOutputStream] =
    Resource.make {
      Sync[F].blocking(new FileOutputStream(f))
    } { out =>
      Sync[F]
        .blocking(out.close)
        .handleErrorWith(_ => Sync[F].unit)  
    }

  def inputOutputStreams [F[_]: Sync] (in: File, out: File): Resource[F, (InputStream, OutputStream)] =
    (inputStream(in), outputStream(out)).tupled

  def transmit [F[_]: Sync] (
    origin: InputStream, 
    destination: OutputStream, 
    buffer: Array[Byte], 
    acc: Long
  ): F[Long] = {

    val readChunk = Sync[F].blocking { origin.read(buffer, 0, buffer.size) }
    def writeChunk (amount: Int) = Sync[F].blocking { destination.write(buffer, 0, amount) }
    def writeNextChunk (amount: Int) = transmit(origin, destination, buffer, acc + amount)

    readChunk.flatMap { amount =>
      if (amount > -1) writeChunk(amount) >> writeNextChunk(amount)
      else Sync[F].pure(acc)
    }
  }

  def transfer [F[_]: Sync] (origin: InputStream, destination: OutputStream): F[Long] = 
    transmit(origin, destination, new Array(1024 * 10), 0L)

  def copy [F[_]: Sync] (origin: File, destination: File): F[Long] = 
    inputOutputStreams(origin, destination).use {
      case (in, out) => transfer(in, out)
    }

  def syncTry [F[_]: Sync, A] (body: => A): F[A] = 
    Sync[F].fromTry(Try { body })

  // Raise an error if the origin and destination paths are the same
  def guardDiffOriginAndDest [F[_]] (origin: String, dest: String)
    (implicit F: MonadError[F, Throwable]) =
    F.raiseWhen (origin == dest) {
      new IllegalArgumentException("origin file should be different from destination")
    }

  /*
    If the destination file already exists, it asks the user if they want
    to overwrite it. If the answer is positive or if the file doesn't 
    exists, it returns true
  */
  def checkExistingDest [F[_]: Console: Monad] (intendedDest: Path) =
    if (Files.exists(intendedDest)) for {
      _   <- Console[F].println(s"Are you sure you want to overwrite: $intendedDest? [Y/n]")
      ans <- Console[F].readLine.map(_.toLowerCase)
    } yield ans.isBlank || ans == "y" || ans == "yes"
    else Applicative[F].pure(true)
  
  def getCopyFileRequest [F[_]: Console] (origin: String, dest: String)
    (implicit F: MonadError[F, Throwable]): F[Option[(Path, Path)]] = 
    for {
      _          <- guardDiffOriginAndDest(origin, dest)
      (originPath, destPath) = Paths.get(origin) -> Paths.get(dest)
      continue   <- checkExistingDest(destPath)
    } yield Option.when (continue) (originPath -> destPath)
  
  // Gets the parent of `p` if it exists, otherwise returns `p`
  def getParentIfExists (p: Path): Path = {
    val parent = p.getParent
    if (parent == null) p else parent
  }
  
  def processSingleCopyRequest [F[_]: Sync: Console] (origin: File, dest: File): F[Long] =
    for {
      count      <- copy(origin, dest)
      _          <- Console[F].println(s"$count bytes copied from ${origin} to ${dest}")
    } yield count

  /*
    Copies the contents of the origin file to the destination file. Logs the
    number of copied bytes
  */
  def copySingle [F[_]: Sync: Console] (origin: Path, dest: Path): F[Long] = 
    for {
      originFile <- syncTry(new File(origin.toString))
      destParent =  getParentIfExists(dest)
      _          <- syncTry(Files.createDirectories(destParent))
      destFile   <- syncTry(new File(dest.toString))
      count      <- processSingleCopyRequest(originFile, destFile)
    } yield count

  /*
    Copies the contents of the origin file to the destination file. If the origin 
    file is a directory, it recursively copies all of its contents.
    Returns a Long number with the total number of copied bytes
  */
  def copyAll [F[_]: Sync: Parallel: Console] (origin: Path, dest: Path): F[Long] = {
    
    val allOriginPaths = Files
      .walk(origin)
      .filter(!Files.isDirectory(_))
      .toScala(LazyList)

    val allDestPaths = allOriginPaths.map { 
      p => dest.resolve(origin.relativize(p))
    }

    def copyChunk (chunk: LazyList[(Path, Path)]) = 
      chunk
        .traverse {
          case (origin, dest) => copySingle(origin, dest)
        }
        .map(_.sum)

    val chunks = LazyList.from(
      allOriginPaths.zip(allDestPaths).grouped(100) // Chunk size should be a parameter
    )

    chunks
      .parTraverse(copyChunk)
      .map(_.sum)
  }

  def run(args: List[String]): IO[ExitCode] = {
    
    val getRequest = args match {
      case List(origin, dest) => getCopyFileRequest[IO](origin, dest)
      case _ => IO.raiseError(
        new IllegalArgumentException("Need origin and destination files")
      )
    }

    getRequest
      .flatMap {
        case Some((origin, dest)) if !Files.isDirectory(origin) =>
          for {
            originFile <- syncTry[IO, File](new File(origin.toString))
            destFile   <- syncTry[IO, File](new File(dest.toString))
            count      <- processSingleCopyRequest[IO](originFile, destFile)
          } yield count
        case Some((origin, dest)) => copyAll[IO](origin, dest).flatMap { totalCopied => 
          IO.println(s"$totalCopied bytes copied in total")
        }
        case None                 => IO.unit
      }
      .as(ExitCode.Success)
  }
}