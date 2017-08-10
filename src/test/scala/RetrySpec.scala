import io.clickhouse.ext.tools.Retry
import org.scalatest.{Succeeded, _}
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext}
import scala.util.{Failure, Success}

class RetrySpec extends FlatSpec with Matchers {
  "Retry" should "async" in {

    implicit val ec = ExecutionContext.global

    var counter = 0
    val ignoreThrowable = (ex: Throwable) => ex match {
        case _e: NullPointerException =>
          counter += 1
          false
        case _ => true
      }

    val maxRetry = 3
    val f = Retry.retry(maxRetry, ignoreThrowable = ignoreThrowable){
      throw new NullPointerException("test")
      1
    }

    f onComplete{
      case Success(r) =>
      case Failure(ex) =>
        assert(counter == maxRetry)
    }

    Thread.sleep(10.seconds.toMillis)

    true should === (true)
  }

  "Retry" should "sync" in {

    var counter = 0
    val ignoreThrowable = (ex: Throwable) => ex match {
      case _e: NullPointerException =>
        counter += 1
        false
      case _ => true
    }

    val maxRetry = 3

    try{
      val r = Retry.retrySync(maxRetry, ignoreThrowable = ignoreThrowable) {
        throw new NullPointerException("test")
        1
      }
    }catch {
      case e: Throwable =>
        assert(counter == maxRetry)
        println("")
    }

    assert(true)
  }
}
