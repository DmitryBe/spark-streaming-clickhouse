import org.scalatest._

class TemplateSpec extends FlatSpec with Matchers {
  "Hello" should "have tests" in {

    true should === (true)
  }
}
