package sparkvideocourse

import org.scalatest.funsuite.AnyFunSuite

class FirstTest extends AnyFunSuite {

  test("add(2, 3) returns 5"){
    val result = Main.add(2, 3)
    assert(result == 5)

  }

}
