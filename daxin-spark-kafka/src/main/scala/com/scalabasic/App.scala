package com.scalabasic

/**
 * @author ${user.name}
 */
object App {
  
  def foo(x : Array[String]) = x.foldLeft("")((a,b) => a + b)
  
  def main(args : Array[String]) {

    val map=Map("1"->"11","2"->"22","3"->"33")
    map.map{
      case(a,b)=>
        println(a+" "+b)
    }

  }

}
