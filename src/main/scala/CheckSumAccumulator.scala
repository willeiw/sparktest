class CheckSumAccumulator {
  var sum = 0
  def add(b:Byte)={
    sum += b
  }
  def checkSum():Int={
    return ~(sum &0xFF)+1
  }

}
