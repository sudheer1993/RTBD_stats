import breeze.linalg.Options.Median
import org.apache.spark.mllib.stat.{MultivariateStatisticalSummary, Statistics}
import org.apache.spark.util.StatCounter
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by sudheer on 10/2/15.
 */
object StatsApp {

  def main(args: Array[String]) {

    val conf = new SparkConf().setMaster("local[*]").setAppName("StatsApp")
    val sc = new SparkContext(conf)

    val lines = sc.textFile("src/main/resources/input").map(_.toInt).cache()
    val array = lines.collect().sortBy(identity)
    val length = array.length
    // Median :
    var median: Double = 0.0
    if(length % 2 == 0)
      {
        median = (array{length/2}+array{length/2-1}).toFloat / 2
      }
    else
      {
        median = array{length/2}.toFloat
      }
    // Mode :
    var maxCount = 0
    var mode = 0
    var mode2 = 0
    for(i <- 1 until array.length-1)
      {
        var c= 0
        for(j <- 0 until array.length)
          {
            if(array(j)==array(i))
              {
                c = c + 1
              }
          }
        if (c > maxCount)
          {
            maxCount = c
            mode = array{i}
          }

      }
   println("The largest number is "+lines.max+"\nThe smallest number is "+lines.min+"\nThe mean of given numbers is "+lines.mean+"\nThe Variance given numbers is "+lines.variance+"\nThe Std Dev given numbers is "+lines.stdev()+"\nThe Range given numbers is "+(lines.max()-lines.min())+"\nThe Median of given numbers is "+median+"\nThe Mode is "+mode+" with count "+maxCount)
    sc.stop()
  }
}
