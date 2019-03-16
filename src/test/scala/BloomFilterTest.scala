import scala.io.Source
import scala.util.Random;
import scala.math.abs;

class BloomFilterTest extends org.scalatest.FunSuite {
  val filterSize = 8 * 64 * 1024 // 64kb
  val distinctHashFunctions = 5
  val wordList = "/usr/share/dict/words";

  test("BloomFilter.single") {
    val words = Iterator ("one")
    val filter = new BloomFilter (filterSize, distinctHashFunctions).appendMany (words);

    assert(filter ("one"))
  }

  test("BloomFilter.multi") {
    val words = Iterator ("one", "two", "three")
    val filter = new BloomFilter (filterSize, distinctHashFunctions).appendMany (words);

    assert(filter ("one"))
    assert(filter ("two"))
    assert(filter ("three"))
  }

  // The bad API
  test("BloomFilter.mutation") {
    val filter = new BloomFilter (filterSize, distinctHashFunctions);
    assert(!filter ("one"))
    filter.add ("one")
    assert(filter ("one"))
  }

  // The good APIs
  test("BloomFilter.appendMany") {
    val words = Source.fromFile(wordList)(io.Codec("UTF-8")).getLines;
    val filter = new BloomFilter (filterSize, distinctHashFunctions).appendMany (words);

    assert(filter ("true"))
    assert(filter ("bloom"))
    assert(filter ("love"))
    assert(filter ("algorithm"))
  }

  def truePositives (filter: BloomFilter): Unit = {
    // Reopen the word file and check that all of the words are in there
    // A bloom filter is expected to have only false positives as errors,
    // no false negatives
    val test_words = Source.fromFile(wordList)(io.Codec("UTF-8")).getLines;
    test_words.foreach { (word: String) => 
      assert(filter(word))
    }
  }

  val alphabet = ('a' to 'z')
  def randomAlpha (): Char = {
    alphabet(abs(Random.nextInt) % alphabet.length)
  }

  def randomFiveAlpha (): String = {
    // 5 times, get a random lowercase alpha character,
    // join to form string
    0.to(4).map((_: Int) => randomAlpha ()).mkString
  }

  def falsePositives (filter: BloomFilter): String = {
    val trialCount = 3000;

    // Produce a list of possible 5-letter combinations that may be words
    val possible_words = 0.to(trialCount).map ((_: Int) => randomFiveAlpha())

    // Filter for words the bitmask claims to have seen
    val positives: Array[String] = possible_words.filter(filter.apply).toArray

    // Prepare the real list of words to compare against
    val real_words = Source.fromFile(wordList)(io.Codec("UTF-8")).getLines.toSet;

    // Compute words the bitmask claims to have seen that aren't real words
    val false_positives = positives.filter (!real_words.contains(_))

    // Compute number of false positives and percentage of false positives
    val error = false_positives.length
    val error_percent = (error.toDouble / trialCount) * 100;

    assert (error_percent < 100)

    f"False positive rate: $error_percent%.4f%% for ($error / $trialCount) with ${real_words.size} words"
  }

  test("BloomFilter.truePositives") {
    val words = Source.fromFile(wordList)(io.Codec("UTF-8")).getLines;
    val filter = new BloomFilter (filterSize, distinctHashFunctions).appendMany (words);
    truePositives (filter);
  }

  test("BloomFilter.parallelTruePositives") {
    val wordList = "/usr/share/dict/words";
    val words = Source.fromFile(wordList)(io.Codec("UTF-8")).getLines;
    val filter = new BloomFilter (filterSize, distinctHashFunctions).appendManyPar (words);
  }

  val nanoSecondsInASecond = 1000000000;

  test("BloomFilter.falsePositives") {
    val words = Source.fromFile(wordList)(io.Codec("UTF-8")).getLines;

    val t0 = System.nanoTime()
    val filter = new BloomFilter (filterSize, distinctHashFunctions).appendMany (words);
    val t1 = System.nanoTime()
    val elapsed = (t1 - t0).toDouble / nanoSecondsInASecond;

    val results = falsePositives(filter);
    println(f"%nSerial Stats:\n\t$results")
    println(f"\tElapsed time: $elapsed%.4f seconds%n")
  }

  test("BloomFilter.parallelFalsePositives") {
    val words = Source.fromFile(wordList)(io.Codec("UTF-8")).getLines;

    val t0 = System.nanoTime()
    val filter = new BloomFilter (filterSize, distinctHashFunctions).appendManyPar (words);
    val t1 = System.nanoTime()
    val elapsed = (t1 - t0).toDouble / nanoSecondsInASecond;

    val results = falsePositives(filter);
    println(f"Parallel Stats:\n\t$results")
    println(f"\tElapsed time: $elapsed%.4f seconds%n")
  }

  // Test bloom filter concat
  test("BloomFilter.concat") {
    val wordFirst = Iterator ("one")
    val wordSecond = Iterator ("two", "three")

    val shared_setup = new BloomFilter (filterSize, distinctHashFunctions)
    val filterFirst = shared_setup.appendMany (wordFirst);
    val filterSecond = shared_setup.appendMany (wordSecond);

    assert(filterFirst("one"))
    assert(!filterFirst("two"))
    assert(!filterFirst("three"))

    assert(!filterSecond("one"))
    assert(filterSecond("two"))
    assert(filterSecond("three"))

    val filterCombinedForwardsOpt = filterFirst.concat(filterSecond)
    val filterCombinedBackwardsOpt = filterSecond.concat(filterFirst)

    // Concatenating is referentially transparent
    assert(filterFirst("one"))
    assert(!filterFirst("two"))
    assert(!filterFirst("three"))

    assert(!filterSecond("one"))
    assert(filterSecond("two"))
    assert(filterSecond("three"))

    assert (!filterCombinedForwardsOpt.isEmpty)
    assert (!filterCombinedBackwardsOpt.isEmpty)

    // Concatenating in either direction gets the same result
    val filterCombinedForwards = filterCombinedForwardsOpt.get
    val filterCombinedBackwards = filterCombinedBackwardsOpt.get

    assert(filterCombinedForwards ("one"))
    assert(filterCombinedForwards ("two"))
    assert(filterCombinedForwards ("three"))
    assert(filterCombinedBackwards ("one"))
    assert(filterCombinedBackwards ("two"))
    assert(filterCombinedBackwards ("three"))
  }
}
