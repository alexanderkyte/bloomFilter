import scala.util.Random;
import scala.math.abs;

/** 
 *  A change to make to a BloomBitmask
 *
 *  Rather than mutating the underlying bitmask directly, it is
 *  powerful to be able to map over iterators of insertions and turn them into
 *  iterators of changes to later commit.
 *
 *  @constructor create a staged bitmask insertion from an offset
 *  to set and a bitmask with the relevant bit set.
 *
 *  @param byteOffset the offset into the array of bytes that makes up
 *  the underlying bitmask
 *
 *  @param localBitmask the byte bitmask to XOR into the sub-bitmask at
 *  the offset described by byteOffset
 */
case class BloomBitmaskInsertion(byteOffset: Int, localBitmask: Byte)

/** 
 *  A bitmask used by BloomFilter to track the seen state of various hashes
 *
 *  This bitmask attempts to be useful when used as a 
 *  persistent data structure by offering infrastructure to work
 *  with staged changes captured as instances of BloomBitmaskInsertion
 *
 *  @constructor create a bloom filter bitmask from an array of bitmask
 *  bytes
 *
 *  @param store An array of bytes that are to be manipulated as the
 *  bloom filter bitmask
 */
class BloomBitmask(val store: Array [Byte]) {
  /** 
   *  Copy constructor
   *
   *  @constructor create a new BloomBitmask from another one
   *
   *  @param other The other BloomBitmask to copy from
   *  
   */
  def this (other: BloomBitmask) {
    // Copy/clone constructor
    this(other.store.clone ())
  }

  /** 
   *  Produce staged bitmask insertion from hash
   *
   *  Our Bitmask type backs the mask with an array of bytes
   *  in order to have a flexible number of bits. This means we
   *  need to map an abstract bit-index to a concrete array index.
   *
   *  Rather than directly interacting with the underlying store
   *  after computing the index, we produce a BloomBitmaskInsertion
   *  object for the operation. 
   *
   *  A concrete BloomBitmask object is just a (lossy) approximation
   *  of Set[BloomBitmaskInsertion]. 
   *
   *  Since our bitmask operations commute in a bloom filter, we have
   *  a lot of power in choosing how we combine these insertions to make the
   *  end result.
   *
   *  I could have used SZArray from Scalaz. In reality
   *  it is really just doing what I'm doing here, and the batching
   *  is implicit, not explicit. Explicit staging of writes makes the
   *  parallel code easier to read here, in my opinion. This way also
   *  makes it easier to reuse this for both referentially
   *  transparent and mutable/efficient versions of Bitmask insertion.
   *
   *  In this function we are taking an offset into a bit array and
   *  producing the corresponding offset into an array 
   *  of bytes, and the offset into that byte.
   *
   *  @param hash The insertion to transform into the insertion
   */
  def preprocess (offset: Int): BloomBitmaskInsertion = {
    val bitsInByte = 8;
    val numBitsInBitmask = bitsInByte * store.length;
    val offsetInBounds = abs(offset) % numBitsInBitmask;

    val byteOffset = offsetInBounds / bitsInByte;
    val bitOffsetInByte = offsetInBounds % bitsInByte;
    val localBitmask = (1 << bitOffsetInByte).toByte;

    new BloomBitmaskInsertion(byteOffset, localBitmask)
  }

  /** 
   *  Query set status of bit in BloomBitmask 
   *
   *  Report whether the the bitmask has set the
   *  bit stored at the offset provided by the input
   *  hash.
   *
   *  @param hash The insertion to check for in the bitmask
   */
  def apply(hash: Int): Boolean = {
     // Find location hash refers to, check byte in array
     // has bit set.
    val insertion = preprocess (hash);

    // Has this insertion been committed before?
    val slot = store(insertion.byteOffset);
    val fetched = slot & insertion.localBitmask;

    fetched != 0
  }

  /** 
   *  Destructive insertion of BloomBitmaskInsertion
   *
   *  Apply the change described by the insertion to the
   *  data contained in this BloomBitmask
   *
   *  @param diff A BloomBitmaskInsertion to be inserted into 
   *  the current underlying bitmask data store
   */
  def commit (diff: BloomBitmaskInsertion): Unit = {
    // Commit the staged write. This is the only
    // function that mutates memory here. 
    //
    // Think of it as the "sink" in the dataflow model
    // of the words to add to the filter.
    val first = store(diff.byteOffset);
    val snd = diff.localBitmask; 
    val both = (first | snd).toByte;
    store(diff.byteOffset) = both;
  }

  /** 
   *  Destructive batch insertion of BloomBitmaskInsertions
   *
   *  Apply the changes described by the stream of insertions
   *  to the data contained in this BloomBitmask
   *
   *  @param diffs An iterator of instances of BloomBitmaskInsertion
   *  to be inserted into the current underlying bitmask data store
   */
  def commit (diffs: Iterator [BloomBitmaskInsertion]): Unit = {
    // Helper glue
    diffs.foreach(commit)
  }

  /** 
   *  Concatenate this bitmask with another
   *
   *  Two BloomBitmasks produced from the same BloomHashState
   *  can be concatenated together to produce a new BloomBitmask.
   *
   *  Functionally, inserting all of the hashes into one bitmask
   *  should be equivalent to splitting them between two bitmasks 
   *  and concatenating them.
   *
   *  @param other The other BloomBitmask to concatenate with
   *
   *  @return A new BloomBitmask which behaves as if it had the 
   *  hashes inserted into "this" and into the passed bitmask
   *
   */
  def concat (other: BloomBitmask): BloomBitmask = {
    val combined = store.iterator.zip(other.store.iterator)
    val newStore = combined.map { (pair) =>
      (pair._1 | pair._2).toByte
    }.toArray
    new BloomBitmask (newStore)
  }
}

/** 
 *  Companion object for BloomHashState, provides helpers
 *  needed by constructors
 */
object BloomHashState {
  /** 
   *  Produce a chosen number of random bytes
   *
   *  This helder method produces N 
   *
   *  @param seeds number of random bytes to produce
   *  @return an array of random bytes
   *
   */
  private def randomBytes (count: Int): Array[Byte] = {
    val seeds = new Array[Byte](count);
    Random.nextBytes (seeds);
    seeds
  }
}

/** 
 *  The hashing state for one or more BloomFilters
 *
 *  @constructor create a bloom filter hash state with a given
 *  collection of randomized hash function seeds
 *
 *  @param seeds Random bytes used to differentiate the
 *  hashing functions used during the multiple hashing rounds
 */
class BloomHashState (val seeds: Array[Byte])  {
  /** 
   *  Alternate constructor
   *
   *  @constructor create a bloom filter hash state with a given
   *  collection of randomized hash function seeds
   *
   *  @param seeds Random bytes used to differentiate the
   *  hashing functions used during the multiple hashing rounds
   */
  def this (timesToHash: Int) {
    // In order to have timesToHash number of hash functions, we
    // produce that many bitmasks, combine with the input, and hash.
    // We produce these random bytes for our hashing seeds.
    this (BloomHashState.randomBytes (timesToHash))
  }

  /** 
   *  Hash a single string for insertion into the bloom filter
   *
   *  We hash a string multiple times for insertion into the bloom filter.
   *
   *  Each hashing round applies XOR between the seed byte and every
   *  byte of the input string. The end hash for that round is the MD5
   *  hash of the resulting unique byte stream.
   *
   *  @param seeds Random bytes used to differentiate the
   *  hashing functions used during the multiple hashing rounds
   *
   *  @return an array of hashes for the string, with one output
   *  hash per hash function 
   *
   */
  def hashString (word: String): Array[Int] = {
    // Use the java supports for MD5. I like keeping Java
    // import scopes small because of name clashes, but I
    // understand it's generally a matter of team style.
    import java.security.MessageDigest
    import java.math.BigInteger

    // Shared by all hashes
    val bytes = word.getBytes ();

    // We have one hash per seed, so map over the seeds
    seeds.map { (seed) => 
      // Could probably make a bit faster, seems fast enough
      // when I profiled. Not sure of the cost of continually
      // getting the MD5 instance. This whole hash segment was probably
      // going to be the CPU bottleneck anyways. 
      val md = MessageDigest.getInstance ("MD5");
      val xored_bytes = bytes.map { (b) => (seed ^ b).toByte }
      val digest = md.digest (xored_bytes);
      val bigInt = new BigInteger (1, digest);
      bigInt.intValue ()
    }
  }
}


/** 
 *  A bloom filter structure for storing string values.
 *
 *  A bloom filter supports inserting values and reporting
 *  whether it contains those values. It is imprecise, but
 *  all errors are only false positives. If you insert a word,
 *  it will always report that it has seen that word.
 *
 *  Tuning bitmask size and the number of distinct hash functions
 *  allows one to trade space and time for that false positive rate.
 *
 *  This Bloom Filter supports mutating and non-mutating methods
 *  of insertion. 
 *
 *  @constructor create a bloom filter with a bitmask and a hashing function
 *  (See alternate constructor)
 *
 *  @param mask A bitmask data structure used by the bloom filter
 *  @param hasher The state for our multiple hash functions
 */
class BloomFilter (val mask: BloomBitmask, val hasher: BloomHashState) {
  /** 
   *  Map input word into changes to make to bitmask
   *
   *  Each word hashes into an index to set. This index can
   *  be split into a byte offset and a bit offset. The output
   *  diff describes the byte offset and a bitmask with a bit set
   *  at the bit offset.
   *
   *  These insertions commute and are combined later with an
   *  old bitmask to produce a new bitmask.
   *
   *  @param input A word to insert into the bitmask
   *
   *  @return An iterator of changes to make to a BloomBitmask
   *  to insert the input string
   *  
   */
  private def collectUpdates (input: String): Iterator [BloomBitmaskInsertion] = {
    hasher.hashString(input).map(mask.preprocess).iterator
  }

  /** 
   *  Nondestructive insertion into bitmask
   *
   *  This function adds all of the words contained
   *  in the iterator to the bitmask structure referenced
   *  by this bloom filter.
   *
   *  It produces a new copy, rather than mutating the
   *  underlying bitmask. 
   *
   *  @param batch An iterator of words to insert
   *
   *  @return A new bloom filter bitmask that reports 
   *  having seen every element that the current 
   *  filter's bitmask has seen, plus the newly added words
   *  
   */
  private def maskWith(batch: Iterator [String]): BloomBitmask = {
    // Referentially transparent, uses mutability for perf
    val updates = batch.flatMap (collectUpdates);
    val newMask = new BloomBitmask (mask);
    newMask.commit (updates)

    newMask
  }

  /** 
   *  Nondestructive insertion
   *
   *  This function adds all of the words contained
   *  in the iterator to the bloom filter. It produces a new
   *  copy, rather than mutating the underlying bitmask. 
   *
   *  Performance critical applications should favor this 
   *  method of inserting words, due to performance gains in
   *  batching the changes to make in the copied structure.
   *
   *  @param batch An iterator of words to insert
   *
   *  @return A new bloom filter that reports having seen
   *  every element that the current filter has seen, plus
   *  the newly added words
   *  
   */
  def appendMany (batch: Iterator [String]): BloomFilter = {
    new BloomFilter (maskWith(batch), hasher)
  }

  /** 
   *  Parallel nondestructive insertion
   *
   *  This function adds all of the words contained
   *  in the iterator to the bloom filter. It produces a new
   *  copy, rather than mutating the underlying bitmask.
   *
   *  The underlying data structure is produced in parallel,
   *  using scala's parallel collections libraries.
   *
   *  @param batch An iterator of words to insert
   *
   *  @return A new bloom filter that reports having seen
   *  every element that the current filter has seen, plus
   *  the newly added words
   *  
   */
  def appendManyPar (batch: Iterator [String]): BloomFilter = {
    val workItemSize = 10 * 1024;
    val groups = batch.grouped(workItemSize) // lazy
    val forked = groups.toStream.par.map { (group) =>
      maskWith(group.toIterator)
    }
    val collapsed = forked.foldLeft (mask) { (accum, added) =>
      accum.concat(added)
    }

    new BloomFilter (collapsed, hasher)
  }

  /** 
   *  Nondestructive single-item insertion
   *
   *  This is just a special case of the batched insertion$
   *  that adds one word.
   *
   *  Note: this is not destructive. It makes a copy of
   *  the bitmask. For code that needs that performance, do
   *  call this method in a loop. Use appendMany,
   *  appendManyPar, or add instead
   *
   *  @param word The word to add
   *
   *  @return A new bloom filter that reports having seen
   *  every element that the current filter has seen, plus
   *  the newly added word
   *  
   */
  def append (word: String): BloomFilter = {
    appendMany (Iterator.single (word))
  }

  /** 
   *  Destructive insertion
   *
   *  Destructively insert this word into this bitmask.
   *  It is not thread safe, but doesn't make any array copies.
   *  For performance-critical applications with access to a batch
   *  of words to insert, consider using addMany or addManyPar instead.
   *
   *  @param word The word to add
   *  
   */
  def add (word: String): Unit = {
    mask.commit (collectUpdates (word))
  }

  /** 
   *  Query Operation
   *
   *  Query the bitmask for the 
   *
   *  @param byteCount The number of bytes in the bitmask
   *  @param timesToHash The number of independent hash functions to use
   *
   *  @return whether the word has been encountered by the bitmask
   *
   */
  def apply (word: String): Boolean = {
    hasher.hashString (word).map(mask.apply).foldLeft (true) (_&&_)
  }

  /** 
   *  Alternate constructor
   *
   *  @constructor create a new BloomFilter with a bitmask size
   *  and hash count
   *
   *  @param byteCount The number of bytes in the bitmask
   *  @param timesToHash The number of independent hash functions to use
   *  
   */
  def this (byteCount: Int, timesToHash: Int) {
    this (new BloomBitmask (new Array[Byte] (byteCount)), new BloomHashState (timesToHash));
  }

  /** 
   *  Concatenate this filter with another
   *
   *  As long as two bloom filters have the same hash function and
   *  the same underlying bitmask length, we can OR the two bitmasks
   *  together to form a new bitmask that behaves as if it had the words 
   *  that made the two inputs.
   *
   *  @param byteCount The number of bytes in the bitmask
   *  @param timesToHash The number of independent hash functions to use
   *
   *  @return an optional type wrapping the new, concatenated filter
   */
  def concat (other: BloomFilter): Option[BloomFilter] = {
    if (other.hasher != hasher)
      return None
    if (other.mask.store.length != mask.store.length)
      return None

    Some(new BloomFilter(mask.concat (other.mask), hasher))
  }
}

