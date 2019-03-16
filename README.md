What is this?
----------------

This is my attempt at [Code Kata 05](http://codekata.com/kata/kata05-bloom-filters/), or the bloom filter
code kata.

It's an experiment in composable design and an exercise in scala programming.

How do I run/test?
----------------

```
git clone https://github.com/alexanderkyte/bloomFilter.git
cd bloomFilter
sbt test
```

##### Expected results:

```
Serial Stats:
	False positive rate: 0.0667% for (2 / 3000) with 235886 words
	Elapsed time: 0.5904 seconds

Parallel Stats:
	False positive rate: 0.0000% for (0 / 3000) with 235886 words
	Elapsed time: 0.7306 seconds

[info] BloomFilterTest:
[info] - BloomFilter.single
[info] - BloomFilter.multi
[info] - BloomFilter.mutation
[info] - BloomFilter.appendMany
[info] - BloomFilter.truePositives
[info] - BloomFilter.parallelTruePositives
[info] - BloomFilter.falsePositives
[info] - BloomFilter.parallelFalsePositives
[info] - BloomFilter.concat
[info] Run completed in 4 seconds, 497 milliseconds.
[info] Total number of tests run: 9
[info] Suites: completed 1, aborted 0
[info] Tests: succeeded 9, failed 0, canceled 0, ignored 0, pending 0
[info] All tests passed.
[success] Total time: 11 s, completed Mar 16, 2019 3:38:00 PM
```
