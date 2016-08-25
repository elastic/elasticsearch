# Elasticsearch Microbenchmark Suite

This directory contains the microbenchmark suite of Elasticsearch. It relies on [JMH](http://openjdk.java.net/projects/code-tools/jmh/).

## Purpose

We do not want to microbenchmark everything but the kitchen sink and should typically rely on our 
[macrobenchmarks](https://elasticsearch-benchmarks.elastic.co/app/kibana#/dashboard/Nightly-Benchmark-Overview) with 
[Rally](http://github.com/elastic/rally). Microbenchmarks are intended to spot performance regressions in performance-critical components. 
The microbenchmark suite is also handy for ad-hoc microbenchmarks but please remove them again before merging your PR.

## Getting Started

Just run `gradle :benchmarks:jmh` from the project root directory. It will build all microbenchmarks, execute them and print the result.

## Running Microbenchmarks

Benchmarks are always run via Gradle with `gradle :benchmarks:jmh`.
 
Running via an IDE is not supported as the results are meaningless (we have no control over the JVM running the benchmarks).

If you want to run a specific benchmark class, e.g. `org.elasticsearch.benchmark.MySampleBenchmark` or have special requirements 
generate the uberjar with `gradle :benchmarks:jmhJar` and run it directly with:

```
java -jar benchmarks/build/distributions/elasticsearch-benchmarks-*.jar
```

JMH supports lots of command line parameters. Add `-h` to the command above to see the available command line options.

## Adding Microbenchmarks

Before adding a new microbenchmark, make yourself familiar with the JMH API. You can check our existing microbenchmarks and also the 
[JMH samples](http://hg.openjdk.java.net/code-tools/jmh/file/tip/jmh-samples/src/main/java/org/openjdk/jmh/samples/).

In contrast to tests, the actual name of the benchmark class is not relevant to JMH. However, stick to the naming convention and 
end the class name of a benchmark with `Benchmark`. To have JMH execute a benchmark, annotate the respective methods with `@Benchmark`.

## Tips and Best Practices

To get realistic results, you should exercise care when running benchmarks. Here are a few tips:

### Do

* Ensure that the system executing your microbenchmarks has as little load as possible. Shutdown every process that can cause unnecessary 
  runtime jitter. Watch the `Error` column in the benchmark results to see the run-to-run variance.
* Ensure to run enough warmup iterations to get the benchmark into a stable state. If you are unsure, don't change the defaults.
* Avoid CPU migrations by pinning your benchmarks to specific CPU cores. On Linux you can use `taskset`.
* Fix the CPU frequency to avoid Turbo Boost from kicking in and skewing your results. On Linux you can use `cpufreq-set` and the 
  `performance` CPU governor.
* Vary the problem input size with `@Param`.
* Use the integrated profilers in JMH to dig deeper if benchmark results to not match your hypotheses:
    * Run the generated uberjar directly and use `-prof gc` to check whether the garbage collector runs during a microbenchmarks and skews 
   your results. If so, try to force a GC between runs (`-gc true`) but watch out for the caveats.
    * Use `-prof perf` or `-prof perfasm` (both only available on Linux) to see hotspots.
* Have your benchmarks peer-reviewed.

### Don't

* Blindly believe the numbers that your microbenchmark produces but verify them by measuring e.g. with `-prof perfasm`.
* Run more threads than your number of CPU cores (in case you run multi-threaded microbenchmarks).
* Look only at the `Score` column and ignore `Error`. Instead take countermeasures to keep `Error` low / variance explainable.