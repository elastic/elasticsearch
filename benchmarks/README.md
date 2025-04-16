# Elasticsearch Microbenchmark Suite

This directory contains the microbenchmark suite of Elasticsearch. It relies on [JMH](http://openjdk.java.net/projects/code-tools/jmh/).

## Purpose

We do not want to microbenchmark everything but the kitchen sink and should typically rely on our
[macrobenchmarks](https://elasticsearch-benchmarks.elastic.co/) with
[Rally](http://github.com/elastic/rally). Microbenchmarks are intended to spot performance regressions in performance-critical components.
The microbenchmark suite is also handy for ad-hoc microbenchmarks, but please remove them again before merging your PR.

## Getting Started

Just run `gradlew -p benchmarks run` from the project root
directory. It will build all microbenchmarks, execute them and print
the result.

## Running Microbenchmarks

Running via an IDE is not supported as the results are meaningless
because we have no control over the JVM running the benchmarks.

If you want to run a specific benchmark class like, say,
`MemoryStatsBenchmark`, you can use `--args`:

```
gradlew -p benchmarks run --args 'MemoryStatsBenchmark'
```

Everything in the `'` gets sent on the command line to JMH.

You can set benchmark parameters with `-p`:
```
gradlew -p benchmarks/ run --args 'RoundingBenchmark.round -prounder=es -prange="2000-10-01 to 2000-11-01" -pzone=America/New_York -pinterval=10d -pcount=1000000'
```

The benchmark code defines default values for the parameters, so if
you leave any out JMH will run with each default value, one after
the other. This will run with `interval` set to `calendar year` then
`calendar hour` then `10d` then `5d` then `1h`:
```
gradlew -p benchmarks/ run --args 'RoundingBenchmark.round -prounder=es -prange="2000-10-01 to 2000-11-01" -pzone=America/New_York -pcount=1000000'
```


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
* Use the integrated profilers in JMH to dig deeper if benchmark results do not match your hypotheses:
    * Add `-prof gc` to the options to check whether the garbage collector runs during a microbenchmark and skews
   your results. If so, try to force a GC between runs (`-gc true`) but watch out for the caveats.
    * Add `-prof perf` or `-prof perfasm` (both only available on Linux, see Disassembling below) to see hotspots.
    * Add `-prof async` to see hotspots.
* Have your benchmarks peer-reviewed.

### Don't

* Blindly believe the numbers that your microbenchmark produces but verify them by measuring e.g. with `-prof perfasm`.
* Run more threads than your number of CPU cores (in case you run multi-threaded microbenchmarks).
* Look only at the `Score` column and ignore `Error`. Instead, take countermeasures to keep `Error` low / variance explainable.

## Disassembling

NOTE: Linux only. Sorry Mac and Windows.

Disassembling is fun! Maybe not always useful, but always fun! Generally, you'll want to install `perf` and the JDK's `hsdis`.
`perf` is generally available via `apg-get install perf` or `pacman -S perf linux-tools`. `hsdis` you'll want to compile from source. is a little more involved. This worked
on 2020-08-01:

```
git clone git@github.com:openjdk/jdk.git
cd jdk
git checkout jdk-24-ga
# Get a known good binutils
wget https://ftp.gnu.org/gnu/binutils/binutils-2.35.tar.gz
tar xf binutils-2.35.tar.gz
bash configure --with-hsdis=binutils --with-binutils-src=binutils-2.35 \
    --with-boot-jdk=~/.gradle/jdks/oracle_corporation-24-amd64-linux.2
make build-hsdis
cp ./build/linux-x86_64-server-release/jdk/lib/hsdis-amd64.so \
    ~/.gradle/jdks/oracle_corporation-24-amd64-linux.2/lib/hsdis.so
```

If you want to disassemble a single method do something like this:

```
gradlew -p benchmarks run --args ' MemoryStatsBenchmark -jvmArgs "-XX:+UnlockDiagnosticVMOptions -XX:CompileCommand=print,*.yourMethodName -XX:PrintAssemblyOptions=intel"
```

If you want `perf` to find the hot methods for you, then do add `-prof perfasm`.

NOTE: `perfasm` will need more access:
```
sudo bash
echo -1 > /proc/sys/kernel/perf_event_paranoid
exit
```

If you get warnings like:
```
The perf event count is suspiciously low (0).
```
then check if you are bumping into [this](https://man.archlinux.org/man/perf-stat.1.en#INTEL_HYBRID_SUPPORT)
by running:
```
perf stat -B dd if=/dev/zero of=/dev/null count=1000000
```

If you see lines like:
```
         765019980      cpu_atom/cycles/                 #    1.728 GHz                         (0.60%)
        2258845959      cpu_core/cycles/                 #    5.103 GHz                         (99.18%)
```
then `perf` is just not going to work for you.

## Async Profiler

Note: Linux and Mac only. Sorry Windows.

IMPORTANT: The 2.0 version of the profiler doesn't seem to be compatible
with JMH as of 2021-04-30.

The async profiler is neat because it does not suffer from the safepoint
bias problem. And because it makes pretty flame graphs!

Let user processes read performance stuff:
```
sudo bash
echo 0 > /proc/sys/kernel/kptr_restrict
echo 1 > /proc/sys/kernel/perf_event_paranoid
exit
```

Grab the async profiler from https://github.com/jvm-profiling-tools/async-profiler
and run `prof async` like so:
```
gradlew -p benchmarks/ run --args 'LongKeyedBucketOrdsBenchmark.multiBucket -prof "async:libPath=/home/nik9000/Downloads/async-profiler-3.0-29ee888-linux-x64/lib/libasyncProfiler.so;dir=/tmp/prof;output=flamegraph"'
```

Note: As of January 2025 the latest release of async profiler doesn't work
      with our JDK but the nightly is fine.

If you are on Mac, this'll warn you that you downloaded the shared library from
the internet. You'll need to go to settings and allow it to run.

The profiler tells you it'll be more accurate if you install debug symbols
with the JVM. I didn't, and the results looked pretty good to me. (2021-02-01)
