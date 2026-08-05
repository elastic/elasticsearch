/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.index.codec.tsdb;

import org.apache.lucene.codecs.DocValuesFormat;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.codec.tsdb.es819.ES819TSDBDocValuesFormatFactory;
import org.elasticsearch.index.codec.tsdb.es819.ES819Version3TSDBDocValuesFormat;
import org.elasticsearch.index.codec.tsdb.es95.ES95TSDBDocValuesFormatFactory;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.util.concurrent.TimeUnit;

/**
 * Compares the cost of creating an {@code ES819} vs an {@code ES95} TSDB doc values format.
 *
 * <p>The ES819 cached/uncached pair shows both the cache lookup cost and the cold
 * construction cost of the previous codec. ES95 has no factory level cache because every
 * production caller supplies a per index {@link
 * org.elasticsearch.index.codec.tsdb.pipeline.FieldContextResolver} that cannot be
 * globally cached; the ES95 row measures the cold construction cost production pays once
 * per supplier.
 *
 * <p>Run with {@code -prof gc} to read {@code gc.alloc.rate.norm} (bytes per op), which is
 * the direct measure of the per call allocation. The same run also reports throughput.
 *
 * <h2>Ready to run command</h2>
 *
 * <pre>{@code
 * ./gradlew :benchmarks:run --args="TSDBDocValuesFormatCreationBenchmark \
 *   -f 3 -wi 5 -w 2 -i 10 -r 2 -prof gc"
 * }</pre>
 */
@Fork(1)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 2)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Benchmark)
public class TSDBDocValuesFormatCreationBenchmark {

    private IndexVersion currentVersion;

    @Setup(Level.Trial)
    public void setupTrial() {
        currentVersion = IndexVersion.current();
    }

    @Benchmark
    public DocValuesFormat createES819Cached() {
        return ES819TSDBDocValuesFormatFactory.createDocValuesFormat(currentVersion, false, false, false);
    }

    @Benchmark
    public DocValuesFormat createES819Uncached() {
        return new ES819Version3TSDBDocValuesFormat(false, false, false);
    }

    @Benchmark
    public DocValuesFormat createES95() {
        return ES95TSDBDocValuesFormatFactory.create(false, false, false, null);
    }
}
