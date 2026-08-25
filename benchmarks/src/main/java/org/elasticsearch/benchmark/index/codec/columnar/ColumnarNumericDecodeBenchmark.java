/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.index.codec.columnar;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.store.Directory;
import org.elasticsearch.benchmark.Utils;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * Sequential full-scan decode throughput for a numeric doc-values {@link NumericFormat}. One segment
 * is built once per trial and decoded repeatedly; JMH measures the average time per full pass over all
 * {@code docCount} values. Set {@code format} to compare ColumNAR's decode path against Lucene90 and
 * ES95 on identical data. ColumNAR uses
 * {@link org.elasticsearch.columnar.numeric.ColumnarNumericBinaryDocValues#directValues()}, which
 * decodes block-by-block off disk without a payload round-trip.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Fork(1)
@Threads(1)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
public class ColumnarNumericDecodeBenchmark {

    static {
        Utils.configureBenchmarkLogging();
    }

    private static final String FIELD = NumericFormat.FIELD;

    @Param({ "LUCENE", "ES819", "ES95", "COLUMNAR" })
    private NumericFormat format;

    @Param({ "MONOTONIC_TIMESTAMPS", "COUNTER_STEADY", "GAUGE", "DOUBLE_GAUGE", "DOUBLE_COUNTER", "RANDOM_FULL" })
    private String workload;

    @Param({ "128", "512" })
    private int blockSize;

    @Param("200000")
    private int docCount;

    private Directory directory;
    private DirectoryReader reader;
    private LeafReader leafReader;

    @Setup(Level.Trial)
    public void setup() throws IOException {
        final long[] values = NumericData.generate(workload, docCount);
        directory = format.buildSegment(FIELD, workload, values, "columnar-decode-", blockSize);
        reader = DirectoryReader.open(directory);
        leafReader = reader.leaves().getFirst().reader();
    }

    @TearDown(Level.Trial)
    public void tearDown() throws IOException {
        reader.close();
        directory.close();
    }

    @Benchmark
    public void decode(Blackhole bh) throws IOException {
        format.readAll(leafReader, FIELD, bh);
    }
}
