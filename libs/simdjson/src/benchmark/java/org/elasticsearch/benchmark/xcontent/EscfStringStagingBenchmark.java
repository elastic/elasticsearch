/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.xcontent;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.benchmark.internal.BenchmarkLogging;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.escf.EscfBatch;
import org.elasticsearch.escf.EscfBatchBuilder;
import org.elasticsearch.escf.EscfRowBuffer;
import org.elasticsearch.transport.BytesRefRecycler;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Stages rows of string fields into an {@link EscfBatchBuilder} and commits them, with no parsing, so that the measured
 * time is dominated by row staging, the drain into column builders and the string writes into column buffers.
 *
 * <p>Values are slices of one shared array, as string values are slices of a document. With {@code mixedTypes}, every
 * other value of a column is a long, so the string writes go to union columns instead of string columns.
 */
@Fork(value = 1, jvmArgsAppend = { "--add-modules=jdk.incubator.vector" })
@Warmup(iterations = 3, time = 5)
@Measurement(iterations = 3, time = 5)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Thread)
public class EscfStringStagingBenchmark {

    /** Bytes per string value: the per-value overhead dominates the small sizes, the copy dominates the large one. */
    @Param({ "8", "64", "512" })
    private int valueLength;

    @Param({ "40" })
    private int fieldsPerRow;

    @Param({ "1000" })
    private int rowCount;

    @Param({ "5" })
    private int partitionCount;

    @Param({ "false", "true" })
    private boolean mixedTypes;

    private static final Recycler<BytesRef> RECYCLER = new BytesRefRecycler(new PageCacheRecycler(Settings.EMPTY));

    private String[] fieldNames;
    private byte[] values;

    @Setup
    public void setUp() {
        BenchmarkLogging.configure();
        fieldNames = new String[fieldsPerRow];
        for (int f = 0; f < fieldsPerRow; f++) {
            fieldNames[f] = "field_" + f;
        }
        Random random = new Random(42);
        values = new byte[rowCount * fieldsPerRow * valueLength];
        for (int i = 0; i < values.length; i++) {
            values[i] = (byte) ('a' + random.nextInt(26));
        }
    }

    @Benchmark
    public int stageAndCommit() {
        try (EscfBatchBuilder builder = new EscfBatchBuilder(RECYCLER)) {
            int offset = 0;
            for (int r = 0; r < rowCount; r++) {
                EscfRowBuffer row = builder.beginRow();
                int rowInPartition = r / partitionCount;
                for (int f = 0; f < fieldsPerRow; f++) {
                    if (mixedTypes && ((rowInPartition + f) & 1) == 1) {
                        row.longField(fieldNames[f], r);
                    } else {
                        row.stringField(fieldNames[f], values, offset, valueLength);
                    }
                    offset += valueLength;
                }
                row.finishRow();
                builder.commit(r % partitionCount);
            }
            int docs = 0;
            for (int p = 0; p < partitionCount; p++) {
                try (EscfBatch batch = builder.buildPartition(p)) {
                    docs += batch.docCount();
                }
            }
            return docs;
        }
    }
}
