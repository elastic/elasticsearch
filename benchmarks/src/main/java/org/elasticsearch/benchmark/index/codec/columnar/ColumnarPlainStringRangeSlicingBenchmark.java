/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.index.codec.columnar;

import org.apache.lucene.util.BytesRef;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.infra.Blackhole;

import java.io.IOException;

/**
 * Range, term and prefix queries over the plain layout of a ColumNAR string column, with the plain skip index
 * off ({@code COLUMNAR_PLAIN}) and on ({@code COLUMNAR_PLAIN_BOUNDED}). Unlike
 * {@link ColumnarStringRangeSlicingBenchmark}, this class keeps the layout plain so the skip-index cost is
 * always in the plain code path. The data shapes cover each way a column can be ordered:
 *
 * <ul>
 * <li>{@code SORTED_POD_NAME}, {@code SORTED_HOSTNAME}: values arrive in term order. The reader bisects the
 *     values, and with the skip index it confines each end of the match to one chunk. A pod name is nearly
 *     unique, a host name repeats in long runs.</li>
 * <li>{@code DESCENDING_POD_NAME}, {@code DESCENDING_HOSTNAME}: values arrive in reverse term order. The reader
 *     cannot bisect, and each chunk covers a narrow portion of the vocabulary, so the skip index prunes the
 *     chunks outside the range and accepts the chunks wholly inside it.</li>
 * <li>{@code CLUSTERED_POD_NAME}: values arrive in runs that restart, so the column is not ordered and has no
 *     skip index. It is a control: both formats must measure the same.</li>
 * <li>{@code SHUFFLED_POD_NAME}: the same multiset in uniformly random document order. Every chunk spans
 *     roughly the full vocabulary. It is a control: both formats must measure the same.</li>
 * <li>{@code TRACE_ID}: all values are distinct hex strings, in no order. It is a control: both formats must
 *     measure the same.</li>
 * </ul>
 *
 * <p>The term query looks up the lower bound of the range, which exists in the column except at selectivity
 * {@code 0.0}, where it is a value just past one that does. The prefix query uses that bound without its last
 * three bytes, which matches a small run of values.
 *
 * <p>Run commands (from the repository root):
 * <pre>
 * # Smoke test: one fork, no warmup, one iteration (finishes in under a minute).
 * ./gradlew :benchmarks:run --args="ColumnarPlainStringRangeSlicingBenchmark -f 1 -wi 0 -i 1 -p numDocs=100000 -rf json -rff columnar-plain-range.json"
 *
 * # Full run: all data shapes across four selectivity values, two doc counts and both formats.
 * ./gradlew :benchmarks:run --args="ColumnarPlainStringRangeSlicingBenchmark -rf json -rff columnar-plain-range.json"
 * </pre>
 */
public class ColumnarPlainStringRangeSlicingBenchmark extends AbstractStringRangeSlicingBenchmark {

    @Param({ "COLUMNAR_PLAIN", "COLUMNAR_PLAIN_BOUNDED" })
    private StringFormat format;

    @Param(
        {
            "SORTED_POD_NAME",
            "DESCENDING_POD_NAME",
            "SORTED_HOSTNAME",
            "DESCENDING_HOSTNAME",
            "CLUSTERED_POD_NAME",
            "SHUFFLED_POD_NAME",
            "TRACE_ID" }
    )
    private StringData data;

    @Param({ "0.0", "0.001", "0.01", "0.1" })
    private double selectivity;

    private BytesRef term;
    private BytesRef prefix;

    @Setup(Level.Trial)
    public void setupPointQueries() {
        term = lower;
        prefix = new BytesRef(lower.bytes, lower.offset, Math.max(1, lower.length - 3));
    }

    @Benchmark
    public void termQuery(Blackhole bh) throws IOException {
        bh.consume(column.queryTerm(term));
    }

    @Benchmark
    public void prefixQuery(Blackhole bh) throws IOException {
        bh.consume(column.queryPrefix(prefix));
    }

    @Override
    StringData data() {
        return data;
    }

    @Override
    double selectivity() {
        return selectivity;
    }

    @Override
    double rangeStart() {
        return 0.5;
    }

    @Override
    StringFormat format() {
        return format;
    }

    @Override
    void validateLayout(StringFormat.Column column) throws IOException {
        format.requirePlainLayout(column, numDocs, data.ordered());
    }
}
