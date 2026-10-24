/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.index.codec.columnar;

import org.openjdk.jmh.annotations.Param;

import java.io.IOException;

/**
 * Range query over the plain layout of a ColumNAR string column, with the plain skip index off
 * ({@code COLUMNAR_PLAIN}) and on ({@code COLUMNAR_PLAIN_BOUNDED}). Unlike
 * {@link ColumnarStringRangeSlicingBenchmark}, this class keeps the layout plain so the skip-index cost is
 * always in the plain code path, and uses data shapes that span the spectrum from maximally skip-friendly to
 * skip-hostile:
 *
 * <ul>
 * <li>{@code SORTED_POD_NAME}: values arrive in term order, so the reader bisects and does not use the skip
 *     index, which is still written. It is the control: both formats must measure the same.</li>
 * <li>{@code DESCENDING_POD_NAME}: values arrive in reverse term order. The reader cannot bisect, and each
 *     chunk covers a narrow portion of the vocabulary, so nearly every chunk outside the query range is
 *     pruned. This is the best case for the skip index.</li>
 * <li>{@code SHUFFLED_POD_NAME}: the same multiset in uniformly random document order. Every
 *     chunk spans roughly the full vocabulary, so no chunk can be pruned. This is the worst case.</li>
 * <li>{@code TRACE_ID}: all values are distinct hex strings. Chunks cover almost the whole
 *     lexicographic space, so little pruning is possible.</li>
 * </ul>
 *
 * <p>Run commands (from the repository root):
 * <pre>
 * # Smoke test: one fork, no warmup, one iteration (finishes in under a minute).
 * ./gradlew :benchmarks:run --args="ColumnarPlainStringRangeSlicingBenchmark -f 1 -wi 0 -i 1 -p numDocs=100000 -rf json -rff columnar-plain-range.json"
 *
 * # Full run: all four data shapes across four selectivity values, two doc counts and both formats.
 * ./gradlew :benchmarks:run --args="ColumnarPlainStringRangeSlicingBenchmark -rf json -rff columnar-plain-range.json"
 * </pre>
 */
public class ColumnarPlainStringRangeSlicingBenchmark extends AbstractStringRangeSlicingBenchmark {

    @Param({ "COLUMNAR_PLAIN", "COLUMNAR_PLAIN_BOUNDED" })
    private StringFormat format;

    @Param({ "SORTED_POD_NAME", "DESCENDING_POD_NAME", "SHUFFLED_POD_NAME", "TRACE_ID" })
    private StringData data;

    @Param({ "0.0", "0.001", "0.01", "0.1" })
    private double selectivity;

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
        format.requirePlainLayout(column, numDocs);
    }
}
