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
 * Range query over the plain layout of a ColumNAR string column, isolating the per-interval byte
 * skip index. Unlike {@link ColumnarStringRangeSlicingBenchmark}, this class fixes the format to
 * {@code COLUMNAR_PLAIN} so the skip-index cost is always in the plain code path, and uses data
 * shapes that span the spectrum from maximally skip-friendly to skip-hostile:
 *
 * <ul>
 * <li>{@code SORTED_POD_NAME}: values arrive in term order, each term in one run. The interval
 *     {@code [minTerm, maxTerm]} covers a narrow portion of the vocabulary, so nearly every
 *     interval outside the query range is pruned. This is the best case for the byte skip index.</li>
 * <li>{@code SHUFFLED_POD_NAME}: the same multiset in uniformly random document order. Every
 *     interval spans roughly the full vocabulary, so no interval can be pruned. This is the
 *     worst case, and the direct complement to {@code SORTED_POD_NAME}.</li>
 * <li>{@code TRACE_ID}: all values are distinct hex strings. Intervals cover a modest portion of
 *     the lexicographic space, so some pruning is possible at low selectivities.</li>
 * </ul>
 *
 * <p>Run commands (from the repository root):
 * <pre>
 * # Smoke test: one fork, no warmup, one iteration (finishes in under a minute).
 * ./gradlew :benchmarks:run --args="ColumnarPlainStringRangeSlicingBenchmark -f 1 -wi 0 -i 1 -p numDocs=100000 -rf json -rff columnar-plain-range.json"
 *
 * # Full run: all three data shapes across four selectivity values and two doc counts.
 * ./gradlew :benchmarks:run --args="ColumnarPlainStringRangeSlicingBenchmark -rf json -rff columnar-plain-range.json"
 * </pre>
 */
public class ColumnarPlainStringRangeSlicingBenchmark extends AbstractStringRangeSlicingBenchmark {

    @Param({ "SORTED_POD_NAME", "SHUFFLED_POD_NAME", "TRACE_ID" })
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
        return StringFormat.COLUMNAR_PLAIN;
    }

    @Override
    void validateLayout(StringFormat.Column column) throws IOException {
        if (column.shape().startsWith("plain") == false) {
            throw new IllegalStateException("COLUMNAR_PLAIN produced a non-plain layout: " + column.shape());
        }
    }
}
