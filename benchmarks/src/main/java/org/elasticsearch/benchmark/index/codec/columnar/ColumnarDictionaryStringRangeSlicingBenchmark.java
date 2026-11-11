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
 * Range query over the dictionary layout of a ColumNAR string column, isolating the per-interval
 * ordinal skip index. Unlike {@link ColumnarStringRangeSlicingBenchmark}, this class fixes the
 * format to {@code COLUMNAR_DICTIONARY} so the skip-index cost is always in the ordinal code path,
 * and uses data shapes that span the spectrum from maximally skip-friendly to skip-hostile:
 *
 * <ul>
 * <li>{@code CLUSTERED_POD_NAME}: values arrive in eight region-sorted runs cycling through the
 *     full vocabulary. Within each interval, the ordinal range covers a small slice of the
 *     vocabulary, so most intervals outside the query range are pruned by the ordinal skip index.
 *     This is the best case for the dictionary skip index.</li>
 * <li>{@code POD_NAME}: the same vocabulary with each document drawn uniformly at random. Every
 *     interval covers nearly the full ordinal range, so no interval can be pruned. This is the
 *     worst case, and the direct complement to {@code CLUSTERED_POD_NAME}.</li>
 * </ul>
 *
 * <p>Both shapes draw from a 50 000-term vocabulary and reliably earn a dictionary under the
 * default policy.
 *
 * <p>Run commands (from the repository root):
 * <pre>
 * # Smoke test: one fork, no warmup, one iteration (finishes in under a minute).
 * ./gradlew :benchmarks:run --args="ColumnarDictionaryStringRangeSlicingBenchmark -f 1 -wi 0 -i 1 -p numDocs=100000 -rf json -rff columnar-dict-range.json"
 *
 * # Full run: both data shapes across four selectivity values, two doc counts.
 * ./gradlew :benchmarks:run --args="ColumnarDictionaryStringRangeSlicingBenchmark -rf json -rff columnar-dict-range.json"
 * </pre>
 */
public class ColumnarDictionaryStringRangeSlicingBenchmark extends AbstractStringRangeSlicingBenchmark {

    @Param({ "CLUSTERED_POD_NAME", "POD_NAME" })
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
        return StringFormat.COLUMNAR_DICTIONARY;
    }

    @Override
    void validateLayout(StringFormat.Column column) throws IOException {
        if (column.shape().startsWith("plain")) {
            throw new IllegalStateException(
                "COLUMNAR_DICTIONARY with " + data + " fell back to PLAIN; use a data shape with repeated values"
            );
        }
    }
}
