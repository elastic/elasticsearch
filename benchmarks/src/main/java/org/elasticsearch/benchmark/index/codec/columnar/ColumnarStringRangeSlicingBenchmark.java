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
 * A keyword range query over a single force-merged segment, comparing ColumNAR against ES819 and Lucene
 * sorted doc values. The primary axis is doc-order clustering: {@code CLUSTERED_POD_NAME} clusters similar
 * values near each other; {@code SHUFFLED_POD_NAME} uses the same multiset in random document order,
 * isolating clustering benefit from cardinality and value length.
 *
 * <p>Format {@code COLUMNAR} takes whatever layout the data earns under the default dictionary policy;
 * {@code COLUMNAR_PLAIN} and {@code COLUMNAR_DICTIONARY} force one layout each, so each code path is
 * measurable on data that would not normally earn it.
 *
 * <p>Selectivity 0.001 and 0.01 are the selective-filter cases most likely to benefit from a skip index;
 * 0.1, 0.25, and 0.5 show what happens when selectivity is too high for intervals to be pruned effectively.
 * Selectivity 0.0 represents an empty result with maximum pruning opportunity (near-zero cost for sorted
 * columns, full-scan cost for unsorted columns without a useful skip index).
 *
 * <p>The {@code rangeStart} parameter controls where in the sorted vocabulary the range begins,
 * expressed as a fraction of the sorted value sequence. At 0.25, most values lie above the range
 * and upper-bound pruning dominates. At 0.75, most values lie below and lower-bound pruning
 * dominates. At 0.50 (the midpoint), equal halves are on each side.
 *
 * <p>See also {@link ColumnarPlainStringRangeSlicingBenchmark} and
 * {@link ColumnarDictionaryStringRangeSlicingBenchmark}, which isolate the plain and dictionary
 * layouts respectively across data shapes that target each layout's skip index.
 *
 * <p>Run commands (from the repository root):
 * <pre>
 * # Smoke test: one fork, no warmup, one iteration, 100k docs only (finishes in under a minute).
 * ./gradlew :benchmarks:run --args="ColumnarStringRangeSlicingBenchmark -f 1 -wi 0 -i 1 -p numDocs=100000 -p rangeStart=0.50 -rf json -rff columnar-string-range.json"
 *
 * # Full run with all default param combinations.
 * ./gradlew :benchmarks:run --args="ColumnarStringRangeSlicingBenchmark -rf json -rff columnar-string-range.json"
 *
 * # Targeted run: clustering axis only, two selective filter cases, 1M docs, midpoint range.
 * ./gradlew :benchmarks:run --args="ColumnarStringRangeSlicingBenchmark -p format=COLUMNAR,ES819_SORTED -p data=CLUSTERED_POD_NAME,SHUFFLED_POD_NAME -p numDocs=1000000 -p selectivity=0.001,0.01 -p rangeStart=0.50 -rf json -rff columnar-string-range.json"
 * </pre>
 */
public class ColumnarStringRangeSlicingBenchmark extends AbstractStringRangeSlicingBenchmark {

    @Param({ "COLUMNAR", "COLUMNAR_PLAIN", "COLUMNAR_DICTIONARY", "ES819_SORTED", "ES819_BINARY" })
    private StringFormat format;

    @Param({ "CLUSTERED_POD_NAME", "SHUFFLED_POD_NAME", "SORTED_POD_NAME", "TRACE_ID" })
    private StringData data;

    @Param({ "0.0", "0.001", "0.01", "0.1", "0.25", "0.5" })
    private double selectivity;

    @Param({ "0.25", "0.50", "0.75" })
    private double rangeStart;

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
        return rangeStart;
    }

    @Override
    StringFormat format() {
        return format;
    }

    @Override
    void validateLayout(StringFormat.Column column) throws IOException {
        if (format == StringFormat.COLUMNAR_DICTIONARY && column.shape().startsWith("plain")) {
            throw new IllegalStateException(
                "COLUMNAR_DICTIONARY with " + data + " fell back to PLAIN; use a data shape with repeated values"
            );
        }
    }
}
