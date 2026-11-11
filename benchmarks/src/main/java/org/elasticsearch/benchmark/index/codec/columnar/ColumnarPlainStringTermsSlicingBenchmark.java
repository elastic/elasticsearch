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
 * A {@code terms} query over a {@code PLAIN} column, over the data shapes
 * {@link ColumnarPlainStringRangeSlicingBenchmark} ranges over. One of the three arrives in term order, so it
 * is bisected by rank, and the other two are compared value by value.
 *
 * <p>Run with:
 * <pre>
 * ./gradlew :benchmarks:run --args="ColumnarPlainStringTermsSlicingBenchmark -p numDocs=1000000 \
 *     -rf json -rff columnar-plain-terms.json"
 * </pre>
 */
public class ColumnarPlainStringTermsSlicingBenchmark extends AbstractStringTermsSlicingBenchmark {

    @Param({ "SORTED_POD_NAME", "SHUFFLED_POD_NAME", "TRACE_ID" })
    private StringData data;

    /**
     * Resolution costs one bisection per term against the dictionary's single sweep, so the two cross over
     * somewhere near {@code dictionarySize / log2(dictionarySize)}. The top of this range is there to reach
     * that point rather than because a query of this size is common.
     */
    @Param({ "1", "8", "64", "512", "4096" })
    private int queryTerms;

    @Param({ "PRESENT", "ABSENT" })
    private Probe probe;

    @Override
    StringData data() {
        return data;
    }

    @Override
    StringFormat format() {
        return StringFormat.COLUMNAR_PLAIN;
    }

    @Override
    int queryTerms() {
        return queryTerms;
    }

    @Override
    Probe probe() {
        return probe;
    }

    @Override
    void validateLayout(StringFormat.Column column) throws IOException {
        if (column.shape().startsWith("plain") == false) {
            throw new IllegalStateException("COLUMNAR_PLAIN produced a non-plain layout: " + column.shape());
        }
    }
}
