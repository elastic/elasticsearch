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
 * A {@code terms} query over a {@code DICTIONARY} column, over the data shapes
 * {@link ColumnarDictionaryStringRangeSlicingBenchmark} ranges over. Each query term is resolved to an ordinal
 * by bisecting the dictionary, and the slots are then decided over ordinals.
 *
 * <p>Run with:
 * <pre>
 * ./gradlew :benchmarks:run --args="ColumnarDictionaryStringTermsSlicingBenchmark -p numDocs=1000000 \
 *     -rf json -rff columnar-dictionary-terms.json"
 * </pre>
 */
public class ColumnarDictionaryStringTermsSlicingBenchmark extends AbstractStringTermsSlicingBenchmark {

    @Param({ "CLUSTERED_POD_NAME", "POD_NAME" })
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
        return StringFormat.COLUMNAR_DICTIONARY;
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
        if (column.shape().startsWith("plain")) {
            throw new IllegalStateException(
                "COLUMNAR_DICTIONARY with " + data + " fell back to PLAIN; use a data shape with repeated values"
            );
        }
    }
}
