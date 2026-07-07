/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.exponentialhistogram.fielddata;

import org.apache.lucene.search.DocIdSetIterator;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.exponentialhistogram.ExponentialHistogram;

import java.io.IOException;

public interface ExponentialHistogramValuesReader {

    /**
     * Returns the histogram value for the current document. Must be called only after a successful call to {@link #advanceExact(int)}.
     * The returned histogram instance may be reused across calls, so if you need to hold on to it, make a copy.
     *
     * @return the histogram value for the current document
     */
    ExponentialHistogram histogramValue() throws IOException;

    /**
     * A shortcut for invoking {@link ExponentialHistogram#valueCount()} on the return value of {@link #histogramValue()}.
     * This method is more performant because it avoids loading the unnecessary parts of the histogram.
     * Must be called only after a successful call to {@link #advanceExact(int)}.
     *
     * @return the count of values in the histogram for the current document
     */
    long valuesCountValue() throws IOException;

    /**
     * A shortcut for invoking {@link ExponentialHistogram#sum()} on the return value of {@link #histogramValue()}.
     * This method is more performant because it avoids loading the unnecessary parts of the histogram.
     * Must be called only after a successful call to {@link #advanceExact(int)}.
     *
     * @return the sum of values in the histogram for the current document
     */
    double sumValue() throws IOException;

    /**
     * A shortcut for invoking {@link ExponentialHistogram#min()} on the return value of {@link #histogramValue()}.
     * This method is more performant because it avoids loading the unnecessary parts of the histogram.
     * Must be called only after a successful call to {@link #advanceExact(int)}.
     * If the histogram is empty, this will return {@code Double.POSITIVE_INFINITY}.
     *
     * @return the minimum of the values in the histogram for the current document
     */
    double minValue() throws IOException;

    /**
     * A shortcut for invoking {@link ExponentialHistogram#max()} on the return value of {@link #histogramValue()}.
     * This method is more performant because it avoids loading the unnecessary parts of the histogram.
     * Must be called only after a successful call to {@link #advanceExact(int)}.
     * If the histogram is empty, this will return {@code Double.NEGATIVE_INFINITY}.
     *
     * @return the maximum of the values in the histogram for the current document
     */
    double maxValue() throws IOException;

    /** Advance the iterator to exactly {@code target} and return whether
     *  {@code target} has a value.
     *  {@code target} must be greater than or equal to the current
     *  doc ID and must be a valid doc ID, ie. &ge; 0 and
     *  &lt; {@code maxDoc}.*/
    boolean advanceExact(int target) throws IOException;

    default int docValueCount() {
        return 1;
    }

    /**
     * @return a doc id iterator over the doc values when available, otherwise null.
     *         Moving the returned iterator also moves the position of the {@link ExponentialHistogramValuesReader} instance.
     */
    @Nullable
    default DocIdSetIterator docIdIterator() {
        return null;
    }

    // TODO: add accessors for min/max/sum which don't load the entire histogram
}
