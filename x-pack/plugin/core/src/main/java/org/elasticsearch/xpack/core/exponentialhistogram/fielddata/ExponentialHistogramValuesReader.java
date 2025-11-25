/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.exponentialhistogram.fielddata;

import org.elasticsearch.exponentialhistogram.ExponentialHistogram;

import java.io.IOException;

public interface ExponentialHistogramValuesReader {

    /**
     * Advances to the exact document id, returning true if the document has a value for this field.
     * @param docId the document id
     * @return true if the document has a value for this field, false otherwise
     */
    boolean advanceExact(int docId) throws IOException;

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

    // TODO: add accessors for min/max/sum which don't load the entire histogram
}
