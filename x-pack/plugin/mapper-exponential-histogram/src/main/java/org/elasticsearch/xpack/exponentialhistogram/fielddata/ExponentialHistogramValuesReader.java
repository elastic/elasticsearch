/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.exponentialhistogram.fielddata;

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

    // TODO: add accessors for min/max/sum/count which don't load the entire histogram
}
