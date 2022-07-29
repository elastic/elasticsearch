/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.fielddata;

import java.io.IOException;

/**
 * Per-document histogram value. Every value of the histogram consist on
 * a value and a count.
 */
public abstract class HistogramValue {

    /**
     * Advance this instance to the next value of the histogram
     * @return true if there is a next value
     */
    public abstract boolean next() throws IOException;

    /**
     * the current value of the histogram
     * @return the current value of the histogram
     */
    public abstract double value();

    /**
     * The current count of the histogram
     * @return the current count of the histogram
     */
    public abstract int count();

}
