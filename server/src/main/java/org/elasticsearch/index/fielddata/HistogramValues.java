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
 * Per-segment histogram values.
 */
public abstract class HistogramValues {

    /**
     * Advance this instance to the given document id
     * @return true if there is a value for this document
     */
    public abstract boolean advanceExact(int doc) throws IOException;

    /**
     * Get the {@link HistogramValue} associated with the current document.
     * The returned {@link HistogramValue} might be reused across calls.
     */
    public abstract HistogramValue histogram() throws IOException;

}
