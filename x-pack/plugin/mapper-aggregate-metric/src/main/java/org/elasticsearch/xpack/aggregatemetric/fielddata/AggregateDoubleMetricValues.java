/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.aggregatemetric.fielddata;

import java.io.IOException;

/**
 * Per-segment histogram values.
 */
public abstract class AggregateDoubleMetricValues {

    /**
     * Advance this instance to the given document id
     * @return true if there is a value for this document
     */
    public abstract boolean advanceExact(int doc) throws IOException;

    /**
     * Get the {@link AggregateDoubleMetricValue} associated with the current document.
     * The returned {@link AggregateDoubleMetricValue} might be reused across calls.
     */
    public abstract AggregateDoubleMetricValue metricValue() throws IOException;

}
