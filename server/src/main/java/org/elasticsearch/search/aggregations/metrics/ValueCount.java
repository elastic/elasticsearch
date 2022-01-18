/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.aggregations.metrics;

/**
 * An get that holds the number of <strong>values</strong> that the current document set has for a specific
 * field.
 */
public interface ValueCount extends NumericMetricsAggregation.SingleValue {

    /**
     * @return The count
     */
    long getValue();

}
