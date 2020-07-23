/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.analytics.mapper.fielddata;


import org.elasticsearch.search.aggregations.metrics.AbstractHyperLogLog;

/**
 * Per-document Hll value. represented as a RunLen iterator.
 */
public abstract class HllValue implements AbstractHyperLogLog.RunLenIterator {

    /**
     * Skips over and discards n bytes of data from this HLL value.
     * @param registers the number of registers to skip
     */
    public abstract void skip(int registers);

}
