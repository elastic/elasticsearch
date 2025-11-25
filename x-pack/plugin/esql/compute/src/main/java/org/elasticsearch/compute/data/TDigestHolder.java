/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.apache.lucene.util.BytesRef;

/**
 * This exists to hold the values from a {@link TDigestBlock}.  It is roughly parallel to
 * {@link org.elasticsearch.search.aggregations.metrics.TDigestState} in classic aggregations, which we are not using directly because
 * the serialization format is pretty bad for ESQL's use case (specifically, encoding the near-constant compression and merge strategy
 * data inline as opposed to in a dedicated column isn't great).
 */
public class TDigestHolder {

    private final double min;
    private final double max;
    private final double sum;
    private final long valueCount;
    private final BytesRef encodedDigest;

    public TDigestHolder(BytesRef encodedDigest, double min, double max, double sum, long valueCount) {
        this.encodedDigest = encodedDigest;
        this.min = min;
        this.max = max;
        this.sum = sum;
        this.valueCount = valueCount;
    }
}
