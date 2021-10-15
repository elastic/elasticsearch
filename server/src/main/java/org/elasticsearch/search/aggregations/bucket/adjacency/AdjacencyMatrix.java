/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.bucket.adjacency;

import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;

import java.util.List;

/**
 * A multi bucket aggregation where the buckets are defined by a set of filters
 * (a bucket is produced per filter plus a bucket for each non-empty filter
 * intersection so A, B and A&amp;B).
 */
public interface AdjacencyMatrix extends MultiBucketsAggregation {

    /**
     * A bucket associated with a specific filter or pair (identified by its
     * key)
     */
    interface Bucket extends MultiBucketsAggregation.Bucket {}

    /**
     * The buckets created by this aggregation.
     */
    @Override
    List<? extends Bucket> getBuckets();

    Bucket getBucketByKey(String key);

}
