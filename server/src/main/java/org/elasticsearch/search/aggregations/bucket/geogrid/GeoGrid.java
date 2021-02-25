/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.aggregations.bucket.geogrid;

import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;

import java.util.List;

/**
 * A geo-grid aggregation. Defines multiple buckets, each representing a cell in a geo-grid of a specific
 * precision.
 */
public interface GeoGrid extends MultiBucketsAggregation {

    /**
     * A bucket that is associated with a geo-grid cell. The key of the bucket is
     * the {@link InternalGeoGridBucket#getKeyAsString()} of the cell
     */
    interface Bucket extends MultiBucketsAggregation.Bucket {
    }

    /**
     * @return The buckets of this aggregation (each bucket representing a geo-grid cell)
     */
    @Override
    List<? extends Bucket> getBuckets();
}
