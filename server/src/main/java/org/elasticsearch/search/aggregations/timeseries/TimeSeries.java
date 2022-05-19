/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.timeseries;

import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;

import java.util.List;

public interface TimeSeries extends MultiBucketsAggregation {

    /**
     * A bucket associated with a specific time series (identified by its key)
     */
    interface Bucket extends MultiBucketsAggregation.Bucket {}

    /**
     * The buckets created by this aggregation.
     */
    @Override
    List<? extends TimeSeries.Bucket> getBuckets();

    TimeSeries.Bucket getBucketByKey(String key);
}
