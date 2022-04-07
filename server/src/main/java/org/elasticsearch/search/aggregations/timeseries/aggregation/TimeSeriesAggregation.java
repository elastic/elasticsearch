/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.timeseries.aggregation;

import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;

import java.util.List;

public interface TimeSeriesAggregation extends MultiBucketsAggregation {

    /**
     * A bucket associated with a specific time series (identified by its key)
     */
    interface Bucket extends MultiBucketsAggregation.Bucket {}

    /**
     * The buckets created by this aggregation.
     */
    @Override
    List<? extends Bucket> getBuckets();

    Bucket getBucketByKey(String key);

    class Downsample {
        long range;
        Function function;
    }

    enum Function {
        count,
        sum,
        min,
        max,
        avg,
        last;

        public static Function resolve(String name) {
            return Function.valueOf(name);
        }
    }

    enum Aggregator {
        sum,
        min,
        max,
        avg;

        public static Aggregator resolve(String name) {
            return Aggregator.valueOf(name);
        }
    }
}
