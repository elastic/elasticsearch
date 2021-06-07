/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.bucket.histogram;

import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;

import java.util.List;

/** Implemented by histogram aggregations and used by pipeline aggregations to insert buckets. */
// public so that pipeline aggs can use this API: can we fix it?
public interface HistogramFactory {

    /** Get the key for the given bucket. Date histograms must return the
     *  number of millis since Epoch of the bucket key while numeric histograms
     *  must return the double value of the key. */
    Number getKey(MultiBucketsAggregation.Bucket bucket);

    /** Given a key returned by {@link #getKey}, compute the lowest key that is
     *  greater than it. */
    Number nextKey(Number key);

    /** Create an {@link InternalAggregation} object that wraps the given buckets. */
    InternalAggregation createAggregation(List<MultiBucketsAggregation.Bucket> buckets);

    /** Create a {@link MultiBucketsAggregation.Bucket} object that wraps the
     *  given key, document count and aggregations. */
    MultiBucketsAggregation.Bucket createBucket(Number key, long docCount, InternalAggregations aggregations);

}
