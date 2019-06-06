/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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
