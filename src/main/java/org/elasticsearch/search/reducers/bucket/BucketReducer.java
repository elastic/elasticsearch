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

package org.elasticsearch.search.reducers.bucket;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.bucket.BucketStreamContext;
import org.elasticsearch.search.aggregations.bucket.BucketStreams;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;
import org.elasticsearch.search.reducers.Reducer;
import org.elasticsearch.search.reducers.ReducerContext;
import org.elasticsearch.search.reducers.ReducerFactories;
import org.elasticsearch.search.reducers.ReductionExecutionException;

public abstract class BucketReducer extends Reducer {

    private String bucketsPath;

    public BucketReducer(String name, String bucketsPath, ReducerFactories factories, ReducerContext context, Reducer parent) {
        super(name, factories, context, parent);
        this.bucketsPath = bucketsPath;
    }

    @Override
    public InternalBucketReducerAggregation reduce(InternalAggregations aggregations)
            throws ReductionExecutionException {
        for (Aggregation aggregation : aggregations) {
            if (aggregation.getName().equals(bucketsPath)) {
                if (aggregation instanceof MultiBucketsAggregation) {
                    MultiBucketsAggregation multiBucketsAggregation = (MultiBucketsAggregation) aggregation;
                    BytesReference bucketType = ((InternalAggregation) aggregation).type().stream();
                    BucketStreamContext bucketStreamContext = BucketStreams.stream(bucketType).getBucketStreamContext(multiBucketsAggregation.getBuckets().get(0)); // NOCOMMIT make this cleaner
                    return doReduce(multiBucketsAggregation, bucketType, bucketStreamContext);
                    
                } else {
                    // NOCOMMIT throw exception as path must match a MultiBucketAggregation
                }
            }
        }
        return null; // NOCOMMIT throw exception if we can't find the aggregation
    }

    protected abstract InternalBucketReducerAggregation doReduce(MultiBucketsAggregation aggregation, BytesReference bucketType, BucketStreamContext bucketStreamContext) throws ReductionExecutionException;
}
