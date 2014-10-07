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
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.bucket.BucketStreamContext;
import org.elasticsearch.search.aggregations.bucket.BucketStreams;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.reducers.Reducer;
import org.elasticsearch.search.reducers.ReducerFactories;
import org.elasticsearch.search.reducers.ReductionExecutionException;

import java.util.List;

public abstract class BucketReducer extends Reducer {

    private String path;

    public BucketReducer(String name, String path, ReducerFactories factories, SearchContext context, Reducer parent) {
        super(name, factories, context, parent);
        this.path = path;
    }

    @Override
    public InternalBucketReducerAggregation reduce(List<MultiBucketsAggregation> aggregations, SearchContext context)
            throws ReductionExecutionException {
        for (MultiBucketsAggregation aggregation : aggregations) {
            if (aggregation.getName().equals(path)) {
                BytesReference bucketType = ((InternalAggregation) aggregation).type().stream();
                BucketStreamContext bucketStreamContext = BucketStreams.stream(bucketType).getBucketStreamContext(aggregation.getBuckets().get(0)); // NOCOMMIT make this cleaner
                return doReduce(aggregation, bucketType, bucketStreamContext);
            }
        }
        return null; // NOCOMMIT throw exception if we can't find the aggregation
    }

    protected abstract InternalBucketReducerAggregation doReduce(MultiBucketsAggregation aggregation, BytesReference bucketType, BucketStreamContext bucketStreamContext) throws ReductionExecutionException;
}
