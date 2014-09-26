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

import org.elasticsearch.search.aggregations.InternalAggregation.ReduceContext;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.reducers.Reducer;
import org.elasticsearch.search.reducers.ReducerFactories;
import org.elasticsearch.search.reducers.ReductionExecutionException;

import java.util.List;

public abstract class BucketReducer extends Reducer {

    public BucketReducer(String name, ReducerFactories factories, SearchContext context, Reducer parent) {
        super(name, factories, context, parent);
    }

    @Override
    public InternalBucketReducerAggregation reduce(List<MultiBucketsAggregation> aggregations, ReduceContext reduceContext)
            throws ReductionExecutionException {
        for (MultiBucketsAggregation aggregation : aggregations) {
            if (aggregation.getName().equals("path")) {
                return doReduce(aggregation);
            }
        }
        return null;
    }

    protected abstract InternalBucketReducerAggregation doReduce(MultiBucketsAggregation aggregation) throws ReductionExecutionException;
}
