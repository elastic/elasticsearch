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
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.reducers.Reducer;
import org.elasticsearch.search.reducers.ReducerContext;
import org.elasticsearch.search.reducers.ReducerFactories;
import org.elasticsearch.search.reducers.ReductionExecutionException;
import org.elasticsearch.search.reducers.bucket.InternalBucketReducerAggregation.InternalSelection;

import java.util.ArrayList;
import java.util.List;

public abstract class BucketReducer extends Reducer {

    public BucketReducer(String name, ReducerFactories factories, ReducerContext context, Reducer parent) {
        super(name, factories, context, parent);
    }

    @Override
    public abstract InternalBucketReducerAggregation reduce(Aggregations aggregationTree, Aggregation currentAggregation)
            throws ReductionExecutionException;

    protected InternalAggregations runSubReducers(Aggregations aggregationTree, InternalSelection selection) {
        Reducer[] subReducers = subReducers();
        List<InternalAggregation> aggregations = new ArrayList<>(subReducers.length);
        for (Reducer reducer : subReducers) {
            aggregations.add(reducer.reduce(aggregationTree, selection));
        }
        return new InternalAggregations(aggregations);
    }

    protected BytesReference checkBucketType(BytesReference expectedBucketType, Aggregation aggregation) {
        BytesReference thisBucketType;
        if (aggregation instanceof InternalAggregation) {
            thisBucketType = ((InternalAggregation) aggregation).type().stream();
        } else if (aggregation instanceof InternalSelection) {
            thisBucketType = ((InternalSelection) aggregation).getBucketType();
        } else {
            throw new ReductionExecutionException("aggregation must extend either InternalAggregation or InternalSelection");
        }
        if (expectedBucketType == null) {
            expectedBucketType = thisBucketType;
        } else if (!expectedBucketType.toUtf8().equals(thisBucketType.toUtf8())) {
            throw new ReductionExecutionException("Buckets must all be the same type. Expected: [" + thisBucketType + "], found: ["
                    + thisBucketType + "] for reducer [" + name() + "]");
        }
        return expectedBucketType;
    }
}
