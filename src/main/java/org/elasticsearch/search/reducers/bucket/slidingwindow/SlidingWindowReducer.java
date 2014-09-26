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

package org.elasticsearch.search.reducers.bucket.slidingwindow;

import org.elasticsearch.search.aggregations.bucket.BucketStreamContext;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.reducers.Reducer;
import org.elasticsearch.search.reducers.ReducerFactories;
import org.elasticsearch.search.reducers.bucket.BucketReducer;
import org.elasticsearch.search.reducers.bucket.InternalBucketReducerAggregation;
import org.elasticsearch.search.reducers.bucket.InternalBucketReducerAggregation.InternalSelection;

import java.util.ArrayList;
import java.util.List;

public class SlidingWindowReducer extends BucketReducer {

    private int windowSize;
    private String path;

    public SlidingWindowReducer(String name, String path, int windowSize, ReducerFactories factories, SearchContext context, Reducer parent) {
        super(name, factories, context, parent);
        this.path = path;
        this.windowSize = windowSize;
    }

    protected InternalBucketReducerAggregation doReduce(MultiBucketsAggregation aggregation) {
        List<InternalSelection> selections = new ArrayList<>();
        List<MultiBucketsAggregation.Bucket> aggBuckets = (List<MultiBucketsAggregation.Bucket>) aggregation.getBuckets();
        for (int i = 0; i <= aggBuckets.size() - windowSize; i++) {
            List<MultiBucketsAggregation.Bucket> selectionBuckets = new ArrayList<>();
            for (int j = 0; j < windowSize; j++) {
                selectionBuckets.add(aggBuckets.get(i + j));
            }
            // NOCOMMIT get bucketType from somewhere
            // NOCOMMIT populate aggregations
            // NOCOMMIT sort out BucketStreamContext from somewhere
            InternalSelection selection = new InternalSelection("Selection " + i, null, new BucketStreamContext(), selectionBuckets , null);
            selections.add(selection);
        }
        return new InternalSlidingWindow(name(), selections);
    }

}
