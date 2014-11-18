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

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation.Bucket;
import org.elasticsearch.search.reducers.Reducer;
import org.elasticsearch.search.reducers.ReducerContext;
import org.elasticsearch.search.reducers.ReducerFactories;
import org.elasticsearch.search.reducers.ReducerFactory;
import org.elasticsearch.search.reducers.ReducerFactoryStreams;
import org.elasticsearch.search.reducers.ReductionExecutionException;
import org.elasticsearch.search.reducers.bucket.BucketReducer;
import org.elasticsearch.search.reducers.bucket.InternalBucketReducerAggregation;
import org.elasticsearch.search.reducers.bucket.InternalBucketReducerAggregation.InternalSelection;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class SlidingWindowReducer extends BucketReducer {

    public static final ReducerFactoryStreams.Stream STREAM = new ReducerFactoryStreams.Stream() {
        @Override
        public ReducerFactory readResult(StreamInput in) throws IOException {
            Factory factory = new Factory();
            factory.readFrom(in);
            return factory;
        }
    };

    public static void registerStreams() {
        ReducerFactoryStreams.registerStream(STREAM, InternalSlidingWindow.TYPE.stream());
    }

    private int windowSize;

    public SlidingWindowReducer(String name, String bucketsPath, int windowSize, ReducerFactories factories, ReducerContext context, Reducer parent) {
        super(name, bucketsPath, factories, context, parent);
        this.windowSize = windowSize;
    }

    @Override
    public InternalBucketReducerAggregation doReduce(List<? extends Aggregation> aggregations) throws ReductionExecutionException {
        MultiBucketsAggregation aggregation = (MultiBucketsAggregation) ensureSingleAggregation(aggregations);
        List<InternalSelection> selections = new ArrayList<>();
        List<? extends Bucket> aggBuckets = (List<? extends MultiBucketsAggregation.Bucket>) aggregation.getBuckets();
        for (int i = 0; i <= aggBuckets.size() - windowSize; i++) {
            List<MultiBucketsAggregation.Bucket> selectionBuckets = new ArrayList<>();
            for (int j = 0; j < windowSize; j++) {
                selectionBuckets.add(aggBuckets.get(i + j));
            }
            String selectionKey = null;
            if (selectionBuckets.size() == 1) {
                selectionKey = selectionBuckets.get(0).getKey();
            } else {
                selectionKey = selectionBuckets.get(0).getKey() + " TO " + selectionBuckets.get(selectionBuckets.size() - 1).getKey();
            }
            InternalSelection selection = new InternalSelection(selectionKey, ((InternalAggregation) aggregation).type().stream(),
                    selectionBuckets, InternalAggregations.EMPTY);
            InternalAggregations subReducersResults = runSubReducers(selection);
            selection.setAggregations(subReducersResults);
            selections.add(selection);
        }
        return new InternalSlidingWindow(name(), selections);
    }

    public static class Factory extends ReducerFactory {

        private String bucketsPath;
        private int windowSize;

        public Factory() {
            super(InternalSlidingWindow.TYPE);
        }

        public Factory(String name, String bucketsPath, int windowSize) {
            super(name, InternalSlidingWindow.TYPE);
            this.bucketsPath = bucketsPath;
            this.windowSize = windowSize;
        }

        @Override
        public Reducer create(ReducerContext context, Reducer parent) {
            return new SlidingWindowReducer(name, bucketsPath, windowSize, factories, context, parent);
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            name = in.readString();
            bucketsPath = in.readString();
            windowSize = in.readInt();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(name);
            out.writeString(bucketsPath);
            out.writeInt(windowSize);
        }
        
    }

}
