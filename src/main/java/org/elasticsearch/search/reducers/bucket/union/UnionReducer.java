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

package org.elasticsearch.search.reducers.bucket.union;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.bucket.BucketStreamContext;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation.Bucket;
import org.elasticsearch.search.reducers.Reducer;
import org.elasticsearch.search.reducers.ReducerContext;
import org.elasticsearch.search.reducers.ReducerFactories;
import org.elasticsearch.search.reducers.ReducerFactory;
import org.elasticsearch.search.reducers.ReducerFactoryStreams;
import org.elasticsearch.search.reducers.bucket.BucketReducer;
import org.elasticsearch.search.reducers.bucket.InternalBucketReducerAggregation;
import org.elasticsearch.search.reducers.bucket.InternalBucketReducerAggregation.InternalSelection;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class UnionReducer extends BucketReducer {

    public static final ReducerFactoryStreams.Stream STREAM = new ReducerFactoryStreams.Stream() {
        @Override
        public ReducerFactory readResult(StreamInput in) throws IOException {
            Factory factory = new Factory();
            factory.readFrom(in);
            return factory;
        }
    };

    public static void registerStreams() {
        ReducerFactoryStreams.registerStream(STREAM, InternalUnion.TYPE.stream());
    }

    public UnionReducer(String name, String bucketsPath, ReducerFactories factories, ReducerContext context, Reducer parent) {
        super(name, bucketsPath, factories, context, parent);
    }

    public InternalBucketReducerAggregation doReduce(MultiBucketsAggregation aggregation, BytesReference bucketType, BucketStreamContext bucketStreamContext) {
        Map<String, List<MultiBucketsAggregation.Bucket>> selectionsBucketsMap = new LinkedHashMap<>();
        List<? extends Bucket> aggBuckets = (List<? extends MultiBucketsAggregation.Bucket>) aggregation.getBuckets();
        for (int i = 0; i <= aggBuckets.size() - 1; i++) {
            Bucket aggBucket = aggBuckets.get(i);
            String key = aggBucket.getKey();
            List<MultiBucketsAggregation.Bucket> selectionBuckets = selectionsBucketsMap.get(key);
            if (selectionBuckets == null) {
                selectionBuckets = new ArrayList<>();
            }
            selectionBuckets.add(aggBucket);
        }
        List<InternalSelection> selections = new ArrayList<>();
        for (Entry<String, List<MultiBucketsAggregation.Bucket>> entry : selectionsBucketsMap.entrySet()) {
            String key = entry.getKey();
            List<MultiBucketsAggregation.Bucket> buckets = entry.getValue();
            InternalSelection selection = new InternalSelection(key, bucketType, bucketStreamContext, buckets, InternalAggregations.EMPTY);
            InternalAggregations subReducersResults = runSubReducers(selection);
            selection.setAggregations(subReducersResults);
            selections.add(selection);
        }
        // NOCOMMIT do we need to add sorting here? at the moment the selections
        // will be in discovery order
        return new InternalUnion(name(), selections);
    }

    public static class Factory extends ReducerFactory {

        private String bucketsPath;

        public Factory() {
            super(InternalUnion.TYPE);
        }

        public Factory(String name, String bucketsPath) {
            super(name, InternalUnion.TYPE);
            this.bucketsPath = bucketsPath;
        }

        @Override
        public Reducer create(ReducerContext context, Reducer parent) {
            return new UnionReducer(name, bucketsPath, factories, context, parent);
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            name = in.readString();
            bucketsPath = in.readString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(name);
            out.writeString(bucketsPath);
        }
        
    }

}
