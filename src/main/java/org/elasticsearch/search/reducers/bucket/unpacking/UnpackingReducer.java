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

package org.elasticsearch.search.reducers.bucket.unpacking;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;
import org.elasticsearch.search.reducers.Reducer;
import org.elasticsearch.search.reducers.ReducerContext;
import org.elasticsearch.search.reducers.ReducerFactories;
import org.elasticsearch.search.reducers.ReducerFactory;
import org.elasticsearch.search.reducers.ReducerFactoryStreams;
import org.elasticsearch.search.reducers.ReductionExecutionException;
import org.elasticsearch.search.reducers.bucket.BucketReducer;
import org.elasticsearch.search.reducers.bucket.InternalBucketReducerAggregation;
import org.elasticsearch.search.reducers.bucket.InternalBucketReducerAggregation.InternalSelection;
import org.elasticsearch.search.reducers.bucket.slidingwindow.InternalSlidingWindow;
import org.elasticsearch.search.reducers.bucket.union.InternalUnion;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class UnpackingReducer extends BucketReducer {

    public static final ReducerFactoryStreams.Stream STREAM = new ReducerFactoryStreams.Stream() {
        @Override
        public ReducerFactory readResult(StreamInput in) throws IOException {
            Factory factory = new Factory();
            factory.readFrom(in);
            return factory;
        }
    };

    public static void registerStreams() {
        ReducerFactoryStreams.registerStream(STREAM, InternalUnpacking.TYPE.stream());
    }

    private String unpackPath;
    private List<String> bucketsPaths;

    public UnpackingReducer(String name, List<String> bucketsPaths, String unpackPath, ReducerFactories factories, ReducerContext context,
            Reducer parent) {
        super(name, factories, context, parent);
        this.bucketsPaths = bucketsPaths;
        this.unpackPath = unpackPath;
    }

    @Override
    public InternalBucketReducerAggregation reduce(Aggregations aggregationsTree, Aggregation currentAggregation) {
        List<MultiBucketsAggregation> aggregations = new ArrayList<MultiBucketsAggregation>();
        for (String bucketsPath : bucketsPaths) {
            if (currentAggregation == null) {
                Object o = aggregationsTree.getProperty(bucketsPath);
                if (o instanceof MultiBucketsAggregation) {
                    aggregations.add((MultiBucketsAggregation) o);
                } else {
                    throw new ReductionExecutionException("bucketsPath must resolve to a muti-bucket aggregation for reducer [" + name()
                            + "]");
                }
            } else {
                if (currentAggregation instanceof MultiBucketsAggregation) {
                    aggregations.add((MultiBucketsAggregation) currentAggregation);
                } else {
                    throw new ReductionExecutionException("aggregation must be a muti-bucket aggregation for reducer [" + name() + "]");
                }
            }
        }
        return doReduce(aggregationsTree, aggregations);
    }

    public InternalBucketReducerAggregation doReduce(Aggregations aggregationsTree, List<MultiBucketsAggregation> aggregations)
            throws ReductionExecutionException {
        List<MultiBucketsAggregation.Bucket> buckets = new ArrayList<>();
        BytesReference bucketType = null;
        for (MultiBucketsAggregation aggregation : aggregations) {
            Object[] objArray = (Object[]) aggregation.getProperty(unpackPath);
            if (objArray != null) {
                for (Object o : objArray) {
                    if (o instanceof MultiBucketsAggregation) {
                        MultiBucketsAggregation multiBucketsAggregation = (MultiBucketsAggregation) o;
                        bucketType = checkBucketType(bucketType, multiBucketsAggregation);
                        buckets.addAll(multiBucketsAggregation.getBuckets());
                    } else {
                        throw new ReductionExecutionException("Unpack path must point to a MultiBucketAggregation. Found [" + o.getClass()
                                + "] for unpack path [" + unpackPath + "]");
                    }
                }
            }
        }
        InternalSelection selection = new InternalSelection(unpackPath, bucketType, buckets,
                InternalAggregations.EMPTY);
        InternalAggregations subReducersResults = runSubReducers(aggregationsTree, selection);
        selection.setAggregations(subReducersResults);
        return new InternalUnpacking(name(), Collections.singletonList(selection));
    }

    public static class Factory extends ReducerFactory {

        private List<String> bucketsPaths;
        private String unpackPath;

        public Factory() {
            super(InternalSlidingWindow.TYPE);
        }

        public Factory(String name, List<String> bucketsPaths, String unpackPath) {
            super(name, InternalSlidingWindow.TYPE);
            this.bucketsPaths = bucketsPaths;
            this.unpackPath = unpackPath;
        }

        @Override
        public Reducer create(ReducerContext context, Reducer parent) {
            return new UnpackingReducer(name, bucketsPaths, unpackPath, factories, context, parent);
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            name = in.readString();
            int size = in.readVInt();
            List<String> bucketsPaths = new ArrayList<>(size);
            for (int i = 0; i < size; i++) {
                bucketsPaths.add(in.readString());
            }
            this.bucketsPaths = bucketsPaths;
            unpackPath = in.readString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(name);
            out.writeVInt(bucketsPaths.size());
            for (String path : bucketsPaths) {
                out.writeString(path);
            }
            out.writeString(unpackPath);
        }

    }
}
