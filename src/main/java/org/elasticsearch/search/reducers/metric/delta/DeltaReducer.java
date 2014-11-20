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

package org.elasticsearch.search.reducers.metric.delta;

import org.elasticsearch.search.aggregations.Aggregations;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;
import org.elasticsearch.search.reducers.Reducer;
import org.elasticsearch.search.reducers.ReducerContext;
import org.elasticsearch.search.reducers.ReducerFactories;
import org.elasticsearch.search.reducers.ReducerFactory;
import org.elasticsearch.search.reducers.ReducerFactoryStreams;
import org.elasticsearch.search.reducers.ReductionExecutionException;

import java.io.IOException;
import java.util.List;

public class DeltaReducer extends Reducer {

    public static final ReducerFactoryStreams.Stream STREAM = new ReducerFactoryStreams.Stream() {
        @Override
        public ReducerFactory readResult(StreamInput in) throws IOException {
            Factory factory = new Factory();
            factory.readFrom(in);
            return factory;
        }
    };
    private String fieldName;
    private boolean gradient;
    private String bucketsPath;

    public static void registerStreams() {
        ReducerFactoryStreams.registerStream(STREAM, InternalDelta.TYPE.stream());
    }

    public DeltaReducer(String name, String bucketsPath, String fieldName, boolean gradient, ReducerFactories factories, ReducerContext context, Reducer parent) {
        super(name, factories, context, parent);
        this.bucketsPath = bucketsPath;
        this.fieldName = fieldName;
        this.gradient = gradient;
    }

    @Override
    public InternalAggregation reduce(Aggregations aggregationsTree, Aggregation currentAggregation) {
        MultiBucketsAggregation aggregation;
        if (currentAggregation == null) {
            Object o = aggregationsTree.getProperty(bucketsPath);
            if (o instanceof MultiBucketsAggregation) {
               aggregation = (MultiBucketsAggregation) o;
            } else {
                throw new ReductionExecutionException("bucketsPath must point to an instance of MultiBucketAggregation"); // NOCOMMIT make this message user friendly
            }
        } else {
            if (currentAggregation instanceof MultiBucketsAggregation) {
                aggregation = (MultiBucketsAggregation) currentAggregation;
            } else {
                throw new ReductionExecutionException("aggregation must be an instance of MultiBucketAggregation"); // NOCOMMIT make this message user friendly
            }
        }
        return doReduce(aggregationsTree, aggregation);
    }

    public InternalAggregation doReduce(Aggregations aggregationsTree, Aggregation aggregation) throws ReductionExecutionException {
        Object[] bucketProperties = (Object[]) aggregation.getProperty(fieldName);
        double firstBucketValue = (double) bucketProperties[0];
        double lastBucketValue = (double) bucketProperties[bucketProperties.length - 1];
        double deltaValue = lastBucketValue - firstBucketValue;
        if (this.gradient) {
            deltaValue = deltaValue / (bucketProperties.length - 1);
        }
        return new InternalDelta(name(), deltaValue);
    }

    public static class Factory extends ReducerFactory {

        private String bucketsPath;
        private String fieldName;
        private boolean showGradient;

        public Factory() {
            super(InternalDelta.TYPE);
        }

        public Factory(String name, String bucketsPath, String fieldName, boolean showGradient) {
            super(name, InternalDelta.TYPE);
            this.fieldName = fieldName;
            this.showGradient = showGradient;
            this.bucketsPath = bucketsPath;
        }

        @Override
        public Reducer create(ReducerContext context, Reducer parent) {
            return new DeltaReducer(name, bucketsPath, fieldName, showGradient, factories, context, parent);
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            name = in.readString();
            bucketsPath = in.readString();
            showGradient = in.readBoolean();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(name);
            out.writeString(bucketsPath);
            out.writeBoolean(showGradient);
        }
        
    }

}
