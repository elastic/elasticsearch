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

package org.elasticsearch.search.reducers.metric.avg;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.bucket.BucketStreamContext;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;
import org.elasticsearch.search.reducers.*;

import java.io.IOException;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

public abstract class AvgReducer extends Reducer {

    public enum AvgType {
        SIMPLE, LINEAR, EXPONENTIAL
    }

    public static final ReducerFactoryStreams.Stream STREAM = new ReducerFactoryStreams.Stream() {
        @Override
        public ReducerFactory readResult(StreamInput in) throws IOException {
            Factory factory = new Factory();
            factory.readFrom(in);
            return factory;
        }
    };
    protected String fieldName;

    public static void registerStreams() {
        ReducerFactoryStreams.registerStream(STREAM, InternalAvg.TYPE.stream());
    }

    public AvgReducer(String name, String bucketsPath, String fieldName, ReducerFactories factories, ReducerContext context, Reducer parent) {
        super(name, bucketsPath, factories, context, parent);
        this.fieldName = fieldName;
    }

    protected Object[] getProperties(MultiBucketsAggregation aggregation) {
        Queue<String> path = new LinkedBlockingQueue<>(1); // NOCOMMIT using a queue here is clunky. Need to use a better datastructure for paths
        path.offer(fieldName);
        return (Object[]) aggregation.getProperty(path);
    }




    public static class SimpleAvgReducer extends AvgReducer {
        public SimpleAvgReducer(String name, String bucketsPath, String fieldName, ReducerFactories factories, ReducerContext context, Reducer parent) {
            super(name, bucketsPath, fieldName, factories, context, parent);
        }

        @Override
        public InternalAggregation doReduce(MultiBucketsAggregation aggregation, BytesReference bucketType,
                                            BucketStreamContext bucketStreamContext) throws ReductionExecutionException {

            Object[] bucketProperties = this.getProperties(aggregation);

            double avg = 0;
            for (Object bucketValue : bucketProperties) {
                avg += (double) bucketValue;
            }

            return new InternalAvg(name(), avg, bucketProperties.length);
        }
    }

    public static class LinearWeightedAvgReducer extends AvgReducer {
        public LinearWeightedAvgReducer(String name, String bucketsPath, String fieldName, ReducerFactories factories, ReducerContext context, Reducer parent) {
            super(name, bucketsPath, fieldName, factories, context, parent);
        }

        @Override
        public InternalAggregation doReduce(MultiBucketsAggregation aggregation, BytesReference bucketType,
                                            BucketStreamContext bucketStreamContext) throws ReductionExecutionException {

            Object[] bucketProperties = this.getProperties(aggregation);

            long current = 1;
            long totalWeight = 1;

            double avg = 0;
            for (Object bucketValue : bucketProperties) {
                avg += (double) bucketValue * current;
                totalWeight += current;
                current += 1;
            }

            return new InternalAvg(name(), avg, totalWeight);
        }
    }

    public static class ExpWeightedAvgReducer extends AvgReducer {
        public ExpWeightedAvgReducer(String name, String bucketsPath, String fieldName, ReducerFactories factories, ReducerContext context, Reducer parent) {
            super(name, bucketsPath, fieldName, factories, context, parent);
        }

        @Override
        public InternalAggregation doReduce(MultiBucketsAggregation aggregation, BytesReference bucketType,
                                            BucketStreamContext bucketStreamContext) throws ReductionExecutionException {

            Object[] bucketProperties = this.getProperties(aggregation);

            double alpha = 2.0 / (double) (bucketProperties.length + 1);  //TODO expose alpha or period as a param to the user

            double ema = 0;
            boolean first = true;
            for (Object bucketValue : bucketProperties) {
                if (first) {
                    ema = (double) bucketValue;
                    first = false;
                } else {
                    ema = ((double) bucketValue * alpha) + (ema * (1 - alpha));
                }

            }

            return new InternalAvg(name(), ema, 1);
        }
    }



    public static class Factory extends ReducerFactory {

        private String bucketsPath;
        private String fieldName;
        private AvgType avgType = AvgType.SIMPLE;

        public Factory() {
            super(InternalAvg.TYPE);
        }

        public Factory(String name, String bucketsPath, String fieldName, AvgType avgType) {
            super(name, InternalAvg.TYPE);
            this.fieldName = fieldName;
            this.avgType = avgType;
            this.bucketsPath = bucketsPath;
        }

        @Override
        public Reducer create(ReducerContext context, Reducer parent) {
            if (this.avgType == AvgType.SIMPLE) {
                return new SimpleAvgReducer(name, bucketsPath, fieldName, factories, context, parent);

            } else if (this.avgType == AvgType.LINEAR) {
                return new LinearWeightedAvgReducer(name, bucketsPath, fieldName, factories, context, parent);

            } else if (this.avgType == AvgType.EXPONENTIAL) {
                return new ExpWeightedAvgReducer(name, bucketsPath, fieldName, factories, context, parent);

            } else {
                return new SimpleAvgReducer(name, bucketsPath, fieldName, factories, context, parent);
            }

        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            name = in.readString();
            bucketsPath = in.readString();
            String avgType = in.readString();

            if (avgType.equals(AvgType.SIMPLE.name())) {
                this.avgType = AvgType.SIMPLE;

            } else if (avgType.equals(AvgType.LINEAR.name())) {
                this.avgType = AvgType.LINEAR;

            } else if (avgType.equals(AvgType.EXPONENTIAL.name())) {
                this.avgType = AvgType.EXPONENTIAL;

            } else {
                // NOCOMMIT throw exception if the type cannot be found
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(name);
            out.writeString(bucketsPath);
            out.writeString(avgType.name());
        }
        
    }

}
