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

package org.elasticsearch.search.reducers.metric.cusum;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.bucket.BucketStreamContext;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;
import org.elasticsearch.search.reducers.*;

import java.io.IOException;

public class CusumReducer extends Reducer {


    public static final ReducerFactoryStreams.Stream STREAM = new ReducerFactoryStreams.Stream() {
        @Override
        public ReducerFactory readResult(StreamInput in) throws IOException {
            Factory factory = new Factory();
            factory.readFrom(in);
            return factory;
        }
    };

    protected String fieldName;
    private double target;

    public static void registerStreams() {
        ReducerFactoryStreams.registerStream(STREAM, InternalCusum.TYPE.stream());
    }

    public CusumReducer(String name, String bucketsPath, String fieldName, double target, ReducerFactories factories, ReducerContext context, Reducer parent) {
        super(name, bucketsPath, factories, context, parent);
        this.fieldName = fieldName;
        this.target = target;
    }

    protected Object[] getProperties(MultiBucketsAggregation aggregation) {
        return (Object[]) aggregation.getProperty(fieldName);
    }

    @Override
    public InternalAggregation doReduce(MultiBucketsAggregation aggregation, BytesReference bucketType,
                                        BucketStreamContext bucketStreamContext) throws ReductionExecutionException {

        Object[] bucketProperties = this.getProperties(aggregation);

        double[] sums = new double[bucketProperties.length];
        double current = 0;
        for (int i = 0; i < bucketProperties.length; i++) {
            current += (double) bucketProperties[i] - target;
            sums[i] = current;
        }

        return new InternalCusum(name(), sums);
    }




    public static class Factory extends ReducerFactory {

        private String bucketsPath;
        private String fieldName;
        private double cusumTarget;

        public Factory() {
            super(InternalCusum.TYPE);
        }

        public Factory(String name, String bucketsPath, String fieldName, double cusumTarget) {
            super(name, InternalCusum.TYPE);
            this.fieldName = fieldName;
            this.bucketsPath = bucketsPath;
            this.cusumTarget = cusumTarget;
        }

        @Override
        public Reducer create(ReducerContext context, Reducer parent) {
            return new CusumReducer(name, bucketsPath, fieldName, cusumTarget, factories, context, parent);

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
