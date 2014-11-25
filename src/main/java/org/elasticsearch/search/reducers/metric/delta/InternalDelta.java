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

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.AggregationStreams;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.metrics.InternalNumericMetricsAggregation;

import java.io.IOException;
import java.util.Map;

public class InternalDelta extends InternalNumericMetricsAggregation.SingleValue implements Delta {

    public static final Type TYPE = new Type("delta");

    public static final AggregationStreams.Stream STREAM = new AggregationStreams.Stream() {
        @Override
        public InternalDelta readResult(StreamInput in) throws IOException {
            InternalDelta delta = new InternalDelta();
            delta.readFrom(in);
            return delta;
        }
    };

    public static void registerStreams() {
        AggregationStreams.registerStream(STREAM, TYPE.stream());
    }

    private double value;

    public InternalDelta() {
        super();
    }

    public InternalDelta(String name, double deltaValue, Map<String, Object> metaData) {
        super(name, metaData);
        this.value = deltaValue;
    }

    @Override
    public Type type() {
        return TYPE;
    }

    @Override
    public double getValue() {
        return value();
    }

    @Override
    public double value() {
        return value;
    }

    
    @Override
    public void doReadFrom(StreamInput in) throws IOException {
        value = in.readDouble();
    }

    @Override
    public void doWriteTo(StreamOutput out) throws IOException {
        out.writeDouble(value);
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        builder.field("value", value);
        return builder;
    }

    @Override
    public InternalAggregation reduce(ReduceContext reduceContext) {
        throw new UnsupportedOperationException("Not supported");
    }

}
