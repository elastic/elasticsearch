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

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.AggregationStreams;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.metrics.InternalNumericMetricsAggregation;

import java.io.IOException;

public class InternalAvg extends InternalNumericMetricsAggregation.SingleValue implements Avg {

    public static final Type TYPE = new Type("avg");

    public static final AggregationStreams.Stream STREAM = new AggregationStreams.Stream() {
        @Override
        public InternalAvg readResult(StreamInput in) throws IOException {
            InternalAvg avg = new InternalAvg();
            avg.readFrom(in);
            return avg;
        }
    };

    public static void registerStreams() {
        AggregationStreams.registerStream(STREAM, TYPE.stream());
    }

    private double value;

    public InternalAvg() {
        super();
    }

    public InternalAvg(String name, double avg) {
        super(name);
        this.value = avg;
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
    public void readFrom(StreamInput in) throws IOException {
        name = in.readString();
        value = in.readDouble();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
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
