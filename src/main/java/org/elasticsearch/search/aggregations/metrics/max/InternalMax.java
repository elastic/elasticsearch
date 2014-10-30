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
package org.elasticsearch.search.aggregations.metrics.max;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.AggregationStreams;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.metrics.InternalNumericMetricsAggregation;
import org.elasticsearch.search.aggregations.support.format.ValueFormatterStreams;

import java.io.IOException;

/**
*
*/
public class InternalMax extends InternalNumericMetricsAggregation.SingleValue implements Max {

    public final static Type TYPE = new Type("max");

    public final static AggregationStreams.Stream STREAM = new AggregationStreams.Stream() {
        @Override
        public InternalMax readResult(StreamInput in) throws IOException {
            InternalMax result = new InternalMax();
            result.readFrom(in);
            return result;
        }
    };

    public static void registerStreams() {
        AggregationStreams.registerStream(STREAM, TYPE.stream());
    }

    private double max;

    InternalMax() {} // for serialization

    public InternalMax(String name, double max) {
        super(name);
        this.max = max;
    }

    @Override
    public double value() {
        return max;
    }

    public double getValue() {
        return max;
    }

    @Override
    public Type type() {
        return TYPE;
    }

    @Override
    public InternalMax reduce(ReduceContext reduceContext) {
        double max = Double.NEGATIVE_INFINITY;
        for (InternalAggregation aggregation : reduceContext.aggregations()) {
            max = Math.max(max, ((InternalMax) aggregation).max);
        }
        return new InternalMax(name, max);
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        name = in.readString();
        valueFormatter = ValueFormatterStreams.readOptional(in);
        max = in.readDouble();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        ValueFormatterStreams.writeOptional(valueFormatter, out);
        out.writeDouble(max);
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        boolean hasValue = !Double.isInfinite(max);
        builder.field(CommonFields.VALUE, hasValue ? max : null);
        if (hasValue && valueFormatter != null) {
            builder.field(CommonFields.VALUE_AS_STRING, valueFormatter.format(max));
        }
        return builder;
    }
}
