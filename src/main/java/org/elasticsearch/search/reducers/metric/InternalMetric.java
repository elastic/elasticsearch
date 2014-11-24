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
package org.elasticsearch.search.reducers.metric;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.AggregationStreams;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.metrics.InternalNumericMetricsAggregation;
import org.elasticsearch.search.aggregations.metrics.sum.Sum;
import org.elasticsearch.search.aggregations.support.format.ValueFormatterStreams;
import org.elasticsearch.search.reducers.ReducerFactoryStreams;

import java.io.IOException;

/**
*
*/
public class InternalMetric extends InternalNumericMetricsAggregation.SingleValue {

    public final static Type TYPE = new Type("simple_metric");

    public final static AggregationStreams.Stream STREAM = new AggregationStreams.Stream() {
        @Override
        public InternalMetric readResult(StreamInput in) throws IOException {
            InternalMetric result = new InternalMetric();
            result.readFrom(in);
            return result;
        }
    };

    public static void registerStreams() {
        AggregationStreams.registerStream(STREAM, TYPE.stream());
    }

    private Number value;

    InternalMetric() {} // for serialization

    public InternalMetric(String name, Number value) {
        super();
        this.name = name;
        this.value = value;
    }


    @Override
    public double value() {
        return value.doubleValue();
    }

    public double getValue() {
        return value.doubleValue();
    }

    @Override
    public Type type() {
        return TYPE;
    }

    @Override
    public InternalAggregation reduce(ReduceContext reduceContext) {
        throw new UnsupportedOperationException("Shard reducers not implemented yet.");
    }


    @Override
    public void doReadFrom(StreamInput in) throws IOException {
        name = in.readString();
        valueFormatter = ValueFormatterStreams.readOptional(in);
        value = in.readDouble();
    }

    @Override
    public void doWriteTo(StreamOutput out) throws IOException {
        out.writeString(name);
        ValueFormatterStreams.writeOptional(valueFormatter, out);
        out.writeDouble(value.doubleValue());
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        builder.field(CommonFields.VALUE, value);
        if (valueFormatter != null) {
            builder.field(CommonFields.VALUE_AS_STRING, valueFormatter.format(value.doubleValue()));
        }
        return builder;
    }

}
