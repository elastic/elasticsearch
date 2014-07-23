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
package org.elasticsearch.search.aggregations.metrics.avg;

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
public class InternalAvg extends InternalNumericMetricsAggregation.SingleValue implements Avg {

    public final static Type TYPE = new Type("avg");

    public final static AggregationStreams.Stream STREAM = new AggregationStreams.Stream() {
        @Override
        public InternalAvg readResult(StreamInput in) throws IOException {
            InternalAvg result = new InternalAvg();
            result.readFrom(in);
            return result;
        }
    };

    public static void registerStreams() {
        AggregationStreams.registerStream(STREAM, TYPE.stream());
    }

    private double sum;
    private long count;

    InternalAvg() {} // for serialization

    public InternalAvg(String name, double sum, long count) {
        super(name);
        this.sum = sum;
        this.count = count;
    }

    @Override
    public double value() {
        return getValue();
    }

    public double getValue() {
        return sum / count;
    }

    @Override
    public Type type() {
        return TYPE;
    }

    @Override
    public InternalAvg reduce(ReduceContext reduceContext) {
        long count = 0;
        double sum = 0;
        for (InternalAggregation aggregation : reduceContext.aggregations()) {
            count += ((InternalAvg) aggregation).count;
            sum += ((InternalAvg) aggregation).sum;
        }
        return new InternalAvg(getName(), sum, count);
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        name = in.readString();
        valueFormatter = ValueFormatterStreams.readOptional(in);
        sum = in.readDouble();
        count = in.readVLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        ValueFormatterStreams.writeOptional(valueFormatter, out);
        out.writeDouble(sum);
        out.writeVLong(count);
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        builder.field(CommonFields.VALUE, count != 0 ? getValue() : null);
        if (count != 0 && valueFormatter != null) {
            builder.field(CommonFields.VALUE_AS_STRING, valueFormatter.format(getValue()));
        }
        return builder;
    }

}
