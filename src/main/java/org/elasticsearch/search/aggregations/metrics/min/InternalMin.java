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
package org.elasticsearch.search.aggregations.metrics.min;

import org.elasticsearch.common.inject.internal.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.AggregationStreams;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.metrics.InternalNumericMetricsAggregation;
import org.elasticsearch.search.aggregations.support.format.ValueFormatter;
import org.elasticsearch.search.aggregations.support.format.ValueFormatterStreams;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
*
*/
public class InternalMin extends InternalNumericMetricsAggregation.SingleValue implements Min {

    public final static Type TYPE = new Type("min");

    public final static AggregationStreams.Stream STREAM = new AggregationStreams.Stream() {
        @Override
        public InternalMin readResult(StreamInput in) throws IOException {
            InternalMin result = new InternalMin();
            result.readFrom(in);
            return result;
        }
    };

    public static void registerStreams() {
        AggregationStreams.registerStream(STREAM, TYPE.stream());
    }


    private double min;

    InternalMin() {} // for serialization

    public InternalMin(String name, double min, @Nullable ValueFormatter formatter, Map<String, Object> metaData) {
        super(name, metaData);
        this.min = min;
        this.valueFormatter = formatter;
    }

    @Override
    public double value() {
        return min;
    }

    @Override
    public double getValue() {
        return min;
    }

    @Override
    public Type type() {
        return TYPE;
    }

    @Override
    public InternalMin reduce(List<InternalAggregation> aggregations, ReduceContext reduceContext) {
        double min = Double.POSITIVE_INFINITY;
        for (InternalAggregation aggregation : aggregations) {
            min = Math.min(min, ((InternalMin) aggregation).min);
        }
        return new InternalMin(getName(), min, this.valueFormatter, getMetaData());
    }

    @Override
    protected void doReadFrom(StreamInput in) throws IOException {
        valueFormatter = ValueFormatterStreams.readOptional(in);
        min = in.readDouble();
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        ValueFormatterStreams.writeOptional(valueFormatter, out);
        out.writeDouble(min);
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        boolean hasValue = !Double.isInfinite(min);
        builder.field(CommonFields.VALUE, hasValue ? min : null);
        if (hasValue && valueFormatter != null && !(valueFormatter instanceof ValueFormatter.Raw)) {
            builder.field(CommonFields.VALUE_AS_STRING, valueFormatter.format(min));
        }
        return builder;
    }

}
