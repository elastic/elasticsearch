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
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.support.format.ValueFormatter;
import org.elasticsearch.search.aggregations.support.format.ValueFormatterStreams;

import java.io.IOException;
import java.util.List;
import java.util.Map;

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

    public InternalAvg(String name, double sum, long count, ValueFormatter formatter, List<PipelineAggregator> pipelineAggregators,
            Map<String, Object> metaData) {
        super(name, pipelineAggregators, metaData);
        this.sum = sum;
        this.count = count;
        this.valueFormatter = formatter;
    }

    @Override
    public double value() {
        return getValue();
    }

    @Override
    public double getValue() {
        return sum / count;
    }

    @Override
    public Type type() {
        return TYPE;
    }

    @Override
    public InternalAvg doReduce(List<InternalAggregation> aggregations, ReduceContext reduceContext) {
        long count = 0;
        double sum = 0;
        for (InternalAggregation aggregation : aggregations) {
            count += ((InternalAvg) aggregation).count;
            sum += ((InternalAvg) aggregation).sum;
        }
        return new InternalAvg(getName(), sum, count, valueFormatter, pipelineAggregators(), getMetaData());
    }

    @Override
    protected void doReadFrom(StreamInput in) throws IOException {
        valueFormatter = ValueFormatterStreams.readOptional(in);
        sum = in.readDouble();
        count = in.readVLong();
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        ValueFormatterStreams.writeOptional(valueFormatter, out);
        out.writeDouble(sum);
        out.writeVLong(count);
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        builder.field(CommonFields.VALUE, count != 0 ? getValue() : null);
        if (count != 0 && !(valueFormatter instanceof ValueFormatter.Raw)) {
            builder.field(CommonFields.VALUE_AS_STRING, valueFormatter.format(getValue()));
        }
        return builder;
    }

}
