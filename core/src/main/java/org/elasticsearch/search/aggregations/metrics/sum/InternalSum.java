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
package org.elasticsearch.search.aggregations.metrics.sum;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.AggregationStreams;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.metrics.InternalNumericMetricsAggregation;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
*
*/
public class InternalSum extends InternalNumericMetricsAggregation.SingleValue implements Sum {

    public final static Type TYPE = new Type("sum");

    public final static AggregationStreams.Stream STREAM = new AggregationStreams.Stream() {
        @Override
        public InternalSum readResult(StreamInput in) throws IOException {
            InternalSum result = new InternalSum();
            result.readFrom(in);
            return result;
        }
    };

    public static void registerStreams() {
        AggregationStreams.registerStream(STREAM, TYPE.stream());
    }

    private double sum;

    InternalSum() {} // for serialization

    InternalSum(String name, double sum, DocValueFormat formatter, List<PipelineAggregator> pipelineAggregators,
            Map<String, Object> metaData) {
        super(name, pipelineAggregators, metaData);
        this.sum = sum;
        this.format = formatter;
    }

    @Override
    public double value() {
        return sum;
    }

    @Override
    public double getValue() {
        return sum;
    }

    @Override
    public Type type() {
        return TYPE;
    }

    @Override
    public InternalSum doReduce(List<InternalAggregation> aggregations, ReduceContext reduceContext) {
        double sum = 0;
        for (InternalAggregation aggregation : aggregations) {
            sum += ((InternalSum) aggregation).sum;
        }
        return new InternalSum(name, sum, format, pipelineAggregators(), getMetaData());
    }

    @Override
    protected void doReadFrom(StreamInput in) throws IOException {
        format = in.readNamedWriteable(DocValueFormat.class);
        sum = in.readDouble();
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeNamedWriteable(format);
        out.writeDouble(sum);
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        builder.field(CommonFields.VALUE, sum);
        if (format != DocValueFormat.RAW) {
            builder.field(CommonFields.VALUE_AS_STRING, format.format(sum));
        }
        return builder;
    }

}
