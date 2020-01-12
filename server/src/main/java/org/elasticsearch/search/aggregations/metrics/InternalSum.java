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
package org.elasticsearch.search.aggregations.metrics;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class InternalSum extends InternalNumericMetricsAggregation.SingleValue implements Sum {
    private final double sum;

    InternalSum(String name, double sum, DocValueFormat formatter, List<PipelineAggregator> pipelineAggregators,
                    Map<String, Object> metaData) {
        super(name, pipelineAggregators, metaData);
        this.sum = sum;
        this.format = formatter;
    }

    /**
     * Read from a stream.
     */
    public InternalSum(StreamInput in) throws IOException {
        super(in);
        format = in.readNamedWriteable(DocValueFormat.class);
        sum = in.readDouble();
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeNamedWriteable(format);
        out.writeDouble(sum);
    }

    @Override
    public String getWriteableName() {
        return SumAggregationBuilder.NAME;
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
    public InternalSum reduce(List<InternalAggregation> aggregations, ReduceContext reduceContext) {
        // Compute the sum of double values with Kahan summation algorithm which is more
        // accurate than naive summation.
        CompensatedSum kahanSummation = new CompensatedSum(0, 0);
        for (InternalAggregation aggregation : aggregations) {
            double value = ((InternalSum) aggregation).sum;
            kahanSummation.add(value);
        }
        return new InternalSum(name, kahanSummation.value(), format, pipelineAggregators(), getMetaData());
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        builder.field(CommonFields.VALUE.getPreferredName(), sum);
        if (format != DocValueFormat.RAW) {
            builder.field(CommonFields.VALUE_AS_STRING.getPreferredName(), format.format(sum).toString());
        }
        return builder;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), sum);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        if (super.equals(obj) == false) return false;

        InternalSum that = (InternalSum) obj;
        return Objects.equals(sum, that.sum);
    }
}
