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
    private final double doubleSum;
    private final long longSum;
    private final boolean isFloating;

    InternalSum(String name, double sum, DocValueFormat formatter, List<PipelineAggregator> pipelineAggregators,
                    Map<String, Object> metaData) {
        super(name, pipelineAggregators, metaData);
        this.doubleSum = sum;
        this.longSum = (long) sum;
        this.format = formatter;
        this.isFloating = true;
    }

    InternalSum(String name, long sum, DocValueFormat formatter, List<PipelineAggregator> pipelineAggregators,
                Map<String, Object> metaData) {
        super(name, pipelineAggregators, metaData);
        this.doubleSum = (double) sum;
        this.longSum = sum;
        this.format = formatter;
        this.isFloating = false;
    }

    /**
     * Read from a stream.
     */
    public InternalSum(StreamInput in) throws IOException {
        super(in);
        format = in.readNamedWriteable(DocValueFormat.class);
        doubleSum = in.readDouble();
        longSum = in.readLong();
        isFloating = in.readBoolean();
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeNamedWriteable(format);
        out.writeDouble(doubleSum);
        out.writeLong(longSum);
        out.writeBoolean(isFloating);
    }

    @Override
    public String getWriteableName() {
        return SumAggregationBuilder.NAME;
    }

    @Override
    public double value() {
        return doubleSum;
    }

    @Override
    public long longValue() {
        return longSum;
    }

    @Override
    public double getValue() {
        return doubleSum;
    }

    @Override
    public long getLongValue() {
        return longSum;
    }

    @Override
    public InternalSum reduce(List<InternalAggregation> aggregations, ReduceContext reduceContext) {
        if (isFloating) {
            // Compute the sum of double values with Kahan summation algorithm which is more
            // accurate than naive summation.
            CompensatedSum kahanSummation = new CompensatedSum(0, 0);
            for (InternalAggregation aggregation : aggregations) {
                double value = ((InternalSum) aggregation).doubleSum;
                kahanSummation.add(value);
            }
            return new InternalSum(name, kahanSummation.value(), format, pipelineAggregators(), getMetaData());
        } else {
            // Compute the sum of long values with naive summation.
            long sum = 0L;
            for (InternalAggregation aggregation : aggregations) {
                long value = ((InternalSum) aggregation).longSum;
                sum += value;
            }
            return new InternalSum(name, sum, format, pipelineAggregators(), getMetaData());
        }
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        if (isFloating) {
            builder.field(CommonFields.VALUE.getPreferredName(), doubleSum);
            if (format != DocValueFormat.RAW) {
                builder.field(CommonFields.VALUE_AS_STRING.getPreferredName(), format.format(doubleSum).toString());
            }
            return builder;
        } else {
            builder.field(CommonFields.VALUE.getPreferredName(), longSum);
            if (format != DocValueFormat.RAW) {
                builder.field(CommonFields.VALUE_AS_STRING.getPreferredName(), format.format(longSum).toString());
            }
            return builder;
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), doubleSum, longSum);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        if (super.equals(obj) == false) return false;

        InternalSum that = (InternalSum) obj;
        return Objects.equals(doubleSum, that.doubleSum) && Objects.equals(longSum, that.longSum);
    }
}
