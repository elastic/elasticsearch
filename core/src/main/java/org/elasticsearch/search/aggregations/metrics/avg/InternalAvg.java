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
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.metrics.InternalNumericMetricsAggregation;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class InternalAvg extends InternalNumericMetricsAggregation.SingleValue implements Avg {
    private final double sum;
    private final long count;

    public InternalAvg(String name, double sum, long count, DocValueFormat format, List<PipelineAggregator> pipelineAggregators,
            Map<String, Object> metaData) {
        super(name, pipelineAggregators, metaData);
        this.sum = sum;
        this.count = count;
        this.format = format;
    }

    /**
     * Read from a stream.
     */
    public InternalAvg(StreamInput in) throws IOException {
        super(in);
        format = in.readNamedWriteable(DocValueFormat.class);
        sum = in.readDouble();
        count = in.readVLong();
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeNamedWriteable(format);
        out.writeDouble(sum);
        out.writeVLong(count);
    }

    @Override
    public double value() {
        return getValue();
    }

    @Override
    public double getValue() {
        return sum / count;
    }

    double getSum() {
        return sum;
    }

    long getCount() {
        return count;
    }

    DocValueFormat getFormatter() {
        return format;
    }

    @Override
    public String getWriteableName() {
        return AvgAggregationBuilder.NAME;
    }

    @Override
    public InternalAvg doReduce(List<InternalAggregation> aggregations, ReduceContext reduceContext) {
        long count = 0;
        double sum = 0;
        for (InternalAggregation aggregation : aggregations) {
            count += ((InternalAvg) aggregation).count;
            sum += ((InternalAvg) aggregation).sum;
        }
        return new InternalAvg(getName(), sum, count, format, pipelineAggregators(), getMetaData());
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        builder.field(CommonFields.VALUE.getPreferredName(), count != 0 ? getValue() : null);
        if (count != 0 && format != DocValueFormat.RAW) {
            builder.field(CommonFields.VALUE_AS_STRING.getPreferredName(), format.format(getValue()));
        }
        return builder;
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(sum, count, format.getWriteableName());
    }

    @Override
    protected boolean doEquals(Object obj) {
        InternalAvg other = (InternalAvg) obj;
        return Objects.equals(sum, other.sum) &&
                Objects.equals(count, other.count) &&
                Objects.equals(format.getWriteableName(), other.format.getWriteableName());
    }
}
