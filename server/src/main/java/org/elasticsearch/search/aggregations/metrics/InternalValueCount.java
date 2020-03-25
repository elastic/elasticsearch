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
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * An internal implementation of {@link ValueCount}.
 */
public class InternalValueCount extends InternalNumericMetricsAggregation.SingleValue implements ValueCount {
    private final long value;

    InternalValueCount(String name, long value, List<PipelineAggregator> pipelineAggregators,
            Map<String, Object> metaData) {
        super(name, pipelineAggregators, metaData);
        this.value = value;
    }

    /**
     * Read from a stream.
     */
    public InternalValueCount(StreamInput in) throws IOException {
        super(in);
        value = in.readVLong();
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeVLong(value);
    }

    @Override
    public String getWriteableName() {
        return ValueCountAggregationBuilder.NAME;
    }

    @Override
    public long getValue() {
        return value;
    }

    @Override
    public double value() {
        return value;
    }

    @Override
    public InternalAggregation reduce(List<InternalAggregation> aggregations, ReduceContext reduceContext) {
        long valueCount = 0;
        for (InternalAggregation aggregation : aggregations) {
            valueCount += ((InternalValueCount) aggregation).value;
        }
        return new InternalValueCount(name, valueCount, pipelineAggregators(), getMetaData());
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        builder.field(CommonFields.VALUE.getPreferredName(), value);
        return builder;
    }

    @Override
    public String toString() {
        return "count[" + value + "]";
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), value);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        if (super.equals(obj) == false) return false;

        InternalValueCount that = (InternalValueCount) obj;
        return Objects.equals(this.value, that.value);
    }
}
