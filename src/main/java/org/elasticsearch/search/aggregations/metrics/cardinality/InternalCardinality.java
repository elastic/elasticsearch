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

package org.elasticsearch.search.aggregations.metrics.cardinality;

import org.elasticsearch.common.inject.internal.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.AggregationStreams;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.metrics.InternalNumericMetricsAggregation;
import org.elasticsearch.search.aggregations.support.format.ValueFormatter;
import org.elasticsearch.search.aggregations.support.format.ValueFormatterStreams;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public final class InternalCardinality extends InternalNumericMetricsAggregation.SingleValue implements Cardinality {

    public final static Type TYPE = new Type("cardinality");

    public final static AggregationStreams.Stream STREAM = new AggregationStreams.Stream() {
        @Override
        public InternalCardinality readResult(StreamInput in) throws IOException {
            InternalCardinality result = new InternalCardinality();
            result.readFrom(in);
            return result;
        }
    };

    public static void registerStreams() {
        AggregationStreams.registerStream(STREAM, TYPE.stream());
    }

    private HyperLogLogPlusPlus counts;

    InternalCardinality(String name, HyperLogLogPlusPlus counts, @Nullable ValueFormatter formatter, Map<String, Object> metaData) {
        super(name, metaData);
        this.counts = counts;
        this.valueFormatter = formatter;
    }

    private InternalCardinality() {
    }

    @Override
    public double value() {
        return getValue();
    }

    @Override
    public long getValue() {
        return counts == null ? 0 : counts.cardinality(0);
    }

    @Override
    public Type type() {
        return TYPE;
    }

    @Override
    protected void doReadFrom(StreamInput in) throws IOException {
        valueFormatter = ValueFormatterStreams.readOptional(in);
        if (in.readBoolean()) {
            counts = HyperLogLogPlusPlus.readFrom(in, BigArrays.NON_RECYCLING_INSTANCE);
        } else {
            counts = null;
        }
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        ValueFormatterStreams.writeOptional(valueFormatter, out);
        if (counts != null) {
            out.writeBoolean(true);
            counts.writeTo(0, out);
        } else {
            out.writeBoolean(false);
        }
    }

    @Override
    public InternalAggregation reduce(List<InternalAggregation> aggregations, ReduceContext reduceContext) {
        InternalCardinality reduced = null;
        for (InternalAggregation aggregation : aggregations) {
            final InternalCardinality cardinality = (InternalCardinality) aggregation;
            if (cardinality.counts != null) {
                if (reduced == null) {
                    reduced = new InternalCardinality(name, new HyperLogLogPlusPlus(cardinality.counts.precision(),
                            BigArrays.NON_RECYCLING_INSTANCE, 1), this.valueFormatter, getMetaData());
                }
                reduced.merge(cardinality);
            }
        }

        if (reduced == null) { // all empty
            return aggregations.get(0);
        } else {
            return reduced;
        }
    }

    public void merge(InternalCardinality other) {
        assert counts != null && other != null;
        counts.merge(0, other.counts, 0);
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        final long cardinality = getValue();
        builder.field(CommonFields.VALUE, cardinality);
        if (valueFormatter != null && !(valueFormatter instanceof ValueFormatter.Raw)) {
            builder.field(CommonFields.VALUE_AS_STRING, valueFormatter.format(cardinality));
        }
        return builder;
    }

}
