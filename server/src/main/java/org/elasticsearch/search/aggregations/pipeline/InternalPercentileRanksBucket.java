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

package org.elasticsearch.search.aggregations.pipeline;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.metrics.InternalNumericMetricsAggregation;
import org.elasticsearch.search.aggregations.metrics.InternalMax;
import org.elasticsearch.search.aggregations.metrics.PercentileRanks;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class InternalPercentileRanksBucket extends InternalNumericMetricsAggregation.MultiValue implements PercentileRanksBucket {
    private double[] percentile_ranks;
    private double[] values;
    private boolean keyed = true;
    private final transient Map<Double, Double> percentileLookups = new HashMap<>();

    InternalPercentileRanksBucket(String name, double[] values, double[] PercentileRanks, boolean keyed,
                                     DocValueFormat formatter, List<PipelineAggregator> pipelineAggregators,
                                     Map<String, Object> metaData) {
        super(name, pipelineAggregators, metaData);
        if ((PercentileRanks.length == values.length) == false) {
            throw new IllegalArgumentException("The number of provided values and PercentileRanks didn't match. values: "
                    + Arrays.toString(values) + ", Percentile Ranks: " + Arrays.toString(PercentileRanks));
        }
        this.format = formatter;
        this.percentile_ranks = percentile_ranks;
        this.values = values;
        this.keyed = keyed;
        computeLookup();
    }

    private void computeLookup() {
        for (int i = 0; i < values.length; i++) {
            percentileLookups.put(values[i], percentile_ranks[i]);
        }
    }

    /**
     * Read from a stream.
     */
    public InternalPercentileRanksBucket(StreamInput in) throws IOException {
        super(in);
        format = in.readNamedWriteable(DocValueFormat.class);
        percentile_ranks = in.readDoubleArray();
        values = in.readDoubleArray();
        keyed = in.readBoolean();

        computeLookup();
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeNamedWriteable(format);
        out.writeDoubleArray(percentile_ranks);
        out.writeDoubleArray(values);
        out.writeBoolean(keyed);
    }

    @Override
    public String getWriteableName() {
        return PercentileRanksBucketPipelineAggregationBuilder.NAME;
    }

    @Override
    public double percentile_rank(double value) throws IllegalArgumentException {
        Double percentile_ranks = percentileLookups.get(value);
        if (percentile_ranks == null) {
            throw new IllegalArgumentException("value requested [" + String.valueOf(value) + "] was not" +
                    " one of the computed percentile ranks.  Available keys are: " + Arrays.toString(values));
        }
        return percentile_ranks;
    }

    @Override
    public String percentileRankAsString(double value) {
        return format.format(percentile_rank(value)).toString();
    }

    DocValueFormat formatter() {
        return format;
    }

    @Override
    public Iterator<Percentile> iterator() {
        return new Iter(values, percentile_ranks);
    }

    @Override
    public double value(String name) {
        return percentile_rank(Double.parseDouble(name));
    }

    @Override
    public InternalMax doReduce(List<InternalAggregation> aggregations, ReduceContext reduceContext) {
        throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        if (keyed) {
            builder.startObject("values");
            for (double value : values) {
                double value = percentile_rank(value);
                boolean hasValue = (Double.isInfinite(value) == true || Double.isNaN(value) == true) != true;
                String key = String.valueOf(value);
                builder.field(key, hasValue ? value : null);
                if (hasValue && format != DocValueFormat.RAW) {
                    builder.field(key + "_as_string", percentileAsString(value));
                }
            }
            builder.endObject();
        } else {
            builder.startArray("values");
            for (double value : values) {
                double value = percentile_rank(value);
                boolean hasValue = !(Double.isInfinite(value) || Double.isNaN(value));
                builder.startObject();
                builder.field("key", value);
                builder.field("value", hasValue ? value : null);
                if (hasValue && format != DocValueFormat.RAW) {
                    builder.field(String.valueOf(value) + "_as_string", percentileRankAsString(value));
                }
                builder.endObject();
            }
            builder.endArray();
        }
        return builder;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        if (super.equals(obj) == false) return false;

        InternalPercentileRanksBucket that = (InternalPercentileRanksBucket) obj;
        return Arrays.equals(values, that.values) && Arrays.equals(percentile_ranks, that.percentile_ranks);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), Arrays.hashCode(values), Arrays.hashCode(percentile_ranks));
    }

    public static class Iter implements Iterator<Percentile> {

        private final double[] values;
        private final double[] percentile_ranks;
        private int i;

        public Iter(double[] values, double[] percentile_ranks) {
            this.values = values;
            this.percentile_ranks = percentile_ranks;
            i = 0;
        }

        @Override
        public boolean hasNext() {
            return i < values.length;
        }

        @Override
        public Percentile next() {
            final Percentile next = new Percentile(percentile_ranks[i], values[i]);
            ++i;
            return next;
        }

        @Override
        public final void remove() {
            throw new UnsupportedOperationException();
        }
    }
}
