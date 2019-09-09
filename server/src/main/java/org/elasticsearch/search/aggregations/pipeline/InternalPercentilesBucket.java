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
import org.elasticsearch.search.aggregations.metrics.Percentile;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class InternalPercentilesBucket extends InternalNumericMetricsAggregation.MultiValue implements PercentilesBucket {
    private double[] percentiles;
    private double[] percents;
    private boolean keyed = true;
    private final transient Map<Double, Double> percentileLookups = new HashMap<>();

    InternalPercentilesBucket(String name, double[] percents, double[] percentiles, boolean keyed,
                                     DocValueFormat formatter, List<PipelineAggregator> pipelineAggregators,
                                     Map<String, Object> metaData) {
        super(name, pipelineAggregators, metaData);
        if ((percentiles.length == percents.length) == false) {
            throw new IllegalArgumentException("The number of provided percents and percentiles didn't match. percents: "
                    + Arrays.toString(percents) + ", percentiles: " + Arrays.toString(percentiles));
        }
        this.format = formatter;
        this.percentiles = percentiles;
        this.percents = percents;
        this.keyed = keyed;
        computeLookup();
    }

    private void computeLookup() {
        for (int i = 0; i < percents.length; i++) {
            percentileLookups.put(percents[i], percentiles[i]);
        }
    }

    /**
     * Read from a stream.
     */
    public InternalPercentilesBucket(StreamInput in) throws IOException {
        super(in);
        format = in.readNamedWriteable(DocValueFormat.class);
        percentiles = in.readDoubleArray();
        percents = in.readDoubleArray();
        keyed = in.readBoolean();

        computeLookup();
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeNamedWriteable(format);
        out.writeDoubleArray(percentiles);
        out.writeDoubleArray(percents);
        out.writeBoolean(keyed);
    }

    @Override
    public String getWriteableName() {
        return PercentilesBucketPipelineAggregationBuilder.NAME;
    }

    @Override
    public double percentile(double percent) throws IllegalArgumentException {
        Double percentile = percentileLookups.get(percent);
        if (percentile == null) {
            throw new IllegalArgumentException("Percent requested [" + String.valueOf(percent) + "] was not" +
                    " one of the computed percentiles.  Available keys are: " + Arrays.toString(percents));
        }
        return percentile;
    }

    @Override
    public String percentileAsString(double percent) {
        return format.format(percentile(percent)).toString();
    }

    DocValueFormat formatter() {
        return format;
    }

    @Override
    public Iterator<Percentile> iterator() {
        return new Iter(percents, percentiles);
    }

    @Override
    public double value(String name) {
        return percentile(Double.parseDouble(name));
    }

    @Override
    public InternalMax doReduce(List<InternalAggregation> aggregations, ReduceContext reduceContext) {
        throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        if (keyed) {
            builder.startObject("values");
            for (double percent : percents) {
                double value = percentile(percent);
                boolean hasValue = !(Double.isInfinite(value) || Double.isNaN(value));
                String key = String.valueOf(percent);
                builder.field(key, hasValue ? value : null);
                if (hasValue && format != DocValueFormat.RAW) {
                    builder.field(key + "_as_string", percentileAsString(percent));
                }
            }
            builder.endObject();
        } else {
            builder.startArray("values");
            for (double percent : percents) {
                double value = percentile(percent);
                boolean hasValue = !(Double.isInfinite(value) || Double.isNaN(value));
                builder.startObject();
                builder.field("key", percent);
                builder.field("value", hasValue ? value : null);
                if (hasValue && format != DocValueFormat.RAW) {
                    builder.field(String.valueOf(percent) + "_as_string", percentileAsString(percent));
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

        InternalPercentilesBucket that = (InternalPercentilesBucket) obj;
        return Arrays.equals(percents, that.percents) && Arrays.equals(percentiles, that.percentiles);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), Arrays.hashCode(percents), Arrays.hashCode(percentiles));
    }

    public static class Iter implements Iterator<Percentile> {

        private final double[] percents;
        private final double[] percentiles;
        private int i;

        public Iter(double[] percents, double[] percentiles) {
            this.percents = percents;
            this.percentiles = percentiles;
            i = 0;
        }

        @Override
        public boolean hasNext() {
            return i < percents.length;
        }

        @Override
        public Percentile next() {
            final Percentile next = new Percentile(percents[i], percentiles[i]);
            ++i;
            return next;
        }

        @Override
        public final void remove() {
            throw new UnsupportedOperationException();
        }
    }
}
