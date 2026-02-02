/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.aggregations.metrics;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.AggregationReduceContext;
import org.elasticsearch.search.aggregations.AggregatorReducer;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.support.SamplingContext;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;

abstract class AbstractInternalTDigestPercentiles extends InternalNumericMetricsAggregation.MultiValue {

    private static final TransportVersion QUERYDSL_PERCENTILES_EXPONENTIAL_HISTOGRAM_SUPPORT = TransportVersion.fromName(
        "query_dsl_percentiles_exponential_histogram_support"
    );

    protected static final Iterator<Percentile> EMPTY_ITERATOR = Collections.emptyIterator();

    protected final double[] keys;
    protected final HistogramUnionState state;
    final boolean keyed;

    AbstractInternalTDigestPercentiles(
        String name,
        double[] keys,
        HistogramUnionState state,
        boolean keyed,
        DocValueFormat formatter,
        Map<String, Object> metadata
    ) {
        super(name, formatter, metadata);
        this.keys = keys;
        this.state = state;
        this.keyed = keyed;

        if (state != null) {
            state.compress();
        }
    }

    /**
     * Read from a stream.
     */
    protected AbstractInternalTDigestPercentiles(StreamInput in) throws IOException {
        super(in);
        keys = in.readDoubleArray();
        if (in.readBoolean()) {
            if (in.getTransportVersion().supports(QUERYDSL_PERCENTILES_EXPONENTIAL_HISTOGRAM_SUPPORT)) {
                state = HistogramUnionState.read(HistogramUnionState.NOOP_BREAKER, in);
            } else {
                state = HistogramUnionState.readAsPureTDigest(HistogramUnionState.NOOP_BREAKER, in);
            }
        } else {
            state = null;
        }
        keyed = in.readBoolean();
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeNamedWriteable(format);
        out.writeDoubleArray(keys);
        if (this.state != null) {
            out.writeBoolean(true);
            if (out.getTransportVersion().supports(QUERYDSL_PERCENTILES_EXPONENTIAL_HISTOGRAM_SUPPORT)) {
                state.writeTo(out);
            } else {
                state.writeAsPureTDigestTo(out);
            }
        } else {
            out.writeBoolean(false);
        }
        out.writeBoolean(keyed);
    }

    @Override
    public double value(String name) {
        if (this.keys.length == 1 && this.name.equals(name)) {
            return value(this.keys[0]);
        }
        return value(Double.parseDouble(name));
    }

    @Override
    public Iterable<String> valueNames() {
        return Arrays.stream(getKeys()).mapToObj(String::valueOf).toList();
    }

    public abstract double value(double key);

    public DocValueFormat formatter() {
        return format;
    }

    /**
     * Return the internal {@link HistogramUnionState} sketch for this metric.
     */
    public HistogramUnionState getState() {
        return state == null ? HistogramUnionState.EMPTY : state;
    }

    /**
     * Return the keys (percentiles) requested.
     */
    public double[] getKeys() {
        return keys;
    }

    /**
     * Should the output be keyed.
     */
    public boolean keyed() {
        return keyed;
    }

    @Override
    protected AggregatorReducer getLeaderReducer(AggregationReduceContext reduceContext, int size) {
        return new AggregatorReducer() {
            HistogramUnionState merged = null;

            @Override
            public void accept(InternalAggregation aggregation) {
                final AbstractInternalTDigestPercentiles percentiles = (AbstractInternalTDigestPercentiles) aggregation;
                if (percentiles.state != null) {
                    if (merged == null) {
                        merged = HistogramUnionState.createUsingParamsFrom(percentiles.state);
                    }
                    merged = merge(merged, percentiles.state);
                }
            }

            @Override
            public InternalAggregation get() {
                return createReduced(getName(), keys, merged == null ? HistogramUnionState.EMPTY : merged, keyed, getMetadata());
            }
        };
    }

    /**
     * Merges two {@link HistogramUnionState}s such that we always merge the one with smaller
     * compression into the one with larger compression.
     * This prevents producing a result that has lower than expected precision.
     *
     * @param digest1 The first histogram to merge
     * @param digest2 The second histogram to merge
     * @return One of the input histograms such that the one with larger compression is used as the one for merging
     */
    private static HistogramUnionState merge(final HistogramUnionState digest1, final HistogramUnionState digest2) {
        HistogramUnionState largerCompression = digest1;
        HistogramUnionState smallerCompression = digest2;
        if (digest2.compression() > digest1.compression()) {
            largerCompression = digest2;
            smallerCompression = digest1;
        }
        largerCompression.add(smallerCompression);
        return largerCompression;
    }

    @Override
    public InternalAggregation finalizeSampling(SamplingContext samplingContext) {
        return this;
    }

    protected abstract AbstractInternalTDigestPercentiles createReduced(
        String name,
        double[] keys,
        HistogramUnionState merged,
        boolean keyed,
        Map<String, Object> metadata
    );

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        HistogramUnionState state = getState();
        if (keyed) {
            builder.startObject(CommonFields.VALUES.getPreferredName());
            for (double v : keys) {
                String key = String.valueOf(v);
                double value = value(v);
                builder.field(key, state.size() == 0 ? null : value);
                if (format != DocValueFormat.RAW && state.size() > 0) {
                    builder.field(key + "_as_string", format.format(value).toString());
                }
            }
            builder.endObject();
        } else {
            builder.startArray(CommonFields.VALUES.getPreferredName());
            for (double key : keys) {
                double value = value(key);
                builder.startObject();
                builder.field(CommonFields.KEY.getPreferredName(), key);
                builder.field(CommonFields.VALUE.getPreferredName(), state.size() == 0 ? null : value);
                if (format != DocValueFormat.RAW && state.size() > 0) {
                    builder.field(CommonFields.VALUE_AS_STRING.getPreferredName(), format.format(value).toString());
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

        AbstractInternalTDigestPercentiles that = (AbstractInternalTDigestPercentiles) obj;
        return keyed == that.keyed && Arrays.equals(keys, that.keys) && Objects.equals(state, that.state);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), keyed, Arrays.hashCode(keys), state);
    }
}
