/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.metrics;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.AggregationReduceContext;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.support.SamplingContext;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;

abstract class AbstractInternalTDigestPercentiles extends InternalNumericMetricsAggregation.MultiValue {

    // NOTE: empty histograms track 0 centroids.
    // This reduces the amount of data to allocate, serialize and deserialize.
    private static final TDigestState EMPTY_HISTOGRAM = new EmptyTDigestState();

    protected final double[] keys;
    protected final TDigestState state;
    final boolean keyed;

    AbstractInternalTDigestPercentiles(
        String name,
        double[] keys,
        TDigestState state,
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
        if (in.getTransportVersion().onOrAfter(TransportVersion.V_8_8_0)) {
            if (in.readBoolean()) {
                state = TDigestState.read(in);
            } else {
                state = null;
            }
        } else {
            state = TDigestState.read(in);
        }
        keyed = in.readBoolean();
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeNamedWriteable(format);
        out.writeDoubleArray(keys);
        if (out.getTransportVersion().onOrAfter(TransportVersion.V_8_8_0)) {
            if (this.state != null) {
                out.writeBoolean(true);
                TDigestState.write(state, out);
            } else {
                out.writeBoolean(false);
            }
        } else {
            TDigestState state = this.state != null ? this.state : EMPTY_HISTOGRAM;
            TDigestState.write(state, out);
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
        return Arrays.stream(getKeys()).mapToObj(d -> String.valueOf(d)).toList();
    }

    public abstract double value(double key);

    public DocValueFormat formatter() {
        return format;
    }

    /**
     * Return the internal {@link TDigestState} sketch for this metric.
     */
    public TDigestState getState() {
        return state;
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
    public AbstractInternalTDigestPercentiles reduce(List<InternalAggregation> aggregations, AggregationReduceContext reduceContext) {
        TDigestState merged = null;
        for (InternalAggregation aggregation : aggregations) {
            final AbstractInternalTDigestPercentiles percentiles = (AbstractInternalTDigestPercentiles) aggregation;
            if (percentiles.state == null) {
                continue;
            }
            if (merged == null) {
                merged = TDigestState.createUsingParamsFrom(percentiles.state);
            }
            merged = merge(merged, percentiles.state);
        }
        if (merged == null) {
            merged = EMPTY_HISTOGRAM;
        }
        return createReduced(getName(), keys, merged, keyed, getMetadata());
    }

    /**
     * Merges two {@link TDigestState}s such that we always merge the one with smaller
     * compression into the one with larger compression.
     * This prevents producing a result that has lower than expected precision.
     *
     * @param digest1 The first histogram to merge
     * @param digest2 The second histogram to merge
     * @return One of the input histograms such that the one with larger compression is used as the one for merging
     */
    private TDigestState merge(final TDigestState digest1, final TDigestState digest2) {
        TDigestState largerCompression = digest1;
        TDigestState smallerCompression = digest2;
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
        TDigestState merged,
        boolean keyed,
        Map<String, Object> metadata
    );

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        TDigestState state = this.state != null ? this.state : EMPTY_HISTOGRAM;
        if (keyed) {
            builder.startObject(CommonFields.VALUES.getPreferredName());
            for (int i = 0; i < keys.length; ++i) {
                String key = String.valueOf(keys[i]);
                double value = value(keys[i]);
                builder.field(key, state.size() == 0 ? null : value);
                if (format != DocValueFormat.RAW && state.size() > 0) {
                    builder.field(key + "_as_string", format.format(value).toString());
                }
            }
            builder.endObject();
        } else {
            builder.startArray(CommonFields.VALUES.getPreferredName());
            for (int i = 0; i < keys.length; i++) {
                double value = value(keys[i]);
                builder.startObject();
                builder.field(CommonFields.KEY.getPreferredName(), keys[i]);
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
