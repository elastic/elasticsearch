/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.metrics;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.InternalAggregation;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

abstract class AbstractInternalTDigestPercentiles extends InternalNumericMetricsAggregation.MultiValue {

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
        super(name, metadata);
        this.keys = keys;
        this.state = state;
        this.keyed = keyed;
        this.format = formatter;
    }

    /**
     * Read from a stream.
     */
    protected AbstractInternalTDigestPercentiles(StreamInput in) throws IOException {
        super(in);
        format = in.readNamedWriteable(DocValueFormat.class);
        keys = in.readDoubleArray();
        state = TDigestState.read(in);
        keyed = in.readBoolean();
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeNamedWriteable(format);
        out.writeDoubleArray(keys);
        TDigestState.write(state, out);
        out.writeBoolean(keyed);
    }

    @Override
    public double value(String name) {
        return value(Double.parseDouble(name));
    }

    @Override
    public Iterable<String> valueNames() {
        return Arrays.stream(getKeys()).mapToObj(d -> String.valueOf(d)).collect(Collectors.toList());
    }

    public abstract double value(double key);

    public DocValueFormat formatter() {
        return format;
    }

    public long getEstimatedMemoryFootprint() {
        return state.byteSize();
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
    public AbstractInternalTDigestPercentiles reduce(List<InternalAggregation> aggregations, ReduceContext reduceContext) {
        TDigestState merged = null;
        for (InternalAggregation aggregation : aggregations) {
            final AbstractInternalTDigestPercentiles percentiles = (AbstractInternalTDigestPercentiles) aggregation;
            if (merged == null) {
                merged = new TDigestState(percentiles.state.compression());
            }
            merged.add(percentiles.state);
        }
        return createReduced(getName(), keys, merged, keyed, getMetadata());
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
