/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.metrics;

import org.HdrHistogram.DoubleHistogram;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.AggregationReduceContext;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.support.SamplingContext;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.zip.DataFormatException;

abstract class AbstractInternalHDRPercentiles extends InternalNumericMetricsAggregation.MultiValue {

    protected final double[] keys;
    protected final DoubleHistogram state;
    protected final boolean keyed;

    AbstractInternalHDRPercentiles(
        String name,
        double[] keys,
        DoubleHistogram state,
        boolean keyed,
        DocValueFormat format,
        Map<String, Object> metadata
    ) {
        super(name, format, metadata);
        this.keys = keys;
        this.state = state;
        this.keyed = keyed;
    }

    /**
     * Read from a stream.
     */
    protected AbstractInternalHDRPercentiles(StreamInput in) throws IOException {
        super(in);
        keys = in.readDoubleArray();
        long minBarForHighestToLowestValueRatio = in.readLong();
        final int serializedLen = in.readVInt();
        byte[] bytes = new byte[serializedLen];
        in.readBytes(bytes, 0, serializedLen);
        ByteBuffer stateBuffer = ByteBuffer.wrap(bytes);
        try {
            state = DoubleHistogram.decodeFromCompressedByteBuffer(stateBuffer, minBarForHighestToLowestValueRatio);
        } catch (DataFormatException e) {
            throw new IOException("Failed to decode DoubleHistogram for aggregation [" + name + "]", e);
        }
        keyed = in.readBoolean();
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeNamedWriteable(format);
        out.writeDoubleArray(keys);
        out.writeLong(state.getHighestToLowestValueRatio());
        ByteBuffer stateBuffer = ByteBuffer.allocate(state.getNeededByteBufferCapacity());
        final int serializedLen = state.encodeIntoCompressedByteBuffer(stateBuffer);
        out.writeVInt(serializedLen);
        out.writeBytes(stateBuffer.array(), 0, serializedLen);
        out.writeBoolean(keyed);
    }

    @Override
    public double value(String name) {
        return value(Double.parseDouble(name));
    }

    @Override
    public Iterable<String> valueNames() {
        return Arrays.stream(getKeys()).mapToObj(d -> String.valueOf(d)).toList();
    }

    public DocValueFormat formatter() {
        return format;
    }

    public abstract double value(double key);

    public long getEstimatedMemoryFootprint() {
        return state.getEstimatedFootprintInBytes();
    }

    /**
     * Return the internal {@link DoubleHistogram} sketch for this metric.
     */
    public DoubleHistogram getState() {
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
    public AbstractInternalHDRPercentiles reduce(List<InternalAggregation> aggregations, AggregationReduceContext reduceContext) {
        DoubleHistogram merged = null;
        for (InternalAggregation aggregation : aggregations) {
            final AbstractInternalHDRPercentiles percentiles = (AbstractInternalHDRPercentiles) aggregation;
            if (merged == null) {
                merged = new DoubleHistogram(percentiles.state);
                merged.setAutoResize(true);
            }
            merged.add(percentiles.state);
        }
        return createReduced(getName(), keys, merged, keyed, getMetadata());
    }

    @Override
    public InternalAggregation finalizeSampling(SamplingContext samplingContext) {
        return this;
    }

    protected abstract AbstractInternalHDRPercentiles createReduced(
        String name,
        double[] keys,
        DoubleHistogram merged,
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
                builder.field(key, state.getTotalCount() == 0 ? null : value);
                if (format != DocValueFormat.RAW && state.getTotalCount() > 0) {
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
                builder.field(CommonFields.VALUE.getPreferredName(), state.getTotalCount() == 0 ? null : value);
                if (format != DocValueFormat.RAW && state.getTotalCount() > 0) {
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

        AbstractInternalHDRPercentiles that = (AbstractInternalHDRPercentiles) obj;
        return keyed == that.keyed && Arrays.equals(keys, that.keys) && Objects.equals(state, that.state);
    }

    @Override
    public int hashCode() {
        // we cannot use state.hashCode at the moment because of:
        // https://github.com/HdrHistogram/HdrHistogram/issues/81
        // TODO: upgrade the HDRHistogram library
        return Objects.hash(
            super.hashCode(),
            keyed,
            Arrays.hashCode(keys),
            state.getIntegerToDoubleValueConversionRatio(),
            state.getTotalCount()
        );
    }
}
