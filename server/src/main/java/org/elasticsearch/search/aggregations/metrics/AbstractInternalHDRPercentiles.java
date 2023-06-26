/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.metrics;

import org.HdrHistogram.DoubleHistogram;
import org.elasticsearch.TransportVersion;
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
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.zip.DataFormatException;

abstract class AbstractInternalHDRPercentiles extends InternalNumericMetricsAggregation.MultiValue {

    protected static final Iterator<Percentile> EMPTY_ITERATOR = Collections.emptyIterator();
    private static final DoubleHistogram EMPTY_HISTOGRAM_THREE_DIGITS = new DoubleHistogram(3);
    private static final DoubleHistogram EMPTY_HISTOGRAM_ZERO_DIGITS = new EmptyDoubleHdrHistogram();

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
        if (in.getTransportVersion().onOrAfter(TransportVersion.V_8_7_0)) {
            if (in.readBoolean()) {
                state = decode(in);
            } else {
                state = null;
            }
        } else {
            state = decode(in);
        }
        keyed = in.readBoolean();
    }

    private DoubleHistogram decode(StreamInput in) throws IOException {
        long minBarForHighestToLowestValueRatio = in.readLong();
        final int serializedLen = in.readVInt();
        byte[] bytes = new byte[serializedLen];
        in.readBytes(bytes, 0, serializedLen);
        ByteBuffer stateBuffer = ByteBuffer.wrap(bytes);
        try {
            return DoubleHistogram.decodeFromCompressedByteBuffer(stateBuffer, minBarForHighestToLowestValueRatio);
        } catch (DataFormatException e) {
            throw new IOException("Failed to decode DoubleHistogram for aggregation [" + name + "]", e);
        }
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeNamedWriteable(format);
        out.writeDoubleArray(keys);
        if (out.getTransportVersion().onOrAfter(TransportVersion.V_8_7_0)) {
            if (this.state != null) {
                out.writeBoolean(true);
                encode(this.state, out);
            } else {
                out.writeBoolean(false);
            }
        } else {
            DoubleHistogram state = this.state != null ? this.state
                : out.getTransportVersion().onOrAfter(TransportVersion.V_8_7_0) ? EMPTY_HISTOGRAM_ZERO_DIGITS
                : EMPTY_HISTOGRAM_THREE_DIGITS;
            encode(state, out);
        }
        out.writeBoolean(keyed);
    }

    private static void encode(DoubleHistogram state, StreamOutput out) throws IOException {
        out.writeLong(state.getHighestToLowestValueRatio());
        ByteBuffer stateBuffer = ByteBuffer.allocate(state.getNeededByteBufferCapacity());
        final int serializedLen = state.encodeIntoCompressedByteBuffer(stateBuffer);
        out.writeVInt(serializedLen);
        out.writeBytes(stateBuffer.array(), 0, serializedLen);
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

    public DocValueFormat formatter() {
        return format;
    }

    public abstract double value(double key);

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
            if (percentiles.state == null) {
                continue;
            }
            if (merged == null) {
                merged = new DoubleHistogram(percentiles.state);
                merged.setAutoResize(true);
            }
            merged = merge(merged, percentiles.state);
        }
        if (merged == null) {
            merged = EMPTY_HISTOGRAM_ZERO_DIGITS;
        }
        return createReduced(getName(), keys, merged, keyed, getMetadata());
    }

    /**
     * Merges two {@link DoubleHistogram}s such that we always merge the one with less
     * numberOfSignificantValueDigits into the one with more numberOfSignificantValueDigits.
     * This prevents producing a result that has lower than expected precision.
     *
     * @param histogram1 The first histogram to merge
     * @param histogram2 The second histogram to merge
     * @return One of the input histograms such that the one with higher numberOfSignificantValueDigits is used as the one for merging
     */
    private DoubleHistogram merge(final DoubleHistogram histogram1, final DoubleHistogram histogram2) {
        DoubleHistogram moreDigits = histogram1;
        DoubleHistogram lessDigits = histogram2;
        if (histogram2.getNumberOfSignificantValueDigits() > histogram1.getNumberOfSignificantValueDigits()) {
            moreDigits = histogram2;
            lessDigits = histogram1;
        }
        moreDigits.setAutoResize(true);
        moreDigits.add(lessDigits);
        return moreDigits;
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
        DoubleHistogram state = this.state != null ? this.state : EMPTY_HISTOGRAM_ZERO_DIGITS;
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
            state != null ? state.getIntegerToDoubleValueConversionRatio() : 0,
            state != null ? state.getTotalCount() : 0
        );
    }
}
