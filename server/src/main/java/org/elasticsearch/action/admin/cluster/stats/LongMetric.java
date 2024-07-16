/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.stats;

import org.HdrHistogram.DoubleHistogram;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.zip.DataFormatException;

/**
 * Metric class that accepts longs and provides count, average, max and maybe percentiles.
 * Abstracts out the details of how exactly the values are stored and calculated.
 * {@link LongMetricValue} is a snapshot of the current state of the metric.
 */
public class LongMetric {
    private final DoubleHistogram values;
    private static final int SIGNIFICANT_DIGITS = 2;

    LongMetric() {
        values = new DoubleHistogram(SIGNIFICANT_DIGITS);
    }

    void record(long v) {
        values.recordValue(v);
    }

    LongMetricValue getValue() {
        return new LongMetricValue(values);
    }

    /**
     * Snapshot of {@link LongMetric} value that provides the current state of the metric.
     * Can be added with another {@link LongMetricValue} object.
     */
    public static final class LongMetricValue implements Writeable {
        private final DoubleHistogram values;

        public LongMetricValue(DoubleHistogram values) {
            // Copy here since we don't want the snapshot value to change if somebody updates the original one
            this.values = values.copy();
        }

        LongMetricValue() {
            this.values = new DoubleHistogram(SIGNIFICANT_DIGITS);
        }

        public void add(LongMetricValue v) {
            this.values.add(v.values);
        }

        public static LongMetricValue fromStream(StreamInput in) throws IOException {
            byte[] b = in.readByteArray();
            ByteBuffer bb = ByteBuffer.wrap(b);
            try {
                // TODO: not sure what is the good value for minBarForHighestToLowestValueRatio here?
                DoubleHistogram dh = DoubleHistogram.decodeFromCompressedByteBuffer(bb, 1_000_000);
                return new LongMetricValue(dh);
            } catch (DataFormatException e) {
                throw new IOException(e);
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            ByteBuffer b = ByteBuffer.allocate(values.getNeededByteBufferCapacity());
            values.encodeIntoCompressedByteBuffer(b);
            int size = b.position();
            out.writeVInt(size);
            out.writeBytes(b.array(), 0, size);
        }

        public long count() {
            return values.getTotalCount();
        }

        public long max() {
            return (long) Math.ceil(values.getMaxValue());
        }

        public long avg() {
            return (long) Math.ceil(values.getMean());
        }

        public long p90() {
            return (long) Math.ceil(values.getValueAtPercentile(90));
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == this) return true;
            if (obj == null || obj.getClass() != this.getClass()) return false;
            var that = (LongMetricValue) obj;
            return this.values.equals(that.values);
        }

        @Override
        public int hashCode() {
            return Objects.hash(values);
        }

        @Override
        public String toString() {
            return "LongMetricValue[count=" + count() + ", " + "max=" + max() + ", " + "avg=" + avg() + "]";
        }

    }
}
