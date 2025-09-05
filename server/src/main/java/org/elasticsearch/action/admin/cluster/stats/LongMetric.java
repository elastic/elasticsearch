/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.cluster.stats;

import org.HdrHistogram.ConcurrentHistogram;
import org.HdrHistogram.Histogram;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.zip.DataFormatException;

/**
 * Metric class that accepts longs and provides count, average, max and percentiles.
 * Abstracts out the details of how exactly the values are stored and calculated.
 * {@link LongMetricValue} is a snapshot of the current state of the metric.
 */
public class LongMetric {
    private final Histogram values;
    private static final int SIGNIFICANT_DIGITS = 2;

    LongMetric() {
        values = new ConcurrentHistogram(SIGNIFICANT_DIGITS);
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
        // We have to carry the full histogram around since we might need to calculate aggregate percentiles
        // after collecting individual stats from the nodes, and we can't do that without having the full histogram.
        // This costs about 2K per metric, which was deemed acceptable.
        private final Histogram values;

        public LongMetricValue(Histogram values) {
            // Copy here since we don't want the snapshot value to change if somebody updates the original one
            this.values = values.copy();
        }

        public LongMetricValue(LongMetricValue v) {
            this.values = v.values.copy();
        }

        LongMetricValue() {
            this.values = new Histogram(SIGNIFICANT_DIGITS);
        }

        public void add(LongMetricValue v) {
            this.values.add(v.values);
        }

        public static LongMetricValue fromStream(StreamInput in) throws IOException {
            byte[] b = in.readByteArray();
            ByteBuffer bb = ByteBuffer.wrap(b);
            try {
                // TODO: not sure what is the good value for minBarForHighestToLowestValueRatio here?
                Histogram dh = Histogram.decodeFromCompressedByteBuffer(bb, 1);
                dh.setAutoResize(true);
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
            return values.getMaxValue();
        }

        public long avg() {
            return (long) Math.ceil(values.getMean());
        }

        public long p90() {
            return values.getValueAtPercentile(90);
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
