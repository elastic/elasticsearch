/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.timeseries;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.compute.lucene.query.TsidPrefixRange;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * Series-milliseconds per {@code _tsid} prefix cell.
 */
public final class TsidPrefixHistogram implements Writeable {
    public static final int MAX_CELLS = 4096;

    public static final TsidPrefixHistogram EMPTY = new TsidPrefixHistogram(
        0,
        new int[0],
        new double[0],
        0,
        Long.MAX_VALUE,
        Long.MIN_VALUE,
        0
    );

    private final int prefixBits;
    private final int[] prefixes;
    private final double[] seriesMillis;
    private final int numPrefixes;
    private final long minTimestamp;
    private final long maxTimestamp;
    private final long docCount;

    public TsidPrefixHistogram(
        int prefixBits,
        int[] prefixes,
        double[] seriesMillis,
        int numPrefixes,
        long minTimestamp,
        long maxTimestamp,
        long docCount
    ) {
        assert prefixes.length == seriesMillis.length : prefixes.length + " != " + seriesMillis.length;
        assert numPrefixes >= 0 && numPrefixes <= prefixes.length : "invalid cell count: " + numPrefixes;
        this.prefixBits = prefixBits;
        this.prefixes = prefixes;
        this.numPrefixes = numPrefixes;
        this.seriesMillis = seriesMillis;
        this.minTimestamp = minTimestamp;
        this.maxTimestamp = maxTimestamp;
        this.docCount = docCount;
    }

    TsidPrefixHistogram(StreamInput in) throws IOException {
        prefixBits = in.readVInt();
        numPrefixes = in.readVInt();
        prefixes = new int[numPrefixes];
        for (int i = 0; i < numPrefixes; i++) {
            prefixes[i] = in.readInt();
        }
        seriesMillis = new double[numPrefixes];
        for (int i = 0; i < numPrefixes; i++) {
            seriesMillis[i] = in.readDouble();
        }
        minTimestamp = in.readLong();
        maxTimestamp = in.readLong();
        docCount = in.readVLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(prefixBits);
        out.writeVInt(numPrefixes);
        for (int i = 0; i < numPrefixes; i++) {
            out.writeInt(prefixes[i]);
        }
        for (int i = 0; i < numPrefixes; i++) {
            out.writeDouble(seriesMillis[i]);
        }
        out.writeLong(minTimestamp);
        out.writeLong(maxTimestamp);
        out.writeVLong(docCount);
    }

    int totalPrefixes() {
        return numPrefixes;
    }

    int prefix(int cell) {
        return prefixes[Objects.checkIndex(cell, numPrefixes)];
    }

    double seriesMillis(int cell) {
        return seriesMillis[Objects.checkIndex(cell, numPrefixes)];
    }

    long minTimestamp() {
        return minTimestamp;
    }

    long maxTimestamp() {
        return maxTimestamp;
    }

    double totalSeriesMillis() {
        return Arrays.stream(seriesMillis, 0, numPrefixes).sum();
    }

    long estimatedSeries() {
        return estimatedSeries(totalSeriesMillis());
    }

    /**
     * The estimated number of {@code (_tsid, time bucket)} groups
     */
    public long estimatedGroups(long bucketMillis) {
        final double total = totalSeriesMillis();
        final long series = estimatedSeries(total);
        return bucketMillis > 0 ? Math.min(docCount, (long) (Math.ceil(total / bucketMillis) + 2.0 * series)) : series;
    }

    private long estimatedSeries(double total) {
        return numPrefixes == 0 ? 0 : (long) Math.ceil(total / timeSpanMillis(minTimestamp, maxTimestamp));
    }

    static double timeSpanMillis(long minTimestamp, long maxTimestamp) {
        long difference = maxTimestamp - minTimestamp;
        return (difference < 0 ? (double) maxTimestamp - minTimestamp : difference) + 1;
    }

    /**
     * Drops the fewest low prefix bits that leave at most {@code maxCells} cells. Node, cluster, and final responses are
     * capped at {@link #MAX_CELLS} to keep their memory and transport size small: without a cap, the 18-bit prefix allows
     * up to 2^18 cells, about 3MB per response. Capping merges adjacent cells, so the series and group estimates do not
     * change, and {@link #splits} can only cut at coarser prefix boundaries. The planner asks for at most 256 splits, so
     * {@link #MAX_CELLS} cells still leave enough prefixes to split good partitions.
     */
    TsidPrefixHistogram capped(int maxCells) {
        if (numPrefixes <= maxCells) {
            return this;
        }
        final int[] mergedAtShift = new int[Integer.SIZE + 1];
        for (int i = 1; i < numPrefixes; i++) {
            // The bit length of the XOR is the first shift at which this pair becomes one cell.
            mergedAtShift[Integer.SIZE - Integer.numberOfLeadingZeros(prefixes[i - 1] ^ prefixes[i])]++;
        }
        int cells = numPrefixes;
        int shift = 0;
        while (cells > maxCells && shift < prefixBits) {
            shift++;
            cells -= mergedAtShift[shift];
        }

        return coarsen(prefixBits - shift, cells);
    }

    private TsidPrefixHistogram coarsen(int newPrefixBits, int capacity) {
        if (newPrefixBits == prefixBits) {
            return this;
        }
        final int shift = prefixBits - newPrefixBits;
        final int[] newPrefixes = new int[capacity];
        final double[] newSeriesMillis = new double[capacity];
        int size = 0;
        for (int i = 0; i < numPrefixes; i++) {
            final int prefix = prefixes[i] >>> shift;
            if (size == 0 || newPrefixes[size - 1] != prefix) {
                newPrefixes[size++] = prefix;
            }
            newSeriesMillis[size - 1] += seriesMillis[i];
        }
        return new TsidPrefixHistogram(newPrefixBits, newPrefixes, newSeriesMillis, size, minTimestamp, maxTimestamp, docCount);
    }

    /**
     * Combines the cells of both histograms at the coarser of their prefix widths.
     */
    static TsidPrefixHistogram merge(TsidPrefixHistogram left, TsidPrefixHistogram right) {
        if (right.numPrefixes == 0) {
            return left;
        }
        if (left.numPrefixes == 0) {
            return right;
        }
        final int minPrefixBits = Math.min(left.prefixBits, right.prefixBits);
        final TsidPrefixHistogram a = left.coarsen(minPrefixBits, left.numPrefixes);
        final TsidPrefixHistogram b = right.coarsen(minPrefixBits, right.numPrefixes);
        final int[] newPrefixes = new int[a.numPrefixes + b.numPrefixes];
        final double[] newSeriesMillis = new double[newPrefixes.length];
        int size = 0;
        int i = 0;
        int j = 0;
        while (i < a.numPrefixes && j < b.numPrefixes) {
            final int prefix = Math.min(a.prefixes[i], b.prefixes[j]);
            newPrefixes[size] = prefix;
            if (a.prefixes[i] == prefix) {
                newSeriesMillis[size] += a.seriesMillis[i++];
            }
            if (b.prefixes[j] == prefix) {
                newSeriesMillis[size] += b.seriesMillis[j++];
            }
            size++;
        }
        while (i < a.numPrefixes) {
            newPrefixes[size] = a.prefixes[i];
            newSeriesMillis[size++] += a.seriesMillis[i++];
        }
        while (j < b.numPrefixes) {
            newPrefixes[size] = b.prefixes[j];
            newSeriesMillis[size++] += b.seriesMillis[j++];
        }
        return new TsidPrefixHistogram(
            minPrefixBits,
            newPrefixes,
            newSeriesMillis,
            size,
            Math.min(left.minTimestamp, right.minTimestamp),
            Math.max(left.maxTimestamp, right.maxTimestamp),
            left.docCount + Math.min(Long.MAX_VALUE - left.docCount, right.docCount)
        );
    }

    /**
     * Splits the prefix space into at most {@code numSplits} contiguous ranges of about equal series-milliseconds
     */
    public List<TsidPrefixRange> splits(int numSplits) {
        final double total = totalSeriesMillis();
        if (total == 0 || numSplits <= 1) {
            return List.of();
        }
        final List<TsidPrefixRange> ranges = new ArrayList<>();
        int lower = 0;
        int next = 1;
        double cumulative = 0;
        for (int i = 0; i < numPrefixes - 1 && next < numSplits; i++) {
            cumulative += seriesMillis[i];
            if (cumulative * numSplits >= total * next) {
                ranges.add(new TsidPrefixRange(prefixBits, lower, prefixes[i + 1]));
                lower = prefixes[i + 1];
                do {
                    next++;
                } while (next < numSplits && cumulative * numSplits >= total * next);
            }
        }
        if (ranges.isEmpty()) {
            return List.of();
        }
        ranges.add(new TsidPrefixRange(prefixBits, lower, 1 << prefixBits));
        return ranges;
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof TsidPrefixHistogram that
            && prefixBits == that.prefixBits
            && numPrefixes == that.numPrefixes
            && minTimestamp == that.minTimestamp
            && maxTimestamp == that.maxTimestamp
            && docCount == that.docCount
            && Arrays.equals(prefixes, 0, numPrefixes, that.prefixes, 0, numPrefixes)
            && Arrays.equals(seriesMillis, 0, numPrefixes, that.seriesMillis, 0, numPrefixes);
    }

    @Override
    public int hashCode() {
        int result = 31 * prefixBits + numPrefixes;
        for (int i = 0; i < numPrefixes; i++) {
            result = 31 * result + Integer.hashCode(prefixes[i]);
            result = 31 * result + Double.hashCode(seriesMillis[i]);
        }
        result = 31 * result + Long.hashCode(minTimestamp);
        result = 31 * result + Long.hashCode(maxTimestamp);
        return 31 * result + Long.hashCode(docCount);
    }

    @Override
    public String toString() {
        return "TsidPrefixHistogram[prefixBits="
            + prefixBits
            + ", cells="
            + numPrefixes
            + ", series="
            + estimatedSeries()
            + ", time="
            + minTimestamp
            + ".."
            + maxTimestamp
            + "]";
    }
}
