/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.xpack.exponentialhistogram.agg;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.AggregationReduceContext;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InvalidAggregationPathException;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.exponentialhistogram.otel.Base2ExponentialHistogramIndexer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.SortedMap;
import java.util.TreeMap;

public class InternalExponentialHistogram extends InternalAggregation {
    final DocValueFormat format;
    protected final int maxBuckets;
    protected int currentScale;
    Base2ExponentialHistogramIndexer indexer;
    private SortedMap<Integer, Long> positive;
    private SortedMap<Integer, Long> negative;
    private long totalCount;

    public InternalExponentialHistogram(
        String name,
        int maxBuckets,
        int maxScale,
        DocValueFormat formatter,
        Map<String, Object> metaData
    ) {
        super(name, metaData);
        this.format = formatter;
        this.maxBuckets = maxBuckets;
        this.currentScale = maxScale;
        this.indexer = Base2ExponentialHistogramIndexer.get(currentScale);
        this.positive = null;
        this.negative = null;
        this.totalCount = 0;
    }

    /**
     * Stream from a stream.
     */
    public InternalExponentialHistogram(StreamInput in) throws IOException {
        super(in);
        format = in.readNamedWriteable(DocValueFormat.class);
        maxBuckets = in.readVInt();
        currentScale = in.readVInt();
        positive = readCounts(in);
        negative = readCounts(in);
        indexer = Base2ExponentialHistogramIndexer.get(currentScale);
        totalCount =
            (positive != null ? positive.values().stream().reduce(0L, Long::sum) : 0L) +
            (negative != null ? negative.values().stream().reduce(0L, Long::sum) : 0L);
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeNamedWriteable(format);
        out.writeVInt(maxBuckets);
        out.writeVInt(currentScale);
        writeCounts(positive, out);
        writeCounts(negative, out);
    }

    private static void writeCounts(final SortedMap<Integer, Long> counts, final StreamOutput out) throws IOException {
        if (counts == null || counts.isEmpty()) {
            out.writeVInt(0);
            return;
        }
        out.writeVInt(counts.size());
        for (Map.Entry<Integer, Long> e : counts.entrySet()) {
            out.writeVInt(e.getKey());
            out.writeVLong(e.getValue());
        }
    }

    private static SortedMap<Integer, Long> readCounts(StreamInput in) throws IOException {
        final int size = in.readVInt();
        if (size == 0) {
            return null;
        }
        final SortedMap<Integer, Long> m = new TreeMap<>();
        for (int i = 0; i < size; i++) {
            int index = in.readVInt();
            long count = in.readVLong();
            m.put(index, count);
        }
        return m;
    }

    @Override
    public String getWriteableName() {
        return ExponentialHistogramAggregationBuilder.NAME;
    }

    public int getCurrentScale() {
        return currentScale;
    }

    public void add(final double value, final long count) {
        if (value == 0) {
            // TODO(axw)
            throw new RuntimeException("zero_count not implemented");
        }
        if (count == 0) {
            return;
        }
        SortedMap<Integer, Long> counts;
        if (value < 0) {
            if (negative == null) {
                negative = new TreeMap<>();
            }
            counts = negative;
        } else {
            if (positive == null) {
                positive = new TreeMap<>();
            }
            counts = positive;
        }

        final int index = indexer.computeIndex(Math.abs(value));
        final Long existing = counts.get(index);
        if (existing != null) {
            counts.put(index, existing+count);
        } else if (counts.size() < maxBuckets) {
            counts.put(index, count);
        } else {
            // NOTE(axw) maxBuckets is maintained independently for the positive and negative ranges
            // for simplicity. Having maxBuckets that applies to both ranges simultaneously is harder
            // to reason about when downsampling. e.g. consider what it would mean for a histogram with
            // maxBuckets that initially has only positive values, and then a negative value is recorded.
            downscale(getScaleReduction(index, counts));
            counts = (value < 0) ? negative : positive;
            final int newIndex = indexer.computeIndex(Math.abs(value));
            counts.put(newIndex,counts.getOrDefault(newIndex, 0L) + count);
        }
        totalCount += count;
    }

    private void downscale(final int scaleReduction) {
        if (scaleReduction <= 0) {
            throw new IllegalStateException("Cannot downscale by non-positive amount: " + scaleReduction);
        }
        negative = downscaleCounts(scaleReduction, negative);
        positive = downscaleCounts(scaleReduction, positive);
        currentScale -= scaleReduction;
        indexer = Base2ExponentialHistogramIndexer.get(currentScale);
    }

    private SortedMap<Integer, Long> downscaleCounts(final int scaleReduction, final SortedMap<Integer, Long> counts) {
        if (counts == null || counts.isEmpty()) {
            return counts;
        }
        SortedMap<Integer, Long> newCounts = new TreeMap<>();
        for (Map.Entry<Integer, Long> e : counts.entrySet()) {
            final long count = e.getValue();
            if (count > 0) {
                final int newIndex = e.getKey() >> scaleReduction;
                newCounts.put(newIndex, newCounts.getOrDefault(newIndex, 0L) + count);
            }
        }
        return newCounts;
    }

    private int getScaleReduction(final int index, final SortedMap<Integer, Long> counts) {
        long newStart = Math.min(index, counts.firstKey());
        long newEnd = Math.max(index, counts.lastKey());
        return getScaleReduction(newStart, newEnd);
    }

    private int getScaleReduction(long newStart, long newEnd) {
        int scaleReduction = 0;
        while ((newEnd - newStart + 1) > maxBuckets) {
            newStart >>= 1;
            newEnd >>= 1;
            scaleReduction++;
        }
        return scaleReduction;
    }

    public long getTotalCount() {
        return totalCount;
    }

    public List<Bucket> getBuckets() {
        List<Bucket> buckets = new ArrayList<>(
            (positive != null ? positive.size() : 0) + (negative != null ? negative.size() : 0)
        );
        if (negative != null) {
            for (Map.Entry<Integer, Long> e : negative.entrySet()) {
                buckets.add(new Bucket(currentScale, e.getKey(), true, e.getValue()));
            }
            Collections.reverse(buckets);
        }
        if (positive != null) {
            for (Map.Entry<Integer, Long> e : positive.entrySet()) {
                buckets.add(new Bucket(currentScale, e.getKey(), false, e.getValue()));
            }
        }
        return buckets;
    }

    @Override
    public InternalAggregation reduce(List<InternalAggregation> aggregations, AggregationReduceContext reduceContext) {
        //InternalExponentialHistogram first = (InternalExponentialHistogram)aggregations.get(0);
        throw new RuntimeException("not implemented");
    };

    @Override
    protected boolean mustReduceOnSingleInternalAgg() {
        return false;
    }

    @Override
    public Object getProperty(List<String> path) {
        if (path.isEmpty()) {
            return this;
        }
        final String aggName = path.get(0);
        throw new InvalidAggregationPathException("Cannot find an key [" + aggName + "] in [" + getName() + "]");
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        builder.field("scale", getCurrentScale());
        builder.startArray(CommonFields.BUCKETS.getPreferredName());
        for (Bucket bucket : getBuckets()) {
            builder.startObject()
                .field("lower_bound", bucket.getLowerBound())
                .field("upper_bound", bucket.getUpperBound())
                .field("count", bucket.getCount())
                .endObject();
        }
        builder.endArray();
        return builder;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        if (super.equals(obj) == false) return false;

        InternalExponentialHistogram that = (InternalExponentialHistogram) obj;
        return Objects.equals(maxBuckets, that.maxBuckets)
            && Objects.equals(currentScale, that.currentScale)
            && Objects.equals(positive, that.positive)
            && Objects.equals(negative, that.negative)
            && Objects.equals(format, that.format);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), maxBuckets, currentScale, positive, negative, format);
    }

    public static class Bucket {
        final double scaleBase;
        final boolean negative;
        final int index;
        final long count;

        Bucket(final int scale, final int index, final boolean negative, final long count) {
            this.scaleBase = Math.pow(2, Math.pow(2, -scale));
            this.negative = negative;
            this.index = index;
            this.count = count;
        }

        public int getIndex() {
            return index;
        }

        // lowerBound returns the lower bound of the exponential histogram bucket.
        //
        // For buckets in the positive range the lower bound is exclusive, while
        // in the negative range it is inclusive.
        public double getLowerBound() {
            if (negative) {
                return Math.pow(scaleBase, index + 1) * -1;
            }
            return Math.pow(scaleBase, index);
        }

        // upperBound returns the upper bound of the exponential histogram bucket.
        //
        // For buckets in the positive range the lower bound is inclusive, while
        // in the negative range it is exclusive.
        public double getUpperBound() {
            if (negative) {
                return Math.pow(scaleBase, index) * -1;
            }
            return Math.pow(scaleBase, index + 1);
        }

        public long getCount() {
            return count;
        }
    }
}
