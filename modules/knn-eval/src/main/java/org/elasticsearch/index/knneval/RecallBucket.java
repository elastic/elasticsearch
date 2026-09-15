/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.knneval;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * One histogram entry. When binned, {@code recall} is the lower edge of a half-open bin -- the {@code histogram} aggregation's
 * {@code key} convention -- and 1.0 is always exact.
 */
public record RecallBucket(double recall, long count) implements Writeable, ToXContentObject {

    static final ParseField RECALL_FIELD = new ParseField("recall");
    static final ParseField COUNT_FIELD = new ParseField("count");

    /** Enough precision to keep 1/k apart for any usable k, and to merge floating point noise. */
    private static final double GROUPING_SCALE = 10_000.0;

    /** Per-query recall is a multiple of {@code 1/relevant}, so at this {@code k} there are at most 21 distinct values. */
    static final int MAX_EXACT_K = 20;

    /** Twenty bins plus an exact entry for 1.0 is again 21 entries. */
    static final double BIN_WIDTH = 0.05;

    private static final int BINS = (int) Math.round(1.0 / BIN_WIDTH);

    static boolean isBinned(int k) {
        return k > MAX_EXACT_K;
    }

    /**
     * Groups the per-query values, ascending, with no empty entries. Past {@link #MAX_EXACT_K} the values are dense enough that the
     * exact form stops being a summary, so each entry becomes a bin's lower edge -- except 1.0, which stays exact so that "perfect" is
     * never merged with "nearly perfect".
     */
    public static List<RecallBucket> histogram(List<Double> values, int k) {
        SortedMap<Double, Long> counts = new TreeMap<>();
        for (double value : values) {
            counts.merge(isBinned(k) ? lowerEdgeOf(value) : round(value), 1L, Long::sum);
        }
        List<RecallBucket> buckets = new ArrayList<>(counts.size());
        for (Map.Entry<Double, Long> count : counts.entrySet()) {
            buckets.add(new RecallBucket(count.getKey(), count.getValue()));
        }
        return buckets;
    }

    /** The lower edge of the bin a value falls in, or 1.0 for an exact match. */
    private static double lowerEdgeOf(double value) {
        if (value >= 1.0) {
            return 1.0;
        }
        // the epsilon keeps a value that should sit on an edge (0.15 arriving as 0.1499999...) out of the bin below
        int bin = Math.min(BINS - 1, (int) Math.floor(value / BIN_WIDTH + 1e-9));
        return round(bin * BIN_WIDTH);
    }

    /** So that {@code 0.30000000000000004} and {@code 0.3} land in one bucket. */
    private static double round(double value) {
        return Math.round(value * GROUPING_SCALE) / GROUPING_SCALE;
    }

    RecallBucket(StreamInput in) throws IOException {
        this(in.readDouble(), in.readVLong());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeDouble(recall);
        out.writeVLong(count);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(RECALL_FIELD.getPreferredName(), recall);
        builder.field(COUNT_FIELD.getPreferredName(), count);
        builder.endObject();
        return builder;
    }
}
