/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.search.aggregations.bucket.range;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.util.LongArray;
import org.elasticsearch.index.fielddata.FieldData;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.AggregationExecutionContext;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.CardinalityUpperBound;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.LeafBucketCollectorBase;
import org.elasticsearch.search.aggregations.bucket.BucketsAggregator;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValuesSource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

/** A range aggregator for values that are stored in SORTED_SET doc values. */
public final class BinaryRangeAggregator extends BucketsAggregator {

    public static class Range {

        final String key;
        final BytesRef from, to;

        public Range(String key, BytesRef from, BytesRef to) {
            this.key = key;
            this.from = from;
            this.to = to;
        }
    }

    static final Comparator<Range> RANGE_COMPARATOR = (a, b) -> {
        int cmp = compare(a.from, b.from, 1);
        if (cmp == 0) {
            cmp = compare(a.to, b.to, -1);
        }
        return cmp;
    };

    private static int compare(BytesRef a, BytesRef b, int m) {
        return a == null ? b == null ? 0 : -m : b == null ? m : a.compareTo(b);
    }

    final ValuesSource.Bytes valuesSource;
    final DocValueFormat format;
    final boolean keyed;
    final Range[] ranges;

    public BinaryRangeAggregator(
        String name,
        AggregatorFactories factories,
        ValuesSource valuesSource,
        DocValueFormat format,
        List<Range> ranges,
        boolean keyed,
        AggregationContext context,
        Aggregator parent,
        CardinalityUpperBound cardinality,
        Map<String, Object> metadata
    ) throws IOException {
        super(name, factories, context, parent, cardinality.multiply(ranges.size()), metadata);
        this.valuesSource = (ValuesSource.Bytes) valuesSource;
        this.format = format;
        this.keyed = keyed;
        this.ranges = ranges.toArray(new Range[0]);
        Arrays.sort(this.ranges, RANGE_COMPARATOR);

    }

    @Override
    public ScoreMode scoreMode() {
        if (valuesSource != null && valuesSource.needsScores()) {
            return ScoreMode.COMPLETE;
        }
        return super.scoreMode();
    }

    @FunctionalInterface
    private interface BucketCollector {
        void accept(LeafBucketCollector sub, int doc, long subBucketOrdinal) throws IOException;
    }

    @Override
    protected LeafBucketCollector getLeafCollector(AggregationExecutionContext aggCtx, LeafBucketCollector sub) throws IOException {
        if (valuesSource == null) {
            return LeafBucketCollector.NO_OP_COLLECTOR;
        }
        BucketCollector collector;
        if (parent() == null) {
            grow(ranges.length);
            collector = this::collectExistingBucket;
        } else {
            collector = this::collectBucket;
        }
        if (valuesSource instanceof ValuesSource.Bytes.WithOrdinals) {
            SortedSetDocValues values = ((ValuesSource.Bytes.WithOrdinals) valuesSource).ordinalsValues(aggCtx.getLeafReaderContext());
            return new SortedSetRangeLeafCollector(values, ranges, sub) {
                @Override
                protected void doCollect(LeafBucketCollector sub, int doc, long bucket) throws IOException {
                    collector.accept(sub, doc, bucket);
                }
            };
        } else {
            SortedBinaryDocValues values = valuesSource.bytesValues(aggCtx.getLeafReaderContext());
            return new SortedBinaryRangeLeafCollector(values, ranges, sub) {
                @Override
                protected void doCollect(LeafBucketCollector sub, int doc, long bucket) throws IOException {
                    collector.accept(sub, doc, bucket);
                }
            };
        }
    }

    abstract static class SortedSetRangeLeafCollector extends LeafBucketCollectorBase {

        private final long[] froms, tos, maxTos;
        private final DocCollector collector;
        private final LeafBucketCollector sub;

        SortedSetRangeLeafCollector(SortedSetDocValues values, Range[] ranges, LeafBucketCollector sub) throws IOException {
            super(sub, values);
            for (int i = 1; i < ranges.length; ++i) {
                if (RANGE_COMPARATOR.compare(ranges[i - 1], ranges[i]) > 0) {
                    throw new IllegalArgumentException("Ranges must be sorted");
                }
            }
            final SortedDocValues singleton = DocValues.unwrapSingleton(values);
            if (singleton != null) {
                this.collector = (doc, bucket) -> {
                    if (singleton.advanceExact(doc)) {
                        collect(doc, singleton.ordValue(), bucket, 0);
                    }
                };
            } else {
                this.collector = (doc, bucket) -> {
                    if (values.advanceExact(doc)) {
                        int lo = 0;
                        for (int i = 0; i < values.docValueCount(); i++) {
                            long ord = values.nextOrd();
                            lo = collect(doc, ord, bucket, lo);
                        }
                    }
                };
            }
            this.sub = sub;
            froms = new long[ranges.length];
            tos = new long[ranges.length]; // inclusive
            maxTos = new long[ranges.length];
            for (int i = 0; i < ranges.length; ++i) {
                if (ranges[i].from == null) {
                    froms[i] = 0;
                } else {
                    froms[i] = values.lookupTerm(ranges[i].from);
                    if (froms[i] < 0) {
                        froms[i] = -1 - froms[i];
                    }
                }
                if (ranges[i].to == null) {
                    tos[i] = values.getValueCount() - 1;
                } else {
                    long ord = values.lookupTerm(ranges[i].to);
                    if (ord < 0) {
                        tos[i] = -2 - ord;
                    } else {
                        tos[i] = ord - 1;
                    }
                }
            }
            maxTos[0] = tos[0];
            for (int i = 1; i < tos.length; ++i) {
                maxTos[i] = Math.max(maxTos[i - 1], tos[i]);
            }
        }

        @Override
        public void collect(int doc, long bucket) throws IOException {
            collector.collect(doc, bucket);
        }

        private int collect(int doc, long ord, long bucket, int lowBound) throws IOException {
            int lo = lowBound, hi = froms.length - 1; // all candidates are between these indexes
            int mid = (lo + hi) >>> 1;
            while (lo <= hi) {
                if (ord < froms[mid]) {
                    hi = mid - 1;
                } else if (ord > maxTos[mid]) {
                    lo = mid + 1;
                } else {
                    break;
                }
                mid = (lo + hi) >>> 1;
            }
            if (lo > hi) return lo; // no potential candidate

            // binary search the lower bound
            int startLo = lo, startHi = mid;
            while (startLo <= startHi) {
                final int startMid = (startLo + startHi) >>> 1;
                if (ord > maxTos[startMid]) {
                    startLo = startMid + 1;
                } else {
                    startHi = startMid - 1;
                }
            }

            // binary search the upper bound
            int endLo = mid, endHi = hi;
            while (endLo <= endHi) {
                final int endMid = (endLo + endHi) >>> 1;
                if (ord < froms[endMid]) {
                    endHi = endMid - 1;
                } else {
                    endLo = endMid + 1;
                }
            }

            assert startLo == lowBound || ord > maxTos[startLo - 1];
            assert endHi == froms.length - 1 || ord < froms[endHi + 1];

            for (int i = startLo; i <= endHi; ++i) {
                if (ord <= tos[i]) {
                    doCollect(sub, doc, bucket * froms.length + i);
                }
            }

            return endHi + 1;
        }

        protected abstract void doCollect(LeafBucketCollector sub, int doc, long bucket) throws IOException;
    }

    abstract static class SortedBinaryRangeLeafCollector extends LeafBucketCollectorBase {

        private final Range[] ranges;
        private final BytesRef[] maxTos;
        private final DocCollector collector;
        private final LeafBucketCollector sub;

        SortedBinaryRangeLeafCollector(SortedBinaryDocValues values, Range[] ranges, LeafBucketCollector sub) {
            super(sub, values);
            for (int i = 1; i < ranges.length; ++i) {
                if (RANGE_COMPARATOR.compare(ranges[i - 1], ranges[i]) > 0) {
                    throw new IllegalArgumentException("Ranges must be sorted");
                }
            }
            final BinaryDocValues singleton = FieldData.unwrapSingleton(values);
            if (singleton != null) {
                this.collector = (doc, bucket) -> {
                    if (singleton.advanceExact(doc)) {
                        collect(doc, singleton.binaryValue(), bucket, 0);
                    }
                };
            } else {
                this.collector = (doc, bucket) -> {
                    if (values.advanceExact(doc)) {
                        for (int i = 0, lo = 0; i < values.docValueCount(); ++i) {
                            lo = collect(doc, values.nextValue(), bucket, lo);
                        }
                    }
                };
            }
            this.sub = sub;
            this.ranges = ranges;
            maxTos = new BytesRef[ranges.length];
            if (ranges.length > 0) {
                maxTos[0] = ranges[0].to;
            }
            for (int i = 1; i < ranges.length; ++i) {
                if (compare(ranges[i].to, maxTos[i - 1], -1) >= 0) {
                    maxTos[i] = ranges[i].to;
                } else {
                    maxTos[i] = maxTos[i - 1];
                }
            }
        }

        @Override
        public void collect(int doc, long bucket) throws IOException {
            collector.collect(doc, bucket);
        }

        private int collect(int doc, BytesRef value, long bucket, int lowBound) throws IOException {
            int lo = lowBound, hi = ranges.length - 1; // all candidates are between these indexes
            int mid = (lo + hi) >>> 1;
            while (lo <= hi) {
                if (compare(value, ranges[mid].from, 1) < 0) {
                    hi = mid - 1;
                } else if (compare(value, maxTos[mid], -1) >= 0) {
                    lo = mid + 1;
                } else {
                    break;
                }
                mid = (lo + hi) >>> 1;
            }
            if (lo > hi) return lo; // no potential candidate

            // binary search the lower bound
            int startLo = lo, startHi = mid;
            while (startLo <= startHi) {
                final int startMid = (startLo + startHi) >>> 1;
                if (compare(value, maxTos[startMid], -1) >= 0) {
                    startLo = startMid + 1;
                } else {
                    startHi = startMid - 1;
                }
            }

            // binary search the upper bound
            int endLo = mid, endHi = hi;
            while (endLo <= endHi) {
                final int endMid = (endLo + endHi) >>> 1;
                if (compare(value, ranges[endMid].from, 1) < 0) {
                    endHi = endMid - 1;
                } else {
                    endLo = endMid + 1;
                }
            }

            assert startLo == lowBound || compare(value, maxTos[startLo - 1], -1) >= 0;
            assert endHi == ranges.length - 1 || compare(value, ranges[endHi + 1].from, 1) < 0;

            for (int i = startLo; i <= endHi; ++i) {
                if (compare(value, ranges[i].to, -1) < 0) {
                    doCollect(sub, doc, bucket * ranges.length + i);
                }
            }

            return endHi + 1;
        }

        protected abstract void doCollect(LeafBucketCollector sub, int doc, long bucket) throws IOException;
    }

    @FunctionalInterface
    private interface DocCollector {
        void collect(int doc, long bucket) throws IOException;
    }

    @Override
    public InternalAggregation[] buildAggregations(LongArray owningBucketOrds) throws IOException {
        return buildAggregationsForFixedBucketCount(
            owningBucketOrds,
            ranges.length,
            (offsetInOwningOrd, docCount, subAggregationResults) -> {
                Range range = ranges[offsetInOwningOrd];
                return new InternalBinaryRange.Bucket(format, range.key, range.from, range.to, docCount, subAggregationResults);
            },
            buckets -> new InternalBinaryRange(name, format, keyed, buckets, metadata())
        );
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        // Create empty buckets with 0 count and with empty sub-aggs so we can merge them with non-empty aggs
        InternalAggregations subAggs = buildEmptySubAggregations();
        List<InternalBinaryRange.Bucket> buckets = new ArrayList<>(ranges.length);
        for (Range range : ranges) {
            InternalBinaryRange.Bucket bucket = new InternalBinaryRange.Bucket(format, range.key, range.from, range.to, 0, subAggs);
            buckets.add(bucket);
        }
        return new InternalBinaryRange(name, format, keyed, buckets, metadata());
    }
}
