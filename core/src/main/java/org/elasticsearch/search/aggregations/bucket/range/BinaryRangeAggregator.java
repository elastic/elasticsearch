/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.search.aggregations.bucket.range;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.LeafBucketCollectorBase;
import org.elasticsearch.search.aggregations.bucket.BucketsAggregator;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyList;

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
        return a == null
                ? b == null ? 0 : -m
                : b == null ? m : a.compareTo(b);
    }

    final ValuesSource.Bytes valuesSource;
    final DocValueFormat format;
    final boolean keyed;
    final Range[] ranges;

    public BinaryRangeAggregator(String name, AggregatorFactories factories,
            ValuesSource.Bytes valuesSource, DocValueFormat format,
            List<Range> ranges, boolean keyed, SearchContext context,
            Aggregator parent, List<PipelineAggregator> pipelineAggregators,
            Map<String, Object> metaData) throws IOException {
        super(name, factories, context, parent, pipelineAggregators, metaData);
        this.valuesSource = valuesSource;
        this.format = format;
        this.keyed = keyed;
        this.ranges = ranges.toArray(new Range[0]);
        Arrays.sort(this.ranges, RANGE_COMPARATOR);

    }

    @Override
    public boolean needsScores() {
        return (valuesSource != null && valuesSource.needsScores()) || super.needsScores();
    }

    @Override
    protected LeafBucketCollector getLeafCollector(LeafReaderContext ctx, LeafBucketCollector sub) throws IOException {
        if (valuesSource == null) {
            return LeafBucketCollector.NO_OP_COLLECTOR;
        }
        if (valuesSource instanceof ValuesSource.Bytes.WithOrdinals) {
            SortedSetDocValues values = ((ValuesSource.Bytes.WithOrdinals) valuesSource).ordinalsValues(ctx);
            return new SortedSetRangeLeafCollector(values, ranges, sub) {
                @Override
                protected void doCollect(LeafBucketCollector sub, int doc, long bucket) throws IOException {
                    collectBucket(sub, doc, bucket);
                }
            };
        } else {
            SortedBinaryDocValues values = valuesSource.bytesValues(ctx);
            return new SortedBinaryRangeLeafCollector(values, ranges, sub) {
                @Override
                protected void doCollect(LeafBucketCollector sub, int doc, long bucket)
                        throws IOException {
                    collectBucket(sub, doc, bucket);
                }
            };
        }
    }

    abstract static class SortedSetRangeLeafCollector extends LeafBucketCollectorBase {

        final long[] froms, tos, maxTos;
        final SortedSetDocValues values;
        final LeafBucketCollector sub;

        SortedSetRangeLeafCollector(SortedSetDocValues values,
                Range[] ranges, LeafBucketCollector sub) throws IOException {
            super(sub, values);
            for (int i = 1; i < ranges.length; ++i) {
                if (RANGE_COMPARATOR.compare(ranges[i-1], ranges[i]) > 0) {
                    throw new IllegalArgumentException("Ranges must be sorted");
                }
            }
            this.values = values;
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
                maxTos[i] = Math.max(maxTos[i-1], tos[i]);
            }
        }

        @Override
        public void collect(int doc, long bucket) throws IOException {
            if (values.advanceExact(doc)) {
                int lo = 0;
                for (long ord = values
                        .nextOrd(); ord != SortedSetDocValues.NO_MORE_ORDS; ord = values
                                .nextOrd()) {
                    lo = collect(doc, ord, bucket, lo);
                }
            }
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

        final Range[] ranges;
        final BytesRef[] maxTos;
        final SortedBinaryDocValues values;
        final LeafBucketCollector sub;

        SortedBinaryRangeLeafCollector(SortedBinaryDocValues values,
                Range[] ranges, LeafBucketCollector sub) {
            super(sub, values);
            for (int i = 1; i < ranges.length; ++i) {
                if (RANGE_COMPARATOR.compare(ranges[i-1], ranges[i]) > 0) {
                    throw new IllegalArgumentException("Ranges must be sorted");
                }
            }
            this.values = values;
            this.sub = sub;
            this.ranges = ranges;
            maxTos = new BytesRef[ranges.length];
            if (ranges.length > 0) {
                maxTos[0] = ranges[0].to;
            }
            for (int i = 1; i < ranges.length; ++i) {
                if (compare(ranges[i].to, maxTos[i-1], -1) >= 0) {
                    maxTos[i] = ranges[i].to;
                } else {
                    maxTos[i] = maxTos[i-1];
                }
            }
        }

        @Override
        public void collect(int doc, long bucket) throws IOException {
            if (values.advanceExact(doc)) {
                final int valuesCount = values.docValueCount();
                for (int i = 0, lo = 0; i < valuesCount; ++i) {
                    final BytesRef value = values.nextValue();
                    lo = collect(doc, value, bucket, lo);
                }
            }
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

    @Override
    public InternalAggregation buildAggregation(long bucket) throws IOException {
        List<InternalBinaryRange.Bucket> buckets = new ArrayList<>(ranges.length);
        for (int i = 0; i < ranges.length; ++i) {
            long bucketOrd = bucket * ranges.length + i;
            buckets.add(new InternalBinaryRange.Bucket(format, keyed,
                    ranges[i].key, ranges[i].from, ranges[i].to,
                    bucketDocCount(bucketOrd), bucketAggregations(bucketOrd)));
        }
        return new InternalBinaryRange(name, format, keyed, buckets, pipelineAggregators(), metaData());
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return new InternalBinaryRange(name, format, keyed, emptyList(), pipelineAggregators(), metaData());
    }
}
