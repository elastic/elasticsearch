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

import com.google.common.collect.Lists;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.util.InPlaceMergeSorter;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;
import org.elasticsearch.search.aggregations.*;
import org.elasticsearch.search.aggregations.bucket.BucketsAggregator;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.aggregations.support.format.ValueFormat;
import org.elasticsearch.search.aggregations.support.format.ValueFormatter;
import org.elasticsearch.search.aggregations.support.format.ValueParser;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 *
 */
public class RangeAggregator extends BucketsAggregator {

    public static class Range {

        public String key;
        public double from = Double.NEGATIVE_INFINITY;
        String fromAsStr;
        public double to = Double.POSITIVE_INFINITY;
        String toAsStr;

        public Range(String key, double from, String fromAsStr, double to, String toAsStr) {
            this.key = key;
            this.from = from;
            this.fromAsStr = fromAsStr;
            this.to = to;
            this.toAsStr = toAsStr;
        }

        boolean matches(double value) {
            return value >= from && value < to;
        }

        @Override
        public String toString() {
            return "[" + from + " to " + to + ")";
        }

        public void process(ValueParser parser, SearchContext context) {
            assert parser != null;
            if (fromAsStr != null) {
                from = parser.parseDouble(fromAsStr, context);
            }
            if (toAsStr != null) {
                to = parser.parseDouble(toAsStr, context);
            }
        }
    }

    private final ValuesSource.Numeric valuesSource;
    private final @Nullable ValueFormatter formatter;
    private final Range[] ranges;
    private final boolean keyed;
    private final InternalRange.Factory rangeFactory;
    private SortedNumericDoubleValues values;

    final double[] maxTo;

    public RangeAggregator(String name,
                           AggregatorFactories factories,
                           ValuesSource.Numeric valuesSource,
                           @Nullable ValueFormat format,
                           InternalRange.Factory rangeFactory,
                           List<Range> ranges,
                           boolean keyed,
                           AggregationContext aggregationContext,
                           Aggregator parent) {

        super(name, BucketAggregationMode.MULTI_BUCKETS, factories, ranges.size() * (parent == null ? 1 : parent.estimatedBucketCount()), aggregationContext, parent);
        assert valuesSource != null;
        this.valuesSource = valuesSource;
        this.formatter = format != null ? format.formatter() : null;
        this.keyed = keyed;
        this.rangeFactory = rangeFactory;
        this.ranges = ranges.toArray(new Range[ranges.size()]);

        ValueParser parser = format != null ? format.parser() : ValueParser.RAW;
        for (int i = 0; i < this.ranges.length; i++) {
            this.ranges[i].process(parser, context.searchContext());
        }
        sortRanges(this.ranges);

        maxTo = new double[this.ranges.length];
        maxTo[0] = this.ranges[0].to;
        for (int i = 1; i < this.ranges.length; ++i) {
            maxTo[i] = Math.max(this.ranges[i].to,maxTo[i-1]);
        }

    }

    @Override
    public boolean shouldCollect() {
        return true;
    }

    @Override
    public void setNextReader(AtomicReaderContext reader) {
        values = valuesSource.doubleValues();
    }

    private final long subBucketOrdinal(long owningBucketOrdinal, int rangeOrd) {
        return owningBucketOrdinal * ranges.length + rangeOrd;
    }

    @Override
    public void collect(int doc, long owningBucketOrdinal) throws IOException {
        values.setDocument(doc);
        final int valuesCount = values.count();
        for (int i = 0, lo = 0; i < valuesCount; ++i) {
            final double value = values.valueAt(i);
            lo = collect(doc, value, owningBucketOrdinal, lo);
        }
    }

    private int collect(int doc, double value, long owningBucketOrdinal, int lowBound) throws IOException {
        int lo = lowBound, hi = ranges.length - 1; // all candidates are between these indexes
        int mid = (lo + hi) >>> 1;
        while (lo <= hi) {
            if (value < ranges[mid].from) {
                hi = mid - 1;
            } else if (value >= maxTo[mid]) {
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
            if (value >= maxTo[startMid]) {
                startLo = startMid + 1;
            } else {
                startHi = startMid - 1;
            }
        }

        // binary search the upper bound
        int endLo = mid, endHi = hi;
        while (endLo <= endHi) {
            final int endMid = (endLo + endHi) >>> 1;
            if (value < ranges[endMid].from) {
                endHi = endMid - 1;
            } else {
                endLo = endMid + 1;
            }
        }

        assert startLo == lowBound || value >= maxTo[startLo - 1];
        assert endHi == ranges.length - 1 || value < ranges[endHi + 1].from;

        for (int i = startLo; i <= endHi; ++i) {
            if (ranges[i].matches(value)) {
                collectBucket(doc, subBucketOrdinal(owningBucketOrdinal, i));
            }
        }

        return endHi + 1;
    }

    @Override
    public InternalAggregation buildAggregation(long owningBucketOrdinal) {
        List<org.elasticsearch.search.aggregations.bucket.range.Range.Bucket> buckets = Lists.newArrayListWithCapacity(ranges.length);
        for (int i = 0; i < ranges.length; i++) {
            Range range = ranges[i];
            final long bucketOrd = subBucketOrdinal(owningBucketOrdinal, i);
            org.elasticsearch.search.aggregations.bucket.range.Range.Bucket bucket =
                    rangeFactory.createBucket(range.key, range.from, range.to, bucketDocCount(bucketOrd),bucketAggregations(bucketOrd), formatter);
            buckets.add(bucket);
        }
        // value source can be null in the case of unmapped fields
        return rangeFactory.create(name, buckets, formatter, keyed);
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        InternalAggregations subAggs = buildEmptySubAggregations();
        List<org.elasticsearch.search.aggregations.bucket.range.Range.Bucket> buckets = Lists.newArrayListWithCapacity(ranges.length);
        for (int i = 0; i < ranges.length; i++) {
            Range range = ranges[i];
            org.elasticsearch.search.aggregations.bucket.range.Range.Bucket bucket =
                    rangeFactory.createBucket(range.key, range.from, range.to, 0, subAggs, formatter);
            buckets.add(bucket);
        }
        // value source can be null in the case of unmapped fields
        return rangeFactory.create(name, buckets, formatter, keyed);
    }

    private static final void sortRanges(final Range[] ranges) {
        new InPlaceMergeSorter() {

            @Override
            protected void swap(int i, int j) {
                final Range tmp = ranges[i];
                ranges[i] = ranges[j];
                ranges[j] = tmp;
            }

            @Override
            protected int compare(int i, int j) {
                int cmp = Double.compare(ranges[i].from, ranges[j].from);
                if (cmp == 0) {
                    cmp = Double.compare(ranges[i].to, ranges[j].to);
                }
                return cmp;
            }
        }.sort(0, ranges.length);
    }

    public static class Unmapped extends NonCollectingAggregator {

        private final List<RangeAggregator.Range> ranges;
        private final boolean keyed;
        private final InternalRange.Factory factory;
        private final ValueFormatter formatter;

        public Unmapped(String name,
                        List<RangeAggregator.Range> ranges,
                        boolean keyed,
                        ValueFormat format,
                        AggregationContext context,
                        Aggregator parent,
                        InternalRange.Factory factory) {

            super(name, context, parent);
            this.ranges = ranges;
            ValueParser parser = format != null ? format.parser() : ValueParser.RAW;
            for (Range range : this.ranges) {
                range.process(parser, context.searchContext());
            }
            this.keyed = keyed;
            this.formatter = format != null ? format.formatter() : null;
            this.factory = factory;
        }

        @Override
        public InternalAggregation buildEmptyAggregation() {
            InternalAggregations subAggs = buildEmptySubAggregations();
            List<org.elasticsearch.search.aggregations.bucket.range.Range.Bucket> buckets = new ArrayList<>(ranges.size());
            for (RangeAggregator.Range range : ranges) {
                buckets.add(factory.createBucket(range.key, range.from, range.to, 0, subAggs, formatter));
            }
            return factory.create(name, buckets, formatter, keyed);
        }
    }

    public static class Factory extends ValuesSourceAggregatorFactory<ValuesSource.Numeric> {

        private final InternalRange.Factory rangeFactory;
        private final List<Range> ranges;
        private final boolean keyed;

        public Factory(String name, ValuesSourceConfig<ValuesSource.Numeric> valueSourceConfig, InternalRange.Factory rangeFactory, List<Range> ranges, boolean keyed) {
            super(name, rangeFactory.type(), valueSourceConfig);
            this.rangeFactory = rangeFactory;
            this.ranges = ranges;
            this.keyed = keyed;
        }

        @Override
        protected Aggregator createUnmapped(AggregationContext aggregationContext, Aggregator parent) {
            return new Unmapped(name, ranges, keyed, config.format(), aggregationContext, parent, rangeFactory);
        }

        @Override
        protected Aggregator create(ValuesSource.Numeric valuesSource, long expectedBucketsCount, AggregationContext aggregationContext, Aggregator parent) {
            return new RangeAggregator(name, factories, valuesSource, config.format(), rangeFactory, ranges, keyed, aggregationContext, parent);
        }
    }

}
