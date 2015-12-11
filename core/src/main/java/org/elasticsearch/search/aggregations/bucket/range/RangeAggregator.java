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
import org.apache.lucene.util.InPlaceMergeSorter;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.LeafBucketCollectorBase;
import org.elasticsearch.search.aggregations.NonCollectingAggregator;
import org.elasticsearch.search.aggregations.bucket.BucketsAggregator;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValueType;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSource.Numeric;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;
import org.elasticsearch.search.aggregations.support.format.ValueFormat;
import org.elasticsearch.search.aggregations.support.format.ValueFormatter;
import org.elasticsearch.search.aggregations.support.format.ValueParser;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 *
 */
public class RangeAggregator extends BucketsAggregator {

    public static final ParseField RANGES_FIELD = new ParseField("ranges");
    public static final ParseField KEYED_FIELD = new ParseField("keyed");

    public static class Range implements Writeable<Range>, ToXContent {

        public static final Range PROTOTYPE = new Range(null, -1, null, -1, null);
        public static final ParseField KEY_FIELD = new ParseField("key");
        public static final ParseField FROM_FIELD = new ParseField("from");
        public static final ParseField TO_FIELD = new ParseField("to");

        protected String key;
        protected double from = Double.NEGATIVE_INFINITY;
        protected String fromAsStr;
        protected double to = Double.POSITIVE_INFINITY;
        protected String toAsStr;

        public Range(String key, double from, double to) {
            this(key, from, null, to, null);
        }

        public Range(String key, String from, String to) {
            this(key, Double.NEGATIVE_INFINITY, from, Double.POSITIVE_INFINITY, to);
        }

        protected Range(String key, double from, String fromAsStr, double to, String toAsStr) {
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

        @Override
        public Range readFrom(StreamInput in) throws IOException {
            String key = in.readOptionalString();
            String fromAsStr = in.readOptionalString();
            String toAsStr = in.readOptionalString();
            double from = in.readDouble();
            double to = in.readDouble();
            return new Range(key, from, fromAsStr, to, toAsStr);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeOptionalString(key);
            out.writeOptionalString(fromAsStr);
            out.writeOptionalString(toAsStr);
            out.writeDouble(from);
            out.writeDouble(to);
        }

        public Range fromXContent(XContentParser parser, ParseFieldMatcher parseFieldMatcher) throws IOException {

            XContentParser.Token token;
            String currentFieldName = null;
            double from = Double.NEGATIVE_INFINITY;
            String fromAsStr = null;
            double to = Double.POSITIVE_INFINITY;
            String toAsStr = null;
            String key = null;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token == XContentParser.Token.VALUE_NUMBER) {
                    if (parseFieldMatcher.match(currentFieldName, FROM_FIELD)) {
                        from = parser.doubleValue();
                    } else if (parseFieldMatcher.match(currentFieldName, TO_FIELD)) {
                        to = parser.doubleValue();
                    }
                } else if (token == XContentParser.Token.VALUE_STRING) {
                    if (parseFieldMatcher.match(currentFieldName, FROM_FIELD)) {
                        fromAsStr = parser.text();
                    } else if (parseFieldMatcher.match(currentFieldName, TO_FIELD)) {
                        toAsStr = parser.text();
                    } else if (parseFieldMatcher.match(currentFieldName, KEY_FIELD)) {
                        key = parser.text();
                    }
                }
            }
            return new Range(key, from, fromAsStr, to, toAsStr);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            if (key != null) {
                builder.field(KEY_FIELD.getPreferredName(), key);
            }
            if (Double.isFinite(from)) {
                builder.field(FROM_FIELD.getPreferredName(), from);
            }
            if (Double.isFinite(to)) {
                builder.field(TO_FIELD.getPreferredName(), to);
            }
            if (fromAsStr != null) {
                builder.field(FROM_FIELD.getPreferredName(), fromAsStr);
            }
            if (toAsStr != null) {
                builder.field(TO_FIELD.getPreferredName(), toAsStr);
            }
            builder.endObject();
            return builder;
        }

        @Override
        public int hashCode() {
            return Objects.hash(key, from, fromAsStr, to, toAsStr);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            Range other = (Range) obj;
            return Objects.equals(key, other.key)
                    && Objects.equals(from, other.from)
                    && Objects.equals(fromAsStr, other.fromAsStr)
                    && Objects.equals(to, other.to)
                    && Objects.equals(toAsStr, other.toAsStr);
        }
    }

    final ValuesSource.Numeric valuesSource;
    final ValueFormatter formatter;
    final Range[] ranges;
    final boolean keyed;
    final InternalRange.Factory rangeFactory;

    final double[] maxTo;

    public RangeAggregator(String name, AggregatorFactories factories, ValuesSource.Numeric valuesSource, ValueFormat format,
            InternalRange.Factory rangeFactory, List<? extends Range> ranges, boolean keyed, AggregationContext aggregationContext,
            Aggregator parent, List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData) throws IOException {

        super(name, factories, aggregationContext, parent, pipelineAggregators, metaData);
        assert valuesSource != null;
        this.valuesSource = valuesSource;
        this.formatter = format.formatter();
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
    public boolean needsScores() {
        return (valuesSource != null && valuesSource.needsScores()) || super.needsScores();
    }

    @Override
    public LeafBucketCollector getLeafCollector(LeafReaderContext ctx,
            final LeafBucketCollector sub) throws IOException {
        final SortedNumericDoubleValues values = valuesSource.doubleValues(ctx);
        return new LeafBucketCollectorBase(sub, values) {
            @Override
            public void collect(int doc, long bucket) throws IOException {
                values.setDocument(doc);
                final int valuesCount = values.count();
                for (int i = 0, lo = 0; i < valuesCount; ++i) {
                    final double value = values.valueAt(i);
                    lo = collect(doc, value, bucket, lo);
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
                        collectBucket(sub, doc, subBucketOrdinal(owningBucketOrdinal, i));
            }
        }

        return endHi + 1;
    }
        };
    }

    private final long subBucketOrdinal(long owningBucketOrdinal, int rangeOrd) {
        return owningBucketOrdinal * ranges.length + rangeOrd;
    }

    @Override
    public InternalAggregation buildAggregation(long owningBucketOrdinal) throws IOException {
        List<org.elasticsearch.search.aggregations.bucket.range.Range.Bucket> buckets = new ArrayList<>(ranges.length);
        for (int i = 0; i < ranges.length; i++) {
            Range range = ranges[i];
            final long bucketOrd = subBucketOrdinal(owningBucketOrdinal, i);
            org.elasticsearch.search.aggregations.bucket.range.Range.Bucket bucket =
                    rangeFactory.createBucket(range.key, range.from, range.to, bucketDocCount(bucketOrd), bucketAggregations(bucketOrd), keyed, formatter);
            buckets.add(bucket);
        }
        // value source can be null in the case of unmapped fields
        return rangeFactory.create(name, buckets, formatter, keyed, pipelineAggregators(), metaData());
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        InternalAggregations subAggs = buildEmptySubAggregations();
        List<org.elasticsearch.search.aggregations.bucket.range.Range.Bucket> buckets = new ArrayList<>(ranges.length);
        for (int i = 0; i < ranges.length; i++) {
            Range range = ranges[i];
            org.elasticsearch.search.aggregations.bucket.range.Range.Bucket bucket =
                    rangeFactory.createBucket(range.key, range.from, range.to, 0, subAggs, keyed, formatter);
            buckets.add(bucket);
        }
        // value source can be null in the case of unmapped fields
        return rangeFactory.create(name, buckets, formatter, keyed, pipelineAggregators(), metaData());
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

        private final List<? extends RangeAggregator.Range> ranges;
        private final boolean keyed;
        private final InternalRange.Factory factory;
        private final ValueFormatter formatter;

        public Unmapped(String name, List<? extends RangeAggregator.Range> ranges, boolean keyed, ValueFormat format,
                AggregationContext context,
                Aggregator parent, InternalRange.Factory factory, List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData)
                throws IOException {

            super(name, context, parent, pipelineAggregators, metaData);
            this.ranges = ranges;
            ValueParser parser = format != null ? format.parser() : ValueParser.RAW;
            for (Range range : this.ranges) {
                range.process(parser, context.searchContext());
            }
            this.keyed = keyed;
            this.formatter = format.formatter();
            this.factory = factory;
        }

        @Override
        public InternalAggregation buildEmptyAggregation() {
            InternalAggregations subAggs = buildEmptySubAggregations();
            List<org.elasticsearch.search.aggregations.bucket.range.Range.Bucket> buckets = new ArrayList<>(ranges.size());
            for (RangeAggregator.Range range : ranges) {
                buckets.add(factory.createBucket(range.key, range.from, range.to, 0, subAggs, keyed, formatter));
            }
            return factory.create(name, buckets, formatter, keyed, pipelineAggregators(), metaData());
        }
    }

    public static class Factory extends ValuesSourceAggregatorFactory<ValuesSource.Numeric> {

        private final InternalRange.Factory rangeFactory;
        private final List<? extends Range> ranges;
        private boolean keyed = false;

        public Factory(String name, List<? extends Range> ranges) {
            this(name, InternalRange.FACTORY, ranges);
        }

        protected Factory(String name, InternalRange.Factory rangeFactory, List<? extends Range> ranges) {
            super(name, rangeFactory.type(), rangeFactory.getValueSourceType(), rangeFactory.getValueType());
            this.rangeFactory = rangeFactory;
            this.ranges = ranges;
        }

        public void keyed(boolean keyed) {
            this.keyed = keyed;
        }

        public boolean keyed() {
            return keyed;
        }

        @Override
        protected Aggregator createUnmapped(AggregationContext aggregationContext, Aggregator parent, List<PipelineAggregator> pipelineAggregators,
                Map<String, Object> metaData) throws IOException {
            return new Unmapped(name, ranges, keyed, config.format(), aggregationContext, parent, rangeFactory, pipelineAggregators, metaData);
        }

        @Override
        protected Aggregator doCreateInternal(ValuesSource.Numeric valuesSource, AggregationContext aggregationContext, Aggregator parent,
                boolean collectsFromSingleBucket, List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData) throws IOException {
            return new RangeAggregator(name, factories, valuesSource, config.format(), rangeFactory, ranges, keyed, aggregationContext, parent, pipelineAggregators, metaData);
        }

        @Override
        protected XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
            builder.field(RANGES_FIELD.getPreferredName(), ranges);
            builder.field(KEYED_FIELD.getPreferredName(), keyed);
            return builder;
        }

        @Override
        protected ValuesSourceAggregatorFactory<Numeric> innerReadFrom(String name, ValuesSourceType valuesSourceType,
                ValueType targetValueType, StreamInput in) throws IOException {
            Factory factory = createFactoryFromStream(name, in);
            factory.keyed = in.readBoolean();
            return factory;
        }

        protected Factory createFactoryFromStream(String name, StreamInput in) throws IOException {
            int size = in.readVInt();
            List<Range> ranges = new ArrayList<>(size);
            for (int i = 0; i < size; i++) {
                ranges.add(Range.PROTOTYPE.readFrom(in));
            }
            return new Factory(name, ranges);
        }

        @Override
        protected void innerWriteTo(StreamOutput out) throws IOException {
            out.writeVInt(ranges.size());
            for (Range range : ranges) {
                range.writeTo(out);
            }
            out.writeBoolean(keyed);
        }

        @Override
        protected int innerHashCode() {
            return Objects.hash(ranges, keyed);
        }

        @Override
        protected boolean innerEquals(Object obj) {
            Factory other = (Factory) obj;
            return Objects.equals(ranges, other.ranges)
                    && Objects.equals(keyed, other.keyed);
        }
    }

}
