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

import org.apache.logging.log4j.LogManager;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ContextParser;
import org.elasticsearch.common.xcontent.ObjectParser.ValueType;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;
import org.elasticsearch.index.mapper.DateFieldMapper.DateFieldType;
import org.elasticsearch.index.mapper.DateFieldMapper.Resolution;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.AdaptingAggregator;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.CardinalityUpperBound;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.LeafBucketCollectorBase;
import org.elasticsearch.search.aggregations.NonCollectingAggregator;
import org.elasticsearch.search.aggregations.bucket.BucketsAggregator;
import org.elasticsearch.search.aggregations.bucket.filter.FiltersAggregator;
import org.elasticsearch.search.aggregations.bucket.filter.InternalFilters;
import org.elasticsearch.search.aggregations.bucket.range.InternalRange.Factory;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSource.Numeric;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

public abstract class RangeAggregator extends BucketsAggregator {

    public static final ParseField RANGES_FIELD = new ParseField("ranges");
    public static final ParseField KEYED_FIELD = new ParseField("keyed");

    public static class Range implements Writeable, ToXContentObject {
        public static final ParseField KEY_FIELD = new ParseField("key");
        public static final ParseField FROM_FIELD = new ParseField("from");
        public static final ParseField TO_FIELD = new ParseField("to");

        protected final String key;
        protected final double from;
        protected final String fromAsStr;
        protected final double to;
        protected final String toAsStr;

        /**
         * Build the range. Generally callers should prefer
         * {@link Range#Range(String, Double, Double)} or
         * {@link Range#Range(String, String, String)}. If you
         * <strong>must</strong> call this know that consumers prefer
         * {@code from} and {@code to} parameters if they are non-null
         * and finite. Otherwise they parse from {@code fromrStr} and
         * {@code toStr}. 
         */
        public Range(String key, Double from, String fromAsStr, Double to, String toAsStr) {
            this.key = key;
            this.from = from == null ? Double.NEGATIVE_INFINITY : from;
            this.fromAsStr = fromAsStr;
            this.to = to == null ? Double.POSITIVE_INFINITY : to;
            this.toAsStr = toAsStr;
        }

        public Range(String key, Double from, Double to) {
            this(key, from, null, to, null);
        }

        public Range(String key, String from, String to) {
            this(key, null, from, null, to);
        }

        /**
         * Read from a stream.
         */
        public Range(StreamInput in) throws IOException {
            key = in.readOptionalString();
            fromAsStr = in.readOptionalString();
            toAsStr = in.readOptionalString();
            from = in.readDouble();
            to = in.readDouble();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeOptionalString(key);
            out.writeOptionalString(fromAsStr);
            out.writeOptionalString(toAsStr);
            out.writeDouble(from);
            out.writeDouble(to);
        }

        public double getFrom() {
            return this.from;
        }

        public double getTo() {
            return this.to;
        }

        public String getFromAsString() {
            return this.fromAsStr;
        }

        public String getToAsString() {
            return this.toAsStr;
        }

        public String getKey() {
            return this.key;
        }

        boolean matches(double value) {
            return value >= from && value < to;
        }

        @Override
        public String toString() {
            return "[" + from + " to " + to + ")";
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

        public static final ConstructingObjectParser<Range, Void> PARSER = new ConstructingObjectParser<>("range", arg -> {
            String key = (String) arg[0];
            Object from = arg[1];
            Object to = arg[2];
            Double fromDouble = from instanceof Number ? ((Number) from).doubleValue() : null;
            Double toDouble = to instanceof Number ? ((Number) to).doubleValue() : null;
            String fromStr = from instanceof String ? (String) from : null;
            String toStr = to instanceof String ? (String) to : null;
            return new Range(key, fromDouble, fromStr, toDouble, toStr);
        });

        static {
            PARSER.declareField(optionalConstructorArg(),
                (p, c) -> p.text(),
                KEY_FIELD, ValueType.DOUBLE); // DOUBLE supports string and number
            ContextParser<Void, Object> fromToParser = (p, c) -> {
                if (p.currentToken() == XContentParser.Token.VALUE_STRING) {
                    return p.text();
                }
                if (p.currentToken() == XContentParser.Token.VALUE_NUMBER) {
                    return p.doubleValue();
                }
                return null;
            };
            // DOUBLE_OR_NULL accepts String, Number, and null
            PARSER.declareField(optionalConstructorArg(), fromToParser, FROM_FIELD, ValueType.DOUBLE_OR_NULL);
            PARSER.declareField(optionalConstructorArg(), fromToParser, TO_FIELD, ValueType.DOUBLE_OR_NULL);
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

    public static Aggregator build(
        String name,
        AggregatorFactories factories,
        ValuesSourceConfig valuesSourceConfig,
        InternalRange.Factory<?, ?> rangeFactory,
        Range[] ranges,
        boolean keyed,
        SearchContext context,
        Aggregator parent,
        CardinalityUpperBound cardinality,
        Map<String, Object> metadata
    ) throws IOException {
        Aggregator adapted = adaptIntoFiltersOrNull(name, factories, valuesSourceConfig, rangeFactory, ranges, keyed, context, parent, cardinality, metadata);
        if (adapted != null) {
            return adapted;
        }
        return buildWithoutAttemptedToAdaptToFilters(
            name,
            factories,
            (ValuesSource.Numeric) valuesSourceConfig.getValuesSource(),
            valuesSourceConfig.format(),
            rangeFactory,
            ranges,
            keyed,
            context,
            parent,
            cardinality,
            metadata
        );
    }

    public static Aggregator adaptIntoFiltersOrNull(
        String name,
        AggregatorFactories factories,
        ValuesSourceConfig valuesSourceConfig,
        InternalRange.Factory<?, ?> rangeFactory,
        Range[] ranges,
        boolean keyed,
        SearchContext context,
        Aggregator parent,
        CardinalityUpperBound cardinality,
        Map<String, Object> metadata
    ) throws IOException {
        if (false == valuesSourceConfig.fieldType().isSearchable()) {
            return null;
        }
        if (valuesSourceConfig.fieldType() instanceof DateFieldType
            && ((DateFieldType) valuesSourceConfig.fieldType()).resolution() == Resolution.NANOSECONDS) {
            // We don't generate sensible Queries for nanoseconds.
            return null;
        }
        String[] keys = new String[ranges.length];
        Query[] filters = new Query[ranges.length];
        for (int i = 0; i < ranges.length; i++) {
            keys[i] = Integer.toString(i);
            filters[i] = valuesSourceConfig.fieldType()
                .rangeQuery(
                    valuesSourceConfig.format().format(ranges[i].from),
                    valuesSourceConfig.format().format(ranges[i].to),
                    true,
                    false,
                    ShapeRelation.CONTAINS,
                    null,
                    null,
                    context.getQueryShardContext()
                );
        }
        FiltersAggregator delegate = FiltersAggregator.build(
            name,
            factories,
            keys,
            filters,
            false,
            null,
            context,
            parent,
            cardinality,
            metadata
        );
        if (false == delegate.collectsInFilterOrder()) {
            return null;
        }
        LogManager.getLogger().error("ADSFADSF {}", (Object) filters);
        return new RangeAdaptedFromFiltersAggregator<>(delegate, valuesSourceConfig.format(), ranges, keyed, rangeFactory);
    }

    public static Aggregator buildWithoutAttemptedToAdaptToFilters(
        String name,
        AggregatorFactories factories,
        ValuesSource.Numeric valuesSource,
        DocValueFormat format,
        InternalRange.Factory<?, ?> rangeFactory,
        Range[] ranges,
        boolean keyed,
        SearchContext context,
        Aggregator parent,
        CardinalityUpperBound cardinality,
        Map<String, Object> metadata
    ) throws IOException {
        if (hasOverlap(ranges)) {
            return new OverlapRangeAggregator(
                name,
                factories,
                valuesSource,
                format,
                rangeFactory,
                ranges,
                keyed,
                context,
                parent,
                cardinality,
                metadata
            );
        }
        return new NoOverlapAggregator(
            name,
            factories,
            valuesSource,
            format,
            rangeFactory,
            ranges,
            keyed,
            context,
            parent,
            cardinality,
            metadata
        );
    }

    private final ValuesSource.Numeric valuesSource;
    private final DocValueFormat format;
    protected final Range[] ranges;
    private final boolean keyed;
    private final InternalRange.Factory rangeFactory;

    private RangeAggregator(String name, AggregatorFactories factories, ValuesSource.Numeric valuesSource, DocValueFormat format,
            InternalRange.Factory rangeFactory, Range[] ranges, boolean keyed, SearchContext context,
            Aggregator parent, CardinalityUpperBound cardinality, Map<String, Object> metadata) throws IOException {

        super(name, factories, context, parent, cardinality.multiply(ranges.length), metadata);
        assert valuesSource != null;
        this.valuesSource = valuesSource;
        this.format = format;
        this.keyed = keyed;
        this.rangeFactory = rangeFactory;
        this.ranges = ranges;
    }

    @Override
    public ScoreMode scoreMode() {
        if (valuesSource != null && valuesSource.needsScores()) {
            return ScoreMode.COMPLETE;
        }
        return super.scoreMode();
    }

    @Override
    public LeafBucketCollector getLeafCollector(LeafReaderContext ctx, LeafBucketCollector sub) throws IOException {
        final SortedNumericDoubleValues values = valuesSource.doubleValues(ctx);
        return new LeafBucketCollectorBase(sub, values) {
            @Override
            public void collect(int doc, long bucket) throws IOException {
                if (values.advanceExact(doc)) {
                    final int valuesCount = values.docValueCount();
                    for (int i = 0, lo = 0; i < valuesCount; ++i) {
                        final double value = values.nextValue();
                        lo = RangeAggregator.this.collect(sub, doc, value, bucket, lo);
                    }
                }
            }
        };
    }

    protected long subBucketOrdinal(long owningBucketOrdinal, int rangeOrd) {
        return owningBucketOrdinal * ranges.length + rangeOrd;
    }

    @Override
    public InternalAggregation[] buildAggregations(long[] owningBucketOrds) throws IOException {
        return buildAggregationsForFixedBucketCount(owningBucketOrds, ranges.length,
            (offsetInOwningOrd, docCount, subAggregationResults) -> {
                Range range = ranges[offsetInOwningOrd];
                return rangeFactory.createBucket(range.key, range.from, range.to, docCount, subAggregationResults, keyed, format);
            }, buckets -> rangeFactory.create(name, buckets, format, keyed, metadata()));
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        InternalAggregations subAggs = buildEmptySubAggregations();
        List<org.elasticsearch.search.aggregations.bucket.range.Range.Bucket> buckets = new ArrayList<>(ranges.length);
        for (int i = 0; i < ranges.length; i++) {
            Range range = ranges[i];
            org.elasticsearch.search.aggregations.bucket.range.Range.Bucket bucket =
                    rangeFactory.createBucket(range.key, range.from, range.to, 0, subAggs, keyed, format);
            buckets.add(bucket);
        }
        // value source can be null in the case of unmapped fields
        return rangeFactory.create(name, buckets, format, keyed, metadata());
    }

    public static class Unmapped<R extends RangeAggregator.Range> extends NonCollectingAggregator {

        private final R[] ranges;
        private final boolean keyed;
        private final InternalRange.Factory factory;
        private final DocValueFormat format;

        public Unmapped(String name, R[] ranges, boolean keyed, DocValueFormat format, SearchContext context, Aggregator parent,
                InternalRange.Factory factory, Map<String, Object> metadata)
                throws IOException {

            super(name, context, parent, metadata);
            this.ranges = ranges;
            this.keyed = keyed;
            this.format = format;
            this.factory = factory;
        }

        @Override
        public InternalAggregation buildEmptyAggregation() {
            InternalAggregations subAggs = buildEmptySubAggregations();
            List<org.elasticsearch.search.aggregations.bucket.range.Range.Bucket> buckets = new ArrayList<>(ranges.length);
            for (RangeAggregator.Range range : ranges) {
                buckets.add(factory.createBucket(range.key, range.from, range.to, 0, subAggs, keyed, format));
            }
            return factory.create(name, buckets, format, keyed, metadata());
        }
    }

    protected abstract int collect(LeafBucketCollector sub, int doc, double value, long owningBucketOrdinal, int lowBound)
        throws IOException;

    private static class NoOverlapAggregator extends RangeAggregator {
        public NoOverlapAggregator(
            String name,
            AggregatorFactories factories,
            Numeric valuesSource,
            DocValueFormat format,
            Factory rangeFactory,
            Range[] ranges,
            boolean keyed,
            SearchContext context,
            Aggregator parent,
            CardinalityUpperBound cardinality,
            Map<String, Object> metadata
        ) throws IOException {
            super(name, factories, valuesSource, format, rangeFactory, ranges, keyed, context, parent, cardinality, metadata);
        }

        @Override
        protected int collect(LeafBucketCollector sub, int doc, double value, long owningBucketOrdinal, int lowBound) throws IOException {
            int lo = lowBound, hi = ranges.length - 1;
            while (lo <= hi) {
                final int mid = (lo + hi) >>> 1;
                if (value < ranges[mid].from) {
                    hi = mid - 1;
                } else if (value >= ranges[mid].to) {
                    lo = mid + 1;
                } else {
                    collectBucket(sub, doc, subBucketOrdinal(owningBucketOrdinal, mid));
                    return mid;
                }
            }
            return lo;
        }
    }

    private static class OverlapRangeAggregator extends RangeAggregator {
        public OverlapRangeAggregator(
            String name,
            AggregatorFactories factories,
            Numeric valuesSource,
            DocValueFormat format,
            Factory rangeFactory,
            Range[] ranges,
            boolean keyed,
            SearchContext context,
            Aggregator parent,
            CardinalityUpperBound cardinality,
            Map<String, Object> metadata
        ) throws IOException {
            super(name, factories, valuesSource, format, rangeFactory, ranges, keyed, context, parent, cardinality, metadata);
            maxTo = new double[ranges.length];
            maxTo[0] = ranges[0].to;
            for (int i = 1; i < ranges.length; ++i) {
                maxTo[i] = Math.max(ranges[i].to, maxTo[i - 1]);
            }
        }

        private final double[] maxTo;

        @Override
        protected int collect(LeafBucketCollector sub, int doc, double value, long owningBucketOrdinal, int lowBound) throws IOException {
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
    }

    private static class RangeAdaptedFromFiltersAggregator<B extends InternalRange.Bucket> extends AdaptingAggregator {
        private final DocValueFormat format;
        private final Range[] ranges;
        private final boolean keyed;
        private final InternalRange.Factory<B, ?> rangeFactory;

        public RangeAdaptedFromFiltersAggregator(
            Aggregator delegate,
            DocValueFormat format,
            Range[] ranges,
            boolean keyed,
            InternalRange.Factory<B, ?> rangeFactory
        ) {
            super(delegate);
            this.format = format;
            this.ranges = ranges;
            this.keyed = keyed;
            this.rangeFactory = rangeFactory;
        }

        @Override
        protected InternalAggregation adapt(InternalAggregation delegateResult) {
            InternalFilters filters = (InternalFilters) delegateResult;
            if (filters.getBuckets().size() != ranges.length) {
                throw new IllegalStateException(
                    "bad number of filters [" + filters.getBuckets().size() + "] expecting [" + ranges.length + "]"
                );
            }
            List<B> buckets = new ArrayList<>(filters.getBuckets().size());
            for (int i = 0; i < ranges.length; i++) {
                Range r = ranges[i];
                InternalFilters.InternalBucket b = filters.getBuckets().get(i);
                buckets.add(
                    rangeFactory.createBucket(
                        r.getKey(),
                        r.getFrom(),
                        r.getTo(),
                        b.getDocCount(),
                        (InternalAggregations) b.getAggregations(),
                        keyed,
                        format
                    )
                );
            }
            return rangeFactory.create(name(), buckets, format, keyed, filters.getMetadata());
        }
    }

    private static boolean hasOverlap(Range[] ranges) {
        double lastEnd = ranges[0].to;
        for (int i = 1; i < ranges.length; ++i) {
            if (ranges[i].from < lastEnd) {
                return true;
            }
            lastEnd = ranges[i].to;
        }
        return false;
    }
}
