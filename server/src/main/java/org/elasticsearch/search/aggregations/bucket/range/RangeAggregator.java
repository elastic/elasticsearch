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

import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.search.CollectionTerminatedException;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.DocIdSetBuilder;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParserUtils;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.LeafBucketCollectorBase;
import org.elasticsearch.search.aggregations.NonCollectingAggregator;
import org.elasticsearch.search.aggregations.bucket.BucketsAggregator;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiFunction;

import static org.apache.lucene.util.FutureArrays.compareUnsigned;

public class RangeAggregator extends BucketsAggregator {

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

        public Range(String key, Double from, String fromAsStr, Double to, String toAsStr) {
            this.key = key;
            this.from = from == null ? Double.NEGATIVE_INFINITY : from;
            this.fromAsStr = fromAsStr;
            this.to = to == null ? Double.POSITIVE_INFINITY : to;
            this.toAsStr = toAsStr;
        }

        boolean matches(double value) {
            return value >= from && value < to;
        }

        @Override
        public String toString() {
            return "[" + from + " to " + to + ")";
        }

        public static Range fromXContent(XContentParser parser) throws IOException {
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
                    if (FROM_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                        from = parser.doubleValue();
                    } else if (TO_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                        to = parser.doubleValue();
                    } else {
                        XContentParserUtils.throwUnknownField(currentFieldName, parser.getTokenLocation());
                    }
                } else if (token == XContentParser.Token.VALUE_STRING) {
                    if (FROM_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                        fromAsStr = parser.text();
                    } else if (TO_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                        toAsStr = parser.text();
                    } else if (KEY_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                        key = parser.text();
                    } else {
                        XContentParserUtils.throwUnknownField(currentFieldName, parser.getTokenLocation());
                    }
                } else if (token == XContentParser.Token.VALUE_NULL) {
                    if (FROM_FIELD.match(currentFieldName, parser.getDeprecationHandler())
                        || TO_FIELD.match(currentFieldName, parser.getDeprecationHandler())
                        || KEY_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                        // ignore null value
                    } else {
                        XContentParserUtils.throwUnknownField(currentFieldName, parser.getTokenLocation());
                    }
                } else {
                    XContentParserUtils.throwUnknownToken(token, parser.getTokenLocation());
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

    private final ValuesSource.Numeric valuesSource;
    final DocValueFormat format;
    final Range[] ranges;

    final boolean keyed;
    private final InternalRange.Factory rangeFactory;
    private final double[] maxTo;

    private final String pointField;
    private final boolean canOptimize;
    private byte[][] encodedRanges;


    public RangeAggregator(String name, AggregatorFactories factories, ValuesSource.Numeric valuesSource, ValuesSourceConfig<?> config,
                           InternalRange.Factory rangeFactory, Range[] ranges, boolean keyed, SearchContext context, Aggregator parent,
                           List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData) throws IOException {

        super(name, factories, context, parent, pipelineAggregators, metaData);
        assert valuesSource != null;
        this.valuesSource = valuesSource;
        this.format = config.format();
        this.keyed = keyed;
        this.rangeFactory = rangeFactory;
        this.ranges = ranges;
        BiFunction<Number, Boolean, byte[]> pointEncoder = configurePointEncoder(context, parent, config);

        // Unbounded ranges collect most documents, so the BKD optimization doesn't
        // help nearly as much
        boolean rangesAreBounded = Double.isFinite(ranges[0].from);

        maxTo = new double[ranges.length];
        maxTo[0] = ranges[0].to;
        for (int i = 1; i < ranges.length; ++i) {
            maxTo[i] = Math.max(ranges[i].to, maxTo[i-1]);
            rangesAreBounded &= Double.isFinite(ranges[i].to);
        }

        if (pointEncoder != null && rangesAreBounded) {
            pointField = config.fieldContext().field();
            encodedRanges = new byte[ranges.length * 2][];
            for (int i = 0; i < ranges.length; i++) {
                byte[] from = Double.isFinite(ranges[i].from) ? pointEncoder.apply(ranges[i].from, false) : null;
                byte[] to = Double.isFinite(ranges[i].to) ? pointEncoder.apply(ranges[i].to, false) : null;
                encodedRanges[i*2] = from;
                encodedRanges[i*2 + 1] = to;
            }
            canOptimize = true;
        } else {
            pointField = null;
            canOptimize = false;
        }
    }

    @Override
    public ScoreMode scoreMode() {
        if (valuesSource != null && valuesSource.needsScores()) {
            return ScoreMode.COMPLETE;
        }
        return super.scoreMode();
    }

    @Override
    public LeafBucketCollector getLeafCollector(LeafReaderContext ctx,
            final LeafBucketCollector sub) throws IOException {

        if (valuesSource == null) {
            if (parent != null) {
                return LeafBucketCollector.NO_OP_COLLECTOR;
            } else {
                // we have no parent and the values source is empty so we can skip collecting hits.
                throw new CollectionTerminatedException();
            }
        }

        if (canOptimize) {
            // if we can optimize, and we decide the optimization is better than DV collection,
            // this will use the BKD to collect hits and then throw a CollectionTerminatedException
            tryBKDOptimization(ctx, sub);
        }

        // We either cannot optimize, or have decided DVs would be faster so
        // fall back to collecting all the values from DVs directly
        final SortedNumericDoubleValues values = valuesSource.doubleValues(ctx);
        return new LeafBucketCollectorBase(sub, values) {
            @Override
            public void collect(int doc, long bucket) throws IOException {
                if (values.advanceExact(doc)) {
                    final int valuesCount = values.docValueCount();
                    for (int i = 0, lo = 0; i < valuesCount; ++i) {
                        final double value = values.nextValue();
                        lo = collectValue(doc, value, bucket, lo, sub);
                    }
                }
            }
        };
    }

    private int collectValue(int doc, double value, long owningBucketOrdinal, int lowBound, LeafBucketCollector sub) throws IOException {
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

    /**
     * Attempt to collect these ranges via the BKD tree instead of DocValues.
     *
     * This estimates the number of matching points in the BKD tree.  If it is
     * less than 75% of maxDoc we attempt to use the BKD tree to collect values.
     * The BKD tree is potentially much faster than DV collection because
     * we only need to inspect leaves that overlap each range, rather than
     * collecting all the values as with DVs.  And since we only care about doc
     * counts, we don't need to decode values when an entire leaf matches.
     *
     * If we use the BKD tree, when it is done collecting values a
     * {@link CollectionTerminatedException} is thrown to signal completion
     */
    private void tryBKDOptimization(LeafReaderContext ctx, LeafBucketCollector sub) throws CollectionTerminatedException, IOException {
        final PointValues pointValues = ctx.reader().getPointValues(pointField);
        if (pointValues != null) {
            PointValues.IntersectVisitor[] visitors = new PointValues.IntersectVisitor[ranges.length];
            DocIdSetBuilder[] results = new DocIdSetBuilder[ranges.length];

            final Bits liveDocs = ctx.reader().getLiveDocs();
            int maxDoc = ctx.reader().maxDoc();
            long estimatedPoints = 0;
            for (int i = 0; i < ranges.length; i++) {
                // OK to allocate DocIdSetBuilder now since it allocates memory lazily and the
                // estimation won't call `grow()` on the visitor (only once we start intersecting)
                results[i]  = new DocIdSetBuilder(maxDoc);
                visitors[i] = getVisitor(liveDocs, encodedRanges[i * 2], encodedRanges[i * 2 + 1], results[i]);
                estimatedPoints += pointValues.estimatePointCount(visitors[i]);
            }

            if (estimatedPoints < maxDoc * 0.75) {
                // We collect ranges individually since a doc can land in multiple ranges.
                for (int i = 0; i < ranges.length; i++) {
                    pointValues.intersect(visitors[i]);
                    DocIdSetIterator iter = results[i].build().iterator();
                    while (iter.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
                        // Now that we know the matching docs, collect the bucket and sub-aggs
                        //
                        // NOTE: because we're in the BKD optimization, we know there is no parent agg
                        // and bucket ordinals are zero-based offset by range ordinal
                        collectBucket(sub, iter.docID(), i);
                    }
                    // free this DocIdSet since we no longer need it, and it could be holding
                    // non-negligible amount of memory
                    results[i] = null;
                }
                throw new CollectionTerminatedException();
            }
        }
    }

    /**
     * Returns a BKD intersection visitor for the provided range (`from` inclusive, `to` exclusive)
     */
    private PointValues.IntersectVisitor getVisitor(Bits liveDocs, byte[] from, byte[] to, DocIdSetBuilder result) {


        return new PointValues.IntersectVisitor() {
            DocIdSetBuilder.BulkAdder adder;

            @Override
            public void grow(int count) {
                adder = result.grow(count);
            }

            @Override
            public void visit(int docID) {
                if ((liveDocs == null || liveDocs.get(docID))) {
                    adder.add(docID);
                }
            }

            @Override
            public void visit(int docID, byte[] packedValue) {
                int packedLength = packedValue.length;

                // Value is inside range if value >= from && value < to
                boolean inside = (from == null || compareUnsigned(packedValue, 0, packedValue.length, from, 0, from.length) >= 0)
                    && (to == null || compareUnsigned(packedValue, 0, packedLength, to, 0, to.length) < 0);

                if (inside) {
                    visit(docID);
                }
            }

            @Override
            public void visit(DocIdSetIterator iterator, byte[] packedValue) throws IOException {
                int packedLength = packedValue.length;

                // Value is inside range if value >= from && value < to
                boolean inside = (from == null || compareUnsigned(packedValue, 0, packedValue.length, from, 0, from.length) >= 0)
                    && (to == null || compareUnsigned(packedValue, 0, packedLength, to, 0, to.length) < 0);

                if (inside) {
                    while (iterator.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
                        visit(iterator.docID());
                    }
                }
            }

            @Override
            public PointValues.Relation compare(byte[] minPackedValue, byte[] maxPackedValue) {

                int packedLength = minPackedValue.length;

                // max < from (exclusive, since ranges are inclusive on from)
                if (from != null && compareUnsigned(maxPackedValue, 0, packedLength, from, 0, from.length) < 0) {
                    return PointValues.Relation.CELL_OUTSIDE_QUERY;
                }
                // min >= from (inclusive, since ranges are exclusive on to)
                if (to != null && compareUnsigned(minPackedValue, 0, packedLength, to, 0, to.length) >= 0) {
                    return PointValues.Relation.CELL_OUTSIDE_QUERY;
                }

                // Leaf is fully inside this range if min >= from && max < to
                if (
                    // `from` is unbounded or `min >= from`
                    (from == null || compareUnsigned(minPackedValue, 0, packedLength, from, 0, from.length) >= 0)
                    &&
                        // `to` is unbounded or `max < to`
                    (to == null || compareUnsigned(maxPackedValue, 0, packedLength, to, 0, to.length) < 0)
                ) {
                    return PointValues.Relation.CELL_INSIDE_QUERY;
                }

                // If we're not outside, and not fully inside, we must be crossing
                return PointValues.Relation.CELL_CROSSES_QUERY;

            }
        };
    }

    /**
     * Returns a converter for point values if BKD optimization is applicable to
     * the context or <code>null</code> otherwise.  Optimization criteria is:
     * - Match_all query
     * - no parent agg
     * - no script
     * - no missing value
     * - has indexed points
     *
     * @param context The {@link SearchContext} of the aggregation.
     * @param parent The parent aggregator.
     * @param config The config for the values source metric.
     */
    private BiFunction<Number, Boolean, byte[]> configurePointEncoder(SearchContext context, Aggregator parent,
                                          ValuesSourceConfig<?> config) {
        if (context.query() != null &&
            context.query().getClass() != MatchAllDocsQuery.class) {
            return null;
        }
        if (parent != null) {
            return null;
        }
        if (config.fieldContext() != null && config.script() == null && config.missing() == null) {
            MappedFieldType fieldType = config.fieldContext().fieldType();
            if (fieldType == null || fieldType.indexOptions() == IndexOptions.NONE) {
                return null;
            }
            if (fieldType instanceof NumberFieldMapper.NumberFieldType) {
                return ((NumberFieldMapper.NumberFieldType) fieldType)::encodePoint;
            } else if (fieldType.getClass() == DateFieldMapper.DateFieldType.class) {
                return NumberFieldMapper.NumberType.LONG::encodePoint;
            }
        }
        return null;
    }

    private long subBucketOrdinal(long owningBucketOrdinal, int rangeOrd) {
        return owningBucketOrdinal * ranges.length + rangeOrd;
    }

    @Override
    public InternalAggregation buildAggregation(long owningBucketOrdinal) throws IOException {
        consumeBucketsAndMaybeBreak(ranges.length);
        List<InternalRange.Bucket> buckets = new ArrayList<>(ranges.length);
        for (int i = 0; i < ranges.length; i++) {
            Range range = ranges[i];
            final long bucketOrd = subBucketOrdinal(owningBucketOrdinal, i);
            InternalRange.Bucket bucket =
                rangeFactory.createBucket(range.key, range.from, range.to, bucketDocCount(bucketOrd),
                    bucketAggregations(bucketOrd), keyed, format);
            buckets.add(bucket);
        }
        // value source can be null in the case of unmapped fields
        return rangeFactory.create(name, buckets, format, keyed, pipelineAggregators(), metaData());
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        InternalAggregations subAggs = buildEmptySubAggregations();
        List<InternalRange.Bucket> buckets = new ArrayList<>(ranges.length);
        for (int i = 0; i < ranges.length; i++) {
            Range range = ranges[i];
            InternalRange.Bucket bucket = rangeFactory.createBucket(range.key, range.from, range.to, 0, subAggs, keyed, format);
            buckets.add(bucket);
        }
        // value source can be null in the case of unmapped fields
        return rangeFactory.create(name, buckets, format, keyed, pipelineAggregators(), metaData());
    }

    public static class Unmapped<R extends RangeAggregator.Range> extends NonCollectingAggregator {

        private final R[] ranges;
        private final boolean keyed;
        private final InternalRange.Factory factory;
        private final DocValueFormat format;

        public Unmapped(String name, R[] ranges, boolean keyed, DocValueFormat format, SearchContext context, Aggregator parent,
                InternalRange.Factory factory, List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData)
                throws IOException {

            super(name, context, parent, pipelineAggregators, metaData);
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
            return factory.create(name, buckets, format, keyed, pipelineAggregators(), metaData());
        }
    }

}
