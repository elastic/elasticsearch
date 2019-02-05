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
import org.apache.lucene.search.ScoreMode;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParserUtils;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;
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
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

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

    final ValuesSource.Numeric valuesSource;
    final DocValueFormat format;
    final Range[] ranges;
    final boolean keyed;
    final InternalRange.Factory rangeFactory;

    final double[] maxTo;

    public RangeAggregator(String name, AggregatorFactories factories, ValuesSource.Numeric valuesSource, DocValueFormat format,
            InternalRange.Factory rangeFactory, Range[] ranges, boolean keyed, SearchContext context,
            Aggregator parent, List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData) throws IOException {

        super(name, factories, context, parent, pipelineAggregators, metaData);
        assert valuesSource != null;
        this.valuesSource = valuesSource;
        this.format = format;
        this.keyed = keyed;
        this.rangeFactory = rangeFactory;

        this.ranges = ranges;

        maxTo = new double[this.ranges.length];
        maxTo[0] = this.ranges[0].to;
        for (int i = 1; i < this.ranges.length; ++i) {
            maxTo[i] = Math.max(this.ranges[i].to,maxTo[i-1]);
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
        final SortedNumericDoubleValues values = valuesSource.doubleValues(ctx);
        return new LeafBucketCollectorBase(sub, values) {
            @Override
            public void collect(int doc, long bucket) throws IOException {
                if (values.advanceExact(doc)) {
                    final int valuesCount = values.docValueCount();
                    for (int i = 0, lo = 0; i < valuesCount; ++i) {
                        final double value = values.nextValue();
                        lo = collect(doc, value, bucket, lo);
                    }
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

    private long subBucketOrdinal(long owningBucketOrdinal, int rangeOrd) {
        return owningBucketOrdinal * ranges.length + rangeOrd;
    }

    @Override
    public InternalAggregation buildAggregation(long owningBucketOrdinal) throws IOException {
        consumeBucketsAndMaybeBreak(ranges.length);
        List<org.elasticsearch.search.aggregations.bucket.range.Range.Bucket> buckets = new ArrayList<>(ranges.length);
        for (int i = 0; i < ranges.length; i++) {
            Range range = ranges[i];
            final long bucketOrd = subBucketOrdinal(owningBucketOrdinal, i);
            org.elasticsearch.search.aggregations.bucket.range.Range.Bucket bucket =
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
        List<org.elasticsearch.search.aggregations.bucket.range.Range.Bucket> buckets = new ArrayList<>(ranges.length);
        for (int i = 0; i < ranges.length; i++) {
            Range range = ranges[i];
            org.elasticsearch.search.aggregations.bucket.range.Range.Bucket bucket =
                    rangeFactory.createBucket(range.key, range.from, range.to, 0, subAggs, keyed, format);
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
