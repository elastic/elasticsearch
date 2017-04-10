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
package org.elasticsearch.search.aggregations.bucket.histogram;

import org.apache.lucene.util.CollectionUtil;
import org.apache.lucene.util.PriorityQueue;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.rounding.Rounding;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.rest.action.search.RestSearchAction;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.InternalMultiBucketAggregation;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.elasticsearch.common.xcontent.XContentParserUtils.parseTypedKeysObject;

/**
 * Implementation of {@link Histogram}.
 */
public final class InternalDateHistogram extends InternalMultiBucketAggregation<InternalDateHistogram, InternalDateHistogram.Bucket>
        implements Histogram, HistogramFactory {

    public static class Bucket extends InternalMultiBucketAggregation.InternalBucket implements Histogram.Bucket {

        final long key;
        final long docCount;
        final InternalAggregations aggregations;
        private final transient boolean keyed;
        protected final transient DocValueFormat format;

        public Bucket(long key, long docCount, boolean keyed, DocValueFormat format,
                InternalAggregations aggregations) {
            this.format = format;
            this.keyed = keyed;
            this.key = key;
            this.docCount = docCount;
            this.aggregations = aggregations;
        }

        /**
         * Read from a stream.
         */
        public Bucket(StreamInput in, boolean keyed, DocValueFormat format) throws IOException {
            this.format = format;
            this.keyed = keyed;
            key = in.readLong();
            docCount = in.readVLong();
            aggregations = InternalAggregations.readAggregations(in);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null || obj.getClass() != InternalDateHistogram.Bucket.class) {
                return false;
            }
            InternalDateHistogram.Bucket that = (InternalDateHistogram.Bucket) obj;
            // No need to take the keyed and format parameters into account,
            // they are already stored and tested on the InternalDateHistogram object
            return key == that.key
                    && docCount == that.docCount
                    && Objects.equals(aggregations, that.aggregations);
        }

        @Override
        public int hashCode() {
            return Objects.hash(getClass(), key, docCount, aggregations);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeLong(key);
            out.writeVLong(docCount);
            aggregations.writeTo(out);
        }

        @Override
        public String getKeyAsString() {
            return format.format(key);
        }

        @Override
        public Object getKey() {
            return new DateTime(key, DateTimeZone.UTC);
        }

        @Override
        public long getDocCount() {
            return docCount;
        }

        @Override
        public Aggregations getAggregations() {
            return aggregations;
        }

        Bucket reduce(List<Bucket> buckets, ReduceContext context) {
            List<InternalAggregations> aggregations = new ArrayList<>(buckets.size());
            long docCount = 0;
            for (Bucket bucket : buckets) {
                docCount += bucket.docCount;
                aggregations.add((InternalAggregations) bucket.getAggregations());
            }
            InternalAggregations aggs = InternalAggregations.reduce(aggregations, context);
            return new InternalDateHistogram.Bucket(key, docCount, keyed, format, aggs);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            String keyAsString = format.format(key);
            if (keyed) {
                builder.startObject(keyAsString);
            } else {
                builder.startObject();
            }
            if (format != DocValueFormat.RAW) {
                builder.field(CommonFields.KEY_AS_STRING.getPreferredName(), keyAsString);
            }
            if (params.paramAsBoolean(RestSearchAction.TYPED_KEYS_PARAM, false)) {
                builder.field(CommonFields.FORMAT.getPreferredName(), format);
            }

            builder.field(CommonFields.KEY.getPreferredName(), key);
            builder.field(CommonFields.DOC_COUNT.getPreferredName(), docCount);
            aggregations.toXContentInternal(builder, params);
            builder.endObject();
            return builder;
        }

        public DocValueFormat getFormatter() {
            return format;
        }

        public boolean getKeyed() {
            return keyed;
        }

        public static Bucket fromXContent(XContentParser parser) throws IOException {
            //TODO tlrx support keyed buckets
            XContentParser.Token token = parser.currentToken();
            ensureExpectedToken(XContentParser.Token.START_OBJECT, token, parser::getTokenLocation);

            long key = 0L;
            long docCount = 0L;
            List<InternalAggregation> aggregations = new ArrayList<>();
            String currentFieldName;
            DocValueFormat format = DocValueFormat.RAW;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                currentFieldName = parser.currentName();
                if (CommonFields.KEY.getPreferredName().equals(currentFieldName)) {
                    if (token.isValue()) {
                        key = parser.longValue();
                    }
                } else if (CommonFields.DOC_COUNT.getPreferredName().equals(currentFieldName)) {
                    if (token.isValue()) {
                        docCount = parser.longValue();
                    }
                } else if (CommonFields.KEY_AS_STRING.getPreferredName().equals(currentFieldName)) {
                    if (token.isValue()) {
                        parser.text();
                    }
                } else if (CommonFields.FORMAT.getPreferredName().equals(currentFieldName)) {
                    format = DocValueFormat.fromXContent(parser);
                } else {
                    aggregations.add(parseTypedKeysObject(parser, TYPED_KEYS_DELIMITER, InternalAggregation.class));
                }
            }
            //TODO tlrx Print out "keyed" and parse it back too
            return new Bucket(key, docCount, false, format, new InternalAggregations(aggregations));
        }
    }

    static class EmptyBucketInfo {

        final Rounding rounding;
        final InternalAggregations subAggregations;
        final ExtendedBounds bounds;

        EmptyBucketInfo(Rounding rounding, InternalAggregations subAggregations) {
            this(rounding, subAggregations, null);
        }

        EmptyBucketInfo(Rounding rounding, InternalAggregations subAggregations, ExtendedBounds bounds) {
            this.rounding = rounding;
            this.subAggregations = subAggregations;
            this.bounds = bounds;
        }

        EmptyBucketInfo(StreamInput in) throws IOException {
            rounding = Rounding.Streams.read(in);
            subAggregations = InternalAggregations.readAggregations(in);
            bounds = in.readOptionalWriteable(ExtendedBounds::new);
        }

        void writeTo(StreamOutput out) throws IOException {
            Rounding.Streams.write(rounding, out);
            subAggregations.writeTo(out);
            out.writeOptionalWriteable(bounds);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            EmptyBucketInfo that = (EmptyBucketInfo) obj;
            return Objects.equals(rounding, that.rounding)
                    && Objects.equals(bounds, that.bounds)
                    && Objects.equals(subAggregations, that.subAggregations);
        }

        @Override
        public int hashCode() {
            return Objects.hash(getClass(), rounding, bounds, subAggregations);
        }
    }

    private final List<Bucket> buckets;
    private final InternalOrder order;
    private final DocValueFormat format;
    private final boolean keyed;
    private final long minDocCount;
    private final long offset;
    private final EmptyBucketInfo emptyBucketInfo;

    InternalDateHistogram(String name, List<Bucket> buckets, InternalOrder order, long minDocCount, long offset,
            EmptyBucketInfo emptyBucketInfo,
            DocValueFormat formatter, boolean keyed, List<PipelineAggregator> pipelineAggregators,
            Map<String, Object> metaData) {
        super(name, pipelineAggregators, metaData);
        this.buckets = buckets;
        this.order = order;
        this.offset = offset;
        assert (minDocCount == 0) == (emptyBucketInfo != null);
        this.minDocCount = minDocCount;
        this.emptyBucketInfo = emptyBucketInfo;
        this.format = formatter;
        this.keyed = keyed;
    }

    /**
     * Stream from a stream.
     */
    public InternalDateHistogram(StreamInput in) throws IOException {
        super(in);
        order = InternalOrder.Streams.readOrder(in);
        minDocCount = in.readVLong();
        if (minDocCount == 0) {
            emptyBucketInfo = new EmptyBucketInfo(in);
        } else {
            emptyBucketInfo = null;
        }
        offset = in.readLong();
        format = in.readNamedWriteable(DocValueFormat.class);
        keyed = in.readBoolean();
        buckets = in.readList(stream -> new Bucket(stream, keyed, format));
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        InternalOrder.Streams.writeOrder(order, out);
        out.writeVLong(minDocCount);
        if (minDocCount == 0) {
            emptyBucketInfo.writeTo(out);
        }
        out.writeLong(offset);
        out.writeNamedWriteable(format);
        out.writeBoolean(keyed);
        out.writeList(buckets);
    }

    @Override
    public String getWriteableName() {
        return DateHistogramAggregationBuilder.NAME;
    }

    @Override
    public List<Histogram.Bucket> getBuckets() {
        return Collections.unmodifiableList(buckets);
    }

    @Override
    public InternalDateHistogram create(List<Bucket> buckets) {
        return new InternalDateHistogram(name, buckets, order, minDocCount, offset, emptyBucketInfo, format,
                keyed, pipelineAggregators(), metaData);
    }

    @Override
    public Bucket createBucket(InternalAggregations aggregations, Bucket prototype) {
        return new Bucket(prototype.key, prototype.docCount, prototype.keyed, prototype.format, aggregations);
    }

    private static class IteratorAndCurrent {

        private final Iterator<Bucket> iterator;
        private Bucket current;

        IteratorAndCurrent(Iterator<Bucket> iterator) {
            this.iterator = iterator;
            current = iterator.next();
        }

    }

    private List<Bucket> reduceBuckets(List<InternalAggregation> aggregations, ReduceContext reduceContext) {

        final PriorityQueue<IteratorAndCurrent> pq = new PriorityQueue<IteratorAndCurrent>(aggregations.size()) {
            @Override
            protected boolean lessThan(IteratorAndCurrent a, IteratorAndCurrent b) {
                return a.current.key < b.current.key;
            }
        };
        for (InternalAggregation aggregation : aggregations) {
            InternalDateHistogram histogram = (InternalDateHistogram) aggregation;
            if (histogram.buckets.isEmpty() == false) {
                pq.add(new IteratorAndCurrent(histogram.buckets.iterator()));
            }
        }

        List<Bucket> reducedBuckets = new ArrayList<>();
        if (pq.size() > 0) {
            // list of buckets coming from different shards that have the same key
            List<Bucket> currentBuckets = new ArrayList<>();
            double key = pq.top().current.key;

            do {
                final IteratorAndCurrent top = pq.top();

                if (top.current.key != key) {
                    // the key changes, reduce what we already buffered and reset the buffer for current buckets
                    final Bucket reduced = currentBuckets.get(0).reduce(currentBuckets, reduceContext);
                    if (reduced.getDocCount() >= minDocCount || reduceContext.isFinalReduce() == false) {
                        reducedBuckets.add(reduced);
                    }
                    currentBuckets.clear();
                    key = top.current.key;
                }

                currentBuckets.add(top.current);

                if (top.iterator.hasNext()) {
                    final Bucket next = top.iterator.next();
                    assert next.key > top.current.key : "shards must return data sorted by key";
                    top.current = next;
                    pq.updateTop();
                } else {
                    pq.pop();
                }
            } while (pq.size() > 0);

            if (currentBuckets.isEmpty() == false) {
                final Bucket reduced = currentBuckets.get(0).reduce(currentBuckets, reduceContext);
                if (reduced.getDocCount() >= minDocCount || reduceContext.isFinalReduce() == false) {
                    reducedBuckets.add(reduced);
                }
            }
        }

        return reducedBuckets;
    }

    private void addEmptyBuckets(List<Bucket> list, ReduceContext reduceContext) {
        Bucket lastBucket = null;
        ExtendedBounds bounds = emptyBucketInfo.bounds;
        ListIterator<Bucket> iter = list.listIterator();

        // first adding all the empty buckets *before* the actual data (based on th extended_bounds.min the user requested)
        InternalAggregations reducedEmptySubAggs = InternalAggregations.reduce(Collections.singletonList(emptyBucketInfo.subAggregations),
                reduceContext);
        if (bounds != null) {
            Bucket firstBucket = iter.hasNext() ? list.get(iter.nextIndex()) : null;
            if (firstBucket == null) {
                if (bounds.getMin() != null && bounds.getMax() != null) {
                    long key = bounds.getMin();
                    long max = bounds.getMax();
                    while (key <= max) {
                        iter.add(new InternalDateHistogram.Bucket(key, 0, keyed, format, reducedEmptySubAggs));
                        key = nextKey(key).longValue();
                    }
                }
            } else {
                if (bounds.getMin() != null) {
                    long key = bounds.getMin();
                    if (key < firstBucket.key) {
                        while (key < firstBucket.key) {
                            iter.add(new InternalDateHistogram.Bucket(key, 0, keyed, format, reducedEmptySubAggs));
                            key = nextKey(key).longValue();
                        }
                    }
                }
            }
        }

        // now adding the empty buckets within the actual data,
        // e.g. if the data series is [1,2,3,7] there're 3 empty buckets that will be created for 4,5,6
        while (iter.hasNext()) {
            Bucket nextBucket = list.get(iter.nextIndex());
            if (lastBucket != null) {
                long key = nextKey(lastBucket.key).longValue();
                while (key < nextBucket.key) {
                    iter.add(new InternalDateHistogram.Bucket(key, 0, keyed, format, reducedEmptySubAggs));
                    key = nextKey(key).longValue();
                }
                assert key == nextBucket.key;
            }
            lastBucket = iter.next();
        }

        // finally, adding the empty buckets *after* the actual data (based on the extended_bounds.max requested by the user)
        if (bounds != null && lastBucket != null && bounds.getMax() != null && bounds.getMax() > lastBucket.key) {
            long key = emptyBucketInfo.rounding.nextRoundingValue(lastBucket.key);
            long max = bounds.getMax();
            while (key <= max) {
                iter.add(new InternalDateHistogram.Bucket(key, 0, keyed, format, reducedEmptySubAggs));
                key = emptyBucketInfo.rounding.nextRoundingValue(key);
            }
        }
    }

    @Override
    public InternalAggregation doReduce(List<InternalAggregation> aggregations, ReduceContext reduceContext) {
        List<Bucket> reducedBuckets = reduceBuckets(aggregations, reduceContext);

        // adding empty buckets if needed
        if (minDocCount == 0) {
            addEmptyBuckets(reducedBuckets, reduceContext);
        }

        if (order == InternalOrder.KEY_ASC || reduceContext.isFinalReduce() == false) {
            // nothing to do, data are already sorted since shards return
            // sorted buckets and the merge-sort performed by reduceBuckets
            // maintains order
        } else if (order == InternalOrder.KEY_DESC) {
            // we just need to reverse here...
            List<Bucket> reverse = new ArrayList<>(reducedBuckets);
            Collections.reverse(reverse);
            reducedBuckets = reverse;
        } else {
            // sorted by sub-aggregation, need to fall back to a costly n*log(n) sort
            CollectionUtil.introSort(reducedBuckets, order.comparator());
        }

        return new InternalDateHistogram(getName(), reducedBuckets, order, minDocCount, offset, emptyBucketInfo,
                format, keyed, pipelineAggregators(), getMetaData());
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        if (keyed) {
            builder.startObject(CommonFields.BUCKETS.getPreferredName());
        } else {
            builder.startArray(CommonFields.BUCKETS.getPreferredName());
        }
        for (Bucket bucket : buckets) {
            bucket.toXContent(builder, params);
        }
        if (keyed) {
            builder.endObject();
        } else {
            builder.endArray();
        }

        if (params.paramAsBoolean(RestSearchAction.TYPED_KEYS_PARAM, false)) {
            builder.field(CommonFields.FORMAT.getPreferredName(), format);
            builder.field(CommonFields.KEYED.getPreferredName(), keyed);
            builder.field(CommonFields.OFFSET.getPreferredName(), offset);
            builder.field(CommonFields.MIN_DOC_COUNT.getPreferredName(), minDocCount);
            builder.field(CommonFields.ORDER.getPreferredName(), order);
        }

        return builder;
    }

    // HistogramFactory method impls

    @Override
    public Number getKey(MultiBucketsAggregation.Bucket bucket) {
        return ((Bucket) bucket).key;
    }

    @Override
    public Number nextKey(Number key) {
        return emptyBucketInfo.rounding.nextRoundingValue(key.longValue() - offset) + offset;
    }

    @Override
    public InternalAggregation createAggregation(List<MultiBucketsAggregation.Bucket> buckets) {
        // convert buckets to the right type
        List<Bucket> buckets2 = new ArrayList<>(buckets.size());
        for (Object b : buckets) {
            buckets2.add((Bucket) b);
        }
        buckets2 = Collections.unmodifiableList(buckets2);
        return new InternalDateHistogram(name, buckets2, order, minDocCount, offset, emptyBucketInfo, format,
                keyed, pipelineAggregators(), getMetaData());
    }

    @Override
    public Bucket createBucket(Number key, long docCount, InternalAggregations aggregations) {
        return new Bucket(key.longValue(), docCount, keyed, format, aggregations);
    }

    @Override
    protected boolean doEquals(Object obj) {
        InternalDateHistogram that = (InternalDateHistogram) obj;
        return Objects.equals(buckets, that.buckets)
                && Objects.equals(order, that.order)
                && Objects.equals(format, that.format)
                && Objects.equals(keyed, that.keyed)
                && Objects.equals(minDocCount, that.minDocCount)
                && Objects.equals(offset, that.offset)
                && Objects.equals(emptyBucketInfo, that.emptyBucketInfo);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(buckets, order, format, keyed, minDocCount, offset, emptyBucketInfo);
    }

    public static InternalDateHistogram fromXContent(XContentParser parser, String name) throws IOException {
        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser::getTokenLocation);

        InternalDateHistogram.Builder builder = new InternalDateHistogram.Builder();
        builder.setName(name);
        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            parseXContentBody(parser, builder);
        }

        parser.nextToken(); //TODO tlrx figure out why this extra one is needed
        return builder.build();
    }

    private static void parseXContentBody(XContentParser parser, InternalDateHistogram.Builder builder) throws IOException {
        XContentParser.Token token = parser.currentToken();
        String currentFieldName = parser.currentName();

        if (CommonFields.BUCKETS.getPreferredName().equals(currentFieldName)) {
            XContentParser.Token endToken = null;
            if (token == XContentParser.Token.START_OBJECT) {
                endToken = XContentParser.Token.END_OBJECT;
            } else if (token == XContentParser.Token.START_ARRAY) {
                endToken = XContentParser.Token.END_ARRAY;
            }

            if (endToken != null) {
                final List<Bucket> buckets = new ArrayList<>();
                while ((token = parser.nextToken()) != endToken) {
                    buckets.add(Bucket.fromXContent(parser));
                }
                builder.setBuckets(buckets);
            }
        } else if (CommonFields.FORMAT.getPreferredName().equals(currentFieldName)) {
            builder.setFormat(DocValueFormat.fromXContent(parser));
        } else if (CommonFields.KEYED.getPreferredName().equals(currentFieldName)) {
            if (token.isValue()) {
                builder.setKeyed(parser.booleanValue());
            }
        } else if (CommonFields.OFFSET.getPreferredName().equals(currentFieldName)) {
            if (token.isValue()) {
                builder.setOffset(parser.longValue());
            }
        } else if (CommonFields.MIN_DOC_COUNT.getPreferredName().equals(currentFieldName)) {
            if (token.isValue()) {
                builder.setMinDocCount(parser.longValue());
            }
        } else if (CommonFields.ORDER.getPreferredName().equals(currentFieldName)) {
            builder.setOrder(InternalOrder.fromXContent(parser));
        } else {
            parseCommonToXContent(parser, builder);
        }
    }

    public static class Builder extends InternalAggregation.Builder {

        private List<Bucket> buckets;
        private DocValueFormat format;
        private InternalOrder order;
        private boolean keyed;
        private long offset;
        private long minDocCount;

        public void setBuckets(List<Bucket> buckets) {
            this.buckets = buckets;
        }

        public void setFormat(DocValueFormat format) {
            this.format = format;
        }

        public void setKeyed(boolean keyed) {
            this.keyed = keyed;
        }

        public void setOffset(long offset) {
            this.offset = offset;
        }

        public void setMinDocCount(long minDocCount) {
            this.minDocCount = minDocCount;
        }

        public void setOrder(InternalOrder order) {
            this.order = order;
        }

        public InternalDateHistogram build() {
            return new InternalDateHistogram(name, buckets, order, minDocCount, offset,
            null, format, keyed, Collections.emptyList(), metaData);
        }
    }
}
