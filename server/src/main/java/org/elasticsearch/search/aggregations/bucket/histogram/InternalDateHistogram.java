/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.aggregations.bucket.histogram;

import org.apache.lucene.util.CollectionUtil;
import org.apache.lucene.util.PriorityQueue;
import org.elasticsearch.common.Rounding;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.InternalMultiBucketAggregation;
import org.elasticsearch.search.aggregations.InternalOrder;
import org.elasticsearch.search.aggregations.KeyComparable;
import org.elasticsearch.search.aggregations.bucket.IteratorAndCurrent;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Objects;
import java.util.function.LongConsumer;

/**
 * Implementation of {@link Histogram}.
 */
public final class InternalDateHistogram extends InternalMultiBucketAggregation<InternalDateHistogram, InternalDateHistogram.Bucket>
    implements
        Histogram,
        HistogramFactory {

    public static class Bucket extends InternalMultiBucketAggregation.InternalBucket implements Histogram.Bucket, KeyComparable<Bucket> {

        final long key;
        final long docCount;
        final InternalAggregations aggregations;
        private final transient boolean keyed;
        protected final transient DocValueFormat format;

        public Bucket(long key, long docCount, boolean keyed, DocValueFormat format, InternalAggregations aggregations) {
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
            aggregations = InternalAggregations.readFrom(in);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null || obj.getClass() != InternalDateHistogram.Bucket.class) {
                return false;
            }
            InternalDateHistogram.Bucket that = (InternalDateHistogram.Bucket) obj;
            // No need to take the keyed and format parameters into account,
            // they are already stored and tested on the InternalDateHistogram object
            return key == that.key && docCount == that.docCount && Objects.equals(aggregations, that.aggregations);
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
            return format.format(key).toString();
        }

        @Override
        public Object getKey() {
            return Instant.ofEpochMilli(key).atZone(ZoneOffset.UTC);
        }

        @Override
        public long getDocCount() {
            return docCount;
        }

        @Override
        public Aggregations getAggregations() {
            return aggregations;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            String keyAsString = format.format(key).toString();
            if (keyed) {
                builder.startObject(keyAsString);
            } else {
                builder.startObject();
            }
            if (format != DocValueFormat.RAW) {
                builder.field(CommonFields.KEY_AS_STRING.getPreferredName(), keyAsString);
            }
            builder.field(CommonFields.KEY.getPreferredName(), key);
            builder.field(CommonFields.DOC_COUNT.getPreferredName(), docCount);
            aggregations.toXContentInternal(builder, params);
            builder.endObject();
            return builder;
        }

        @Override
        public int compareKey(Bucket other) {
            return Long.compare(key, other.key);
        }

        public DocValueFormat getFormatter() {
            return format;
        }

        public boolean getKeyed() {
            return keyed;
        }
    }

    static class EmptyBucketInfo {

        final Rounding rounding;
        final InternalAggregations subAggregations;
        final LongBounds bounds;

        EmptyBucketInfo(Rounding rounding, InternalAggregations subAggregations) {
            this(rounding, subAggregations, null);
        }

        EmptyBucketInfo(Rounding rounding, InternalAggregations subAggregations, LongBounds bounds) {
            this.rounding = rounding;
            this.subAggregations = subAggregations;
            this.bounds = bounds;
        }

        EmptyBucketInfo(StreamInput in) throws IOException {
            rounding = Rounding.read(in);
            subAggregations = InternalAggregations.readFrom(in);
            bounds = in.readOptionalWriteable(LongBounds::new);
        }

        void writeTo(StreamOutput out) throws IOException {
            rounding.writeTo(out);
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
    private final BucketOrder order;
    private final DocValueFormat format;
    private final boolean keyed;
    private final long minDocCount;
    private final long offset;
    final EmptyBucketInfo emptyBucketInfo;

    InternalDateHistogram(
        String name,
        List<Bucket> buckets,
        BucketOrder order,
        long minDocCount,
        long offset,
        EmptyBucketInfo emptyBucketInfo,
        DocValueFormat formatter,
        boolean keyed,
        Map<String, Object> metadata
    ) {
        super(name, metadata);
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
        order = InternalOrder.Streams.readHistogramOrder(in, false);
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
        InternalOrder.Streams.writeHistogramOrder(order, out, false);
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
    public List<InternalDateHistogram.Bucket> getBuckets() {
        return Collections.unmodifiableList(buckets);
    }

    DocValueFormat getFormatter() {
        return format;
    }

    long getMinDocCount() {
        return minDocCount;
    }

    long getOffset() {
        return offset;
    }

    BucketOrder getOrder() {
        return order;
    }

    @Override
    public InternalDateHistogram create(List<Bucket> buckets) {
        return new InternalDateHistogram(name, buckets, order, minDocCount, offset, emptyBucketInfo, format, keyed, metadata);
    }

    @Override
    public Bucket createBucket(InternalAggregations aggregations, Bucket prototype) {
        return new Bucket(prototype.key, prototype.docCount, prototype.keyed, prototype.format, aggregations);
    }

    private List<Bucket> reduceBuckets(List<InternalAggregation> aggregations, ReduceContext reduceContext) {
        final PriorityQueue<IteratorAndCurrent<Bucket>> pq = new PriorityQueue<IteratorAndCurrent<Bucket>>(aggregations.size()) {
            @Override
            protected boolean lessThan(IteratorAndCurrent<Bucket> a, IteratorAndCurrent<Bucket> b) {
                return a.current().key < b.current().key;
            }
        };
        for (InternalAggregation aggregation : aggregations) {
            InternalDateHistogram histogram = (InternalDateHistogram) aggregation;
            if (histogram.buckets.isEmpty() == false) {
                pq.add(new IteratorAndCurrent<Bucket>(histogram.buckets.iterator()));
            }
        }

        List<Bucket> reducedBuckets = new ArrayList<>();
        if (pq.size() > 0) {
            // list of buckets coming from different shards that have the same key
            List<Bucket> currentBuckets = new ArrayList<>();
            double key = pq.top().current().key;

            do {
                final IteratorAndCurrent<Bucket> top = pq.top();

                if (top.current().key != key) {
                    // the key changes, reduce what we already buffered and reset the buffer for current buckets
                    final Bucket reduced = reduceBucket(currentBuckets, reduceContext);
                    if (reduced.getDocCount() >= minDocCount || reduceContext.isFinalReduce() == false) {
                        reducedBuckets.add(reduced);
                    }
                    currentBuckets.clear();
                    key = top.current().key;
                }

                currentBuckets.add(top.current());

                if (top.hasNext()) {
                    top.next();
                    assert top.current().key > key : "shards must return data sorted by key";
                    pq.updateTop();
                } else {
                    pq.pop();
                }
            } while (pq.size() > 0);

            if (currentBuckets.isEmpty() == false) {
                final Bucket reduced = reduceBucket(currentBuckets, reduceContext);
                if (reduced.getDocCount() >= minDocCount || reduceContext.isFinalReduce() == false) {
                    reducedBuckets.add(reduced);
                }
            }
        }

        return reducedBuckets;
    }

    /**
     * Reduce a list of same-keyed buckets (from multiple shards) to a single bucket. This
     * requires all buckets to have the same key.
     */
    @Override
    protected Bucket reduceBucket(List<Bucket> buckets, ReduceContext context) {
        assert buckets.size() > 0;
        List<InternalAggregations> aggregations = new ArrayList<>(buckets.size());
        long docCount = 0;
        for (Bucket bucket : buckets) {
            docCount += bucket.docCount;
            aggregations.add((InternalAggregations) bucket.getAggregations());
        }
        InternalAggregations aggs = InternalAggregations.reduce(aggregations, context);
        return createBucket(buckets.get(0).key, docCount, aggs);
    }

    /**
     * When we pre-count the empty buckets we report them periodically
     * because you can configure the date_histogram to create an astounding
     * number of buckets. It'd take a while to count that high only to abort.
     * So we report every couple thousand buckets. It's be simpler to report
     * every single bucket we plan to allocate one at a time but that'd cause
     * needless overhead on the circuit breakers. Counting a couple thousand
     * buckets is plenty fast to fail this quickly in pathological cases and
     * plenty large to keep the overhead minimal.
     */
    private static final int REPORT_EMPTY_EVERY = 10_000;

    private void addEmptyBuckets(List<Bucket> list, ReduceContext reduceContext) {
        /*
         * Make sure we have space for the empty buckets we're going to add by
         * counting all of the empties we plan to add and firing them into
         * consumeBucketsAndMaybeBreak.
         */
        class Counter implements LongConsumer {
            private int size = list.size();

            @Override
            public void accept(long key) {
                size++;
                if (size >= REPORT_EMPTY_EVERY) {
                    reduceContext.consumeBucketsAndMaybeBreak(size);
                    size = 0;
                }
            }
        }
        Counter counter = new Counter();
        iterateEmptyBuckets(list, list.listIterator(), counter);
        reduceContext.consumeBucketsAndMaybeBreak(counter.size);

        InternalAggregations reducedEmptySubAggs = InternalAggregations.reduce(
            org.elasticsearch.core.List.of(emptyBucketInfo.subAggregations),
            reduceContext
        );
        ListIterator<Bucket> iter = list.listIterator();
        iterateEmptyBuckets(list, iter, key -> iter.add(new InternalDateHistogram.Bucket(key, 0, keyed, format, reducedEmptySubAggs)));
    }

    private void iterateEmptyBuckets(List<Bucket> list, ListIterator<Bucket> iter, LongConsumer onBucket) {
        LongBounds bounds = emptyBucketInfo.bounds;

        // first adding all the empty buckets *before* the actual data (based on the extended_bounds.min the user requested)

        if (bounds != null) {
            Bucket firstBucket = iter.hasNext() ? list.get(iter.nextIndex()) : null;
            if (firstBucket == null) {
                if (bounds.getMin() != null && bounds.getMax() != null) {
                    long key = bounds.getMin() + offset;
                    long max = bounds.getMax() + offset;
                    while (key <= max) {
                        onBucket.accept(key);
                        key = nextKey(key).longValue();
                    }
                }
            } else {
                if (bounds.getMin() != null) {
                    long key = bounds.getMin() + offset;
                    if (key < firstBucket.key) {
                        while (key < firstBucket.key) {
                            onBucket.accept(key);
                            key = nextKey(key).longValue();
                        }
                    }
                }
            }
        }

        Bucket lastBucket = null;
        // now adding the empty buckets within the actual data,
        // e.g. if the data series is [1,2,3,7] there're 3 empty buckets that will be created for 4,5,6
        while (iter.hasNext()) {
            Bucket nextBucket = list.get(iter.nextIndex());
            if (lastBucket != null) {
                long key = nextKey(lastBucket.key).longValue();
                while (key < nextBucket.key) {
                    onBucket.accept(key);
                    key = nextKey(key).longValue();
                }
                assert key == nextBucket.key : "key: " + key + ", nextBucket.key: " + nextBucket.key;
            }
            lastBucket = iter.next();
        }

        // finally, adding the empty buckets *after* the actual data (based on the extended_bounds.max requested by the user)
        if (bounds != null && lastBucket != null && bounds.getMax() != null && bounds.getMax() + offset > lastBucket.key) {
            long key = nextKey(lastBucket.key).longValue();
            long max = bounds.getMax() + offset;
            while (key <= max) {
                onBucket.accept(key);
                key = nextKey(key).longValue();
            }
        }
    }

    @Override
    public InternalAggregation reduce(List<InternalAggregation> aggregations, ReduceContext reduceContext) {
        List<Bucket> reducedBuckets = reduceBuckets(aggregations, reduceContext);
        boolean alreadyAccountedForBuckets = false;
        if (reduceContext.isFinalReduce()) {
            if (minDocCount == 0) {
                addEmptyBuckets(reducedBuckets, reduceContext);
                alreadyAccountedForBuckets = true;
            }
            if (InternalOrder.isKeyDesc(order)) {
                // we just need to reverse here...
                List<Bucket> reverse = new ArrayList<>(reducedBuckets);
                Collections.reverse(reverse);
                reducedBuckets = reverse;
            } else if (InternalOrder.isKeyAsc(order) == false) {
                // nothing to do when sorting by key ascending, as data is already sorted since shards return
                // sorted buckets and the merge-sort performed by reduceBuckets maintains order.
                // otherwise, sorted by compound order or sub-aggregation, we need to fall back to a costly n*log(n) sort
                CollectionUtil.introSort(reducedBuckets, order.comparator());
            }
        }
        if (false == alreadyAccountedForBuckets) {
            reduceContext.consumeBucketsAndMaybeBreak(reducedBuckets.size());
        }
        return new InternalDateHistogram(
            getName(),
            reducedBuckets,
            order,
            minDocCount,
            offset,
            emptyBucketInfo,
            format,
            keyed,
            getMetadata()
        );
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
        return new InternalDateHistogram(name, buckets2, order, minDocCount, offset, emptyBucketInfo, format, keyed, getMetadata());
    }

    @Override
    public Bucket createBucket(Number key, long docCount, InternalAggregations aggregations) {
        return new Bucket(key.longValue(), docCount, keyed, format, aggregations);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        if (super.equals(obj) == false) return false;

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
    public int hashCode() {
        return Objects.hash(super.hashCode(), buckets, order, format, keyed, minDocCount, offset, emptyBucketInfo);
    }
}
