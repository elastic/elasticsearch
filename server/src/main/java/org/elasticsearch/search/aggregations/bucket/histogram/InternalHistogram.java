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
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.AggregationReduceContext;
import org.elasticsearch.search.aggregations.AggregatorReducer;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.InternalMultiBucketAggregation;
import org.elasticsearch.search.aggregations.InternalOrder;
import org.elasticsearch.search.aggregations.KeyComparable;
import org.elasticsearch.search.aggregations.bucket.BucketReducer;
import org.elasticsearch.search.aggregations.bucket.IteratorAndCurrent;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;
import org.elasticsearch.search.aggregations.support.SamplingContext;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Objects;
import java.util.function.DoubleConsumer;

/**
 * Implementation of {@link Histogram}.
 */
public class InternalHistogram extends InternalMultiBucketAggregation<InternalHistogram, InternalHistogram.Bucket>
    implements
        Histogram,
        HistogramFactory {
    public static class Bucket extends AbstractHistogramBucket implements KeyComparable<Bucket> {

        final double key;
        private final transient boolean keyed;

        public Bucket(double key, long docCount, boolean keyed, DocValueFormat format, InternalAggregations aggregations) {
            super(docCount, aggregations, format);
            this.keyed = keyed;
            this.key = key;
        }

        /**
         * Read from a stream.
         */
        public static Bucket readFrom(StreamInput in, boolean keyed, DocValueFormat format) throws IOException {
            return new Bucket(in.readDouble(), in.readVLong(), keyed, format, InternalAggregations.readFrom(in));
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null || obj.getClass() != Bucket.class) {
                return false;
            }
            Bucket that = (Bucket) obj;
            // No need to take the keyed and format parameters into account,
            // they are already stored and tested on the InternalHistogram object
            return key == that.key && docCount == that.docCount && Objects.equals(aggregations, that.aggregations);
        }

        @Override
        public int hashCode() {
            return Objects.hash(getClass(), key, docCount, aggregations);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeDouble(key);
            out.writeVLong(docCount);
            aggregations.writeTo(out);
        }

        @Override
        public String getKeyAsString() {
            return format.format(key).toString();
        }

        @Override
        public Object getKey() {
            return key;
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
            return Double.compare(key, other.key);
        }

        public boolean getKeyed() {
            return keyed;
        }

        Bucket finalizeSampling(SamplingContext samplingContext) {
            return new Bucket(
                key,
                samplingContext.scaleUp(docCount),
                keyed,
                format,
                InternalAggregations.finalizeSampling(aggregations, samplingContext)
            );
        }
    }

    public static class EmptyBucketInfo {

        final double interval, offset, minBound, maxBound;
        final InternalAggregations subAggregations;

        public EmptyBucketInfo(double interval, double offset, double minBound, double maxBound, InternalAggregations subAggregations) {
            this.interval = interval;
            this.offset = offset;
            this.minBound = minBound;
            this.maxBound = maxBound;
            this.subAggregations = subAggregations;
        }

        EmptyBucketInfo(StreamInput in) throws IOException {
            this(in.readDouble(), in.readDouble(), in.readDouble(), in.readDouble(), InternalAggregations.readFrom(in));
        }

        public void writeTo(StreamOutput out) throws IOException {
            out.writeDouble(interval);
            out.writeDouble(offset);
            out.writeDouble(minBound);
            out.writeDouble(maxBound);
            subAggregations.writeTo(out);

        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            EmptyBucketInfo that = (EmptyBucketInfo) obj;
            return interval == that.interval
                && offset == that.offset
                && minBound == that.minBound
                && maxBound == that.maxBound
                && Objects.equals(subAggregations, that.subAggregations);
        }

        @Override
        public int hashCode() {
            return Objects.hash(getClass(), interval, offset, minBound, maxBound, subAggregations);
        }
    }

    private final List<Bucket> buckets;
    private final BucketOrder order;
    private final DocValueFormat format;
    private final boolean keyed;
    private final long minDocCount;
    final EmptyBucketInfo emptyBucketInfo;

    public InternalHistogram(
        String name,
        List<Bucket> buckets,
        BucketOrder order,
        long minDocCount,
        EmptyBucketInfo emptyBucketInfo,
        DocValueFormat formatter,
        boolean keyed,
        Map<String, Object> metadata
    ) {
        super(name, metadata);
        this.buckets = buckets;
        this.order = order;
        assert (minDocCount == 0) == (emptyBucketInfo != null);
        this.minDocCount = minDocCount;
        this.emptyBucketInfo = emptyBucketInfo;
        this.format = formatter;
        this.keyed = keyed;
    }

    /**
     * Stream from a stream.
     */
    public InternalHistogram(StreamInput in) throws IOException {
        super(in);
        order = InternalOrder.Streams.readHistogramOrder(in);
        minDocCount = in.readVLong();
        if (minDocCount == 0) {
            emptyBucketInfo = new EmptyBucketInfo(in);
        } else {
            emptyBucketInfo = null;
        }
        format = in.readNamedWriteable(DocValueFormat.class);
        keyed = in.readBoolean();
        buckets = in.readCollectionAsList(stream -> Bucket.readFrom(stream, keyed, format));
        // we changed the order format in 8.13 for partial reduce, therefore we need to order them to perform merge sort
        if (in.getTransportVersion().between(TransportVersions.V_8_13_0, TransportVersions.V_8_14_0)) {
            // list is mutable by #readCollectionAsList contract
            buckets.sort(Comparator.comparingDouble(b -> b.key));
        }
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        InternalOrder.Streams.writeHistogramOrder(order, out);
        out.writeVLong(minDocCount);
        if (minDocCount == 0) {
            emptyBucketInfo.writeTo(out);
        }
        out.writeNamedWriteable(format);
        out.writeBoolean(keyed);
        out.writeCollection(buckets);
    }

    @Override
    public String getWriteableName() {
        return HistogramAggregationBuilder.NAME;
    }

    @Override
    public List<InternalHistogram.Bucket> getBuckets() {
        return Collections.unmodifiableList(buckets);
    }

    long getMinDocCount() {
        return minDocCount;
    }

    BucketOrder getOrder() {
        return order;
    }

    @Override
    public InternalHistogram create(List<Bucket> buckets) {
        if (this.buckets.equals(buckets)) {
            return this;
        }
        return new InternalHistogram(name, buckets, order, minDocCount, emptyBucketInfo, format, keyed, metadata);
    }

    @Override
    public Bucket createBucket(InternalAggregations aggregations, Bucket prototype) {
        if (prototype.aggregations.equals(aggregations)) {
            return prototype;
        }
        return new Bucket(prototype.key, prototype.docCount, prototype.keyed, prototype.format, aggregations);
    }

    private List<Bucket> reduceBuckets(PriorityQueue<IteratorAndCurrent<Bucket>> pq, AggregationReduceContext reduceContext) {
        List<Bucket> reducedBuckets = new ArrayList<>();
        if (pq.size() > 0) {
            // list of buckets coming from different shards that have the same key
            List<Bucket> currentBuckets = new ArrayList<>();
            double key = pq.top().current().key;

            do {
                final IteratorAndCurrent<Bucket> top = pq.top();

                if (Double.compare(top.current().key, key) != 0) {
                    // The key changes, reduce what we already buffered and reset the buffer for current buckets.
                    // Using Double.compare instead of != to handle NaN correctly.
                    final Bucket reduced = reduceBucket(currentBuckets, reduceContext);
                    maybeAddBucket(reduceContext, reducedBuckets, reduced);
                    currentBuckets.clear();
                    key = top.current().key;
                }

                currentBuckets.add(top.current());

                if (top.hasNext()) {
                    top.next();
                    assert Double.compare(top.current().key, key) > 0 : "shards must return data sorted by key";
                    pq.updateTop();
                } else {
                    pq.pop();
                }
            } while (pq.size() > 0);

            if (currentBuckets.isEmpty() == false) {
                final Bucket reduced = reduceBucket(currentBuckets, reduceContext);
                maybeAddBucket(reduceContext, reducedBuckets, reduced);
            }
        }
        return reducedBuckets;
    }

    private void maybeAddBucket(AggregationReduceContext reduceContext, List<Bucket> reducedBuckets, Bucket reduced) {
        if (reduced.getDocCount() >= minDocCount || reduceContext.isFinalReduce() == false) {
            reduceContext.consumeBucketsAndMaybeBreak(1);
            reducedBuckets.add(reduced);
        } else {
            reduceContext.consumeBucketsAndMaybeBreak(-countInnerBucket(reduced));
        }
    }

    private Bucket reduceBucket(List<Bucket> buckets, AggregationReduceContext context) {
        assert buckets.isEmpty() == false;
        try (BucketReducer<Bucket> reducer = new BucketReducer<>(buckets.get(0), context, buckets.size())) {
            for (Bucket bucket : buckets) {
                reducer.accept(bucket);
            }
            return createBucket(reducer.getProto().key, reducer.getDocCount(), reducer.getAggregations());
        }
    }

    private double nextKey(double key) {
        return round(key + emptyBucketInfo.interval + emptyBucketInfo.interval / 2);
    }

    private double round(double key) {
        return Math.floor((key - emptyBucketInfo.offset) / emptyBucketInfo.interval) * emptyBucketInfo.interval + emptyBucketInfo.offset;
    }

    private void addEmptyBuckets(List<Bucket> list, AggregationReduceContext reduceContext) {
        /*
         * Make sure we have space for the empty buckets we're going to add by
         * counting all of the empties we plan to add and firing them into
         * consumeBucketsAndMaybeBreak.
         */
        class Counter implements DoubleConsumer {
            private int size = 0;

            @Override
            public void accept(double key) {
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

        /*
         * Now that we're sure we have space we allocate all the buckets.
         */
        InternalAggregations reducedEmptySubAggs = InternalAggregations.reduce(
            Collections.singletonList(emptyBucketInfo.subAggregations),
            reduceContext
        );
        ListIterator<Bucket> iter = list.listIterator();
        iterateEmptyBuckets(list, iter, new DoubleConsumer() {
            private int size;

            @Override
            public void accept(double key) {
                size++;
                if (size >= REPORT_EMPTY_EVERY) {
                    reduceContext.consumeBucketsAndMaybeBreak(size);
                    size = 0;
                }
                iter.add(new Bucket(key, 0, keyed, format, reducedEmptySubAggs));
            }
        });
    }

    private void iterateEmptyBuckets(List<Bucket> list, ListIterator<Bucket> iter, DoubleConsumer onBucket) {
        if (iter.hasNext() == false) {
            // fill with empty buckets
            for (double key = round(emptyBucketInfo.minBound); key <= emptyBucketInfo.maxBound; key = nextKey(key)) {
                onBucket.accept(key);
            }
            return;
        }
        Bucket first = list.get(iter.nextIndex());
        if (Double.isFinite(emptyBucketInfo.minBound)) {
            // fill with empty buckets until the first key
            for (double key = round(emptyBucketInfo.minBound); key < first.key; key = nextKey(key)) {
                onBucket.accept(key);
            }
        }

        // now adding the empty buckets within the actual data,
        // e.g. if the data series is [1,2,3,7] there're 3 empty buckets that will be created for 4,5,6
        Bucket lastBucket = null;
        do {
            Bucket nextBucket = list.get(iter.nextIndex());
            if (lastBucket != null) {
                double key = nextKey(lastBucket.key);
                while (key < nextBucket.key) {
                    onBucket.accept(key);
                    key = nextKey(key);
                }
                assert key == nextBucket.key || Double.isNaN(nextBucket.key) : "key: " + key + ", nextBucket.key: " + nextBucket.key;
            }
            lastBucket = iter.next();
        } while (iter.hasNext());

        // finally, adding the empty buckets *after* the actual data (based on the extended_bounds.max requested by the user)
        for (double key = nextKey(lastBucket.key); key <= emptyBucketInfo.maxBound; key = nextKey(key)) {
            onBucket.accept(key);
        }
    }

    @Override
    protected AggregatorReducer getLeaderReducer(AggregationReduceContext reduceContext, int size) {
        return new AggregatorReducer() {
            final PriorityQueue<IteratorAndCurrent<Bucket>> pq = new PriorityQueue<>(size) {
                @Override
                protected boolean lessThan(IteratorAndCurrent<Bucket> a, IteratorAndCurrent<Bucket> b) {
                    return Double.compare(a.current().key, b.current().key) < 0;
                }
            };

            @Override
            public void accept(InternalAggregation aggregation) {
                final InternalHistogram histogram = (InternalHistogram) aggregation;
                if (histogram.buckets.isEmpty() == false) {
                    pq.add(new IteratorAndCurrent<>(histogram.buckets.iterator()));
                }
            }

            @Override
            public InternalAggregation get() {
                List<Bucket> reducedBuckets = reduceBuckets(pq, reduceContext);
                if (reduceContext.isFinalReduce()) {
                    if (minDocCount == 0) {
                        addEmptyBuckets(reducedBuckets, reduceContext);
                    }
                    if (InternalOrder.isKeyDesc(order)) {
                        // we just need to reverse here...
                        Collections.reverse(reducedBuckets);
                    } else if (InternalOrder.isKeyAsc(order) == false) {
                        // nothing to do when sorting by key ascending, as data is already sorted since shards return
                        // sorted buckets and the merge-sort performed by reduceBuckets maintains order.
                        // otherwise, sorted by compound order or sub-aggregation, we need to fall back to a costly n*log(n) sort
                        CollectionUtil.introSort(reducedBuckets, order.comparator());
                    }
                }
                if (reducedBuckets.equals(buckets)) {
                    return InternalHistogram.this;
                }
                return new InternalHistogram(getName(), reducedBuckets, order, minDocCount, emptyBucketInfo, format, keyed, getMetadata());
            }
        };
    }

    @Override
    public InternalAggregation finalizeSampling(SamplingContext samplingContext) {
        return new InternalHistogram(
            getName(),
            buckets.stream().map(b -> b.finalizeSampling(samplingContext)).toList(),
            order,
            minDocCount,
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
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public InternalAggregation createAggregation(List<MultiBucketsAggregation.Bucket> buckets) {
        return new InternalHistogram(name, (List) buckets, order, minDocCount, emptyBucketInfo, format, keyed, getMetadata());
    }

    @Override
    public Bucket createBucket(Number key, long docCount, InternalAggregations aggregations) {
        return new Bucket(key.doubleValue(), docCount, keyed, format, aggregations);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        if (super.equals(obj) == false) return false;

        InternalHistogram that = (InternalHistogram) obj;
        return Objects.equals(buckets, that.buckets)
            && Objects.equals(emptyBucketInfo, that.emptyBucketInfo)
            && Objects.equals(format, that.format)
            && Objects.equals(keyed, that.keyed)
            && Objects.equals(minDocCount, that.minDocCount)
            && Objects.equals(order, that.order);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), buckets, emptyBucketInfo, format, keyed, minDocCount, order);
    }
}
