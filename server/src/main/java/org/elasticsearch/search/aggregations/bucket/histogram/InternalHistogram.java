/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.aggregations.bucket.histogram;

import org.apache.lucene.util.CollectionUtil;
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.AggregationReduceContext;
import org.elasticsearch.search.aggregations.AggregatorReducer;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.InternalMultiBucketAggregation;
import org.elasticsearch.search.aggregations.InternalOrder;
import org.elasticsearch.search.aggregations.KeyComparable;
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
    public static class Bucket extends InternalMultiBucketAggregation.InternalBucket implements Histogram.Bucket, KeyComparable<Bucket> {

        final double key;
        final long docCount;
        final InternalAggregations aggregations;
        private final transient boolean keyed;
        protected final transient DocValueFormat format;

        public Bucket(double key, long docCount, boolean keyed, DocValueFormat format, InternalAggregations aggregations) {
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
            key = in.readDouble();
            docCount = in.readVLong();
            aggregations = InternalAggregations.readFrom(in);
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
        public long getDocCount() {
            return docCount;
        }

        @Override
        public InternalAggregations getAggregations() {
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
            return Double.compare(key, other.key);
        }

        public DocValueFormat getFormatter() {
            return format;
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
        buckets = in.readCollectionAsList(stream -> new Bucket(stream, keyed, format));
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
        return new InternalHistogram(name, buckets, order, minDocCount, emptyBucketInfo, format, keyed, metadata);
    }

    @Override
    public Bucket createBucket(InternalAggregations aggregations, Bucket prototype) {
        return new Bucket(prototype.key, prototype.docCount, prototype.keyed, prototype.format, aggregations);
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

            final LongKeyedMultiBucketsAggregatorReducer<Bucket> reducer = new LongKeyedMultiBucketsAggregatorReducer<>(
                reduceContext,
                size,
                minDocCount
            ) {
                @Override
                protected Bucket createBucket(long key, long docCount, InternalAggregations aggregations) {
                    return InternalHistogram.this.createBucket(NumericUtils.sortableLongToDouble(key), docCount, aggregations);
                }
            };

            @Override
            public void accept(InternalAggregation aggregation) {
                InternalHistogram histogram = (InternalHistogram) aggregation;
                for (Bucket bucket : histogram.buckets) {
                    reducer.accept(NumericUtils.doubleToSortableLong(bucket.key), bucket);
                }
            }

            @Override
            public InternalAggregation get() {
                List<Bucket> reducedBuckets = reducer.get();
                if (reduceContext.isFinalReduce()) {
                    reducedBuckets.sort(Comparator.comparingDouble(b -> b.key));
                    if (minDocCount == 0) {
                        addEmptyBuckets(reducedBuckets, reduceContext);
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
                return new InternalHistogram(getName(), reducedBuckets, order, minDocCount, emptyBucketInfo, format, keyed, getMetadata());
            }

            @Override
            public void close() {
                Releasables.close(reducer);
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
    public InternalAggregation createAggregation(List<MultiBucketsAggregation.Bucket> buckets) {
        // convert buckets to the right type
        List<Bucket> buckets2 = new ArrayList<>(buckets.size());
        for (Object b : buckets) {
            buckets2.add((Bucket) b);
        }
        buckets2 = Collections.unmodifiableList(buckets2);
        return new InternalHistogram(name, buckets2, order, minDocCount, emptyBucketInfo, format, keyed, getMetadata());
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
