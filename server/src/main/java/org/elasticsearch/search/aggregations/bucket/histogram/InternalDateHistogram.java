/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.aggregations.bucket.histogram;

import org.apache.lucene.util.CollectionUtil;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.Rounding;
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
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
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
            return Long.compare(key, other.key);
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

    static class EmptyBucketInfo {

        final Rounding rounding;
        final InternalAggregations subAggregations;
        final LongBounds bounds;

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
    private final boolean downsampledResultsOffset;
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
        boolean downsampledResultsOffset,
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
        this.downsampledResultsOffset = downsampledResultsOffset;
    }

    boolean versionSupportsDownsamplingTimezone(TransportVersion version) {
        return version.onOrAfter(TransportVersions.DATE_HISTOGRAM_SUPPORT_DOWNSAMPLED_TZ)
            || version.between(
                TransportVersions.DATE_HISTOGRAM_SUPPORT_DOWNSAMPLED_TZ_8_12_PATCH,
                TransportVersions.NODE_STATS_REQUEST_SIMPLIFIED
            );
    }

    /**
     * Stream from a stream.
     */
    public InternalDateHistogram(StreamInput in) throws IOException {
        super(in);
        order = InternalOrder.Streams.readHistogramOrder(in);
        minDocCount = in.readVLong();
        if (minDocCount == 0) {
            emptyBucketInfo = new EmptyBucketInfo(in);
        } else {
            emptyBucketInfo = null;
        }
        offset = in.readLong();
        format = in.readNamedWriteable(DocValueFormat.class);
        keyed = in.readBoolean();
        if (versionSupportsDownsamplingTimezone(in.getTransportVersion())) {
            downsampledResultsOffset = in.readBoolean();
        } else {
            downsampledResultsOffset = false;
        }
        buckets = in.readCollectionAsList(stream -> new Bucket(stream, keyed, format));
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        InternalOrder.Streams.writeHistogramOrder(order, out);
        out.writeVLong(minDocCount);
        if (minDocCount == 0) {
            emptyBucketInfo.writeTo(out);
        }
        out.writeLong(offset);
        out.writeNamedWriteable(format);
        out.writeBoolean(keyed);
        if (versionSupportsDownsamplingTimezone(out.getTransportVersion())) {
            out.writeBoolean(downsampledResultsOffset);
        }
        out.writeCollection(buckets);
    }

    @Override
    public String getWriteableName() {
        return DateHistogramAggregationBuilder.NAME;
    }

    @Override
    public List<InternalDateHistogram.Bucket> getBuckets() {
        return Collections.unmodifiableList(buckets);
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
        return new InternalDateHistogram(
            name,
            buckets,
            order,
            minDocCount,
            offset,
            emptyBucketInfo,
            format,
            keyed,
            downsampledResultsOffset,
            metadata
        );
    }

    @Override
    public Bucket createBucket(InternalAggregations aggregations, Bucket prototype) {
        return new Bucket(prototype.key, prototype.docCount, prototype.keyed, prototype.format, aggregations);
    }

    private void addEmptyBuckets(List<Bucket> list, AggregationReduceContext reduceContext) {
        /*
         * Make sure we have space for the empty buckets we're going to add by
         * counting all of the empties we plan to add and firing them into
         * consumeBucketsAndMaybeBreak.
         */
        class Counter implements LongConsumer {
            private int size;

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

        InternalAggregations reducedEmptySubAggs = InternalAggregations.reduce(List.of(emptyBucketInfo.subAggregations), reduceContext);
        ListIterator<Bucket> iter = list.listIterator();
        iterateEmptyBuckets(list, iter, new LongConsumer() {
            private int size = 0;

            @Override
            public void accept(long key) {
                size++;
                if (size >= REPORT_EMPTY_EVERY) {
                    reduceContext.consumeBucketsAndMaybeBreak(size);
                    size = 0;
                }
                iter.add(new InternalDateHistogram.Bucket(key, 0, keyed, format, reducedEmptySubAggs));
            }
        });
    }

    private void iterateEmptyBuckets(List<Bucket> list, ListIterator<Bucket> iter, LongConsumer onBucket) {
        LongBounds bounds = emptyBucketInfo.bounds;
        Rounding.Prepared prepared = null;
        if (bounds != null && list.isEmpty() == false) {
            long min = bounds.getMin() != null ? Math.min(bounds.getMin() + offset, list.get(0).key) : list.get(0).key;
            long max = bounds.getMax() != null
                ? Math.max(bounds.getMax() + offset, list.get(list.size() - 1).key)
                : list.get(list.size() - 1).key;
            prepared = createPrepared(min, max);
        } else if (bounds != null && bounds.getMin() != null && bounds.getMax() != null) {
            prepared = createPrepared(bounds.getMin() + offset, bounds.getMax() + offset);
        } else if (list.isEmpty() == false) {
            prepared = createPrepared(list.get(0).key, list.get(list.size() - 1).key);
        }

        // first adding all the empty buckets *before* the actual data (based on the extended_bounds.min the user requested)

        if (bounds != null) {
            Bucket firstBucket = iter.hasNext() ? list.get(iter.nextIndex()) : null;
            if (firstBucket == null) {
                if (bounds.getMin() != null && bounds.getMax() != null) {
                    long key = bounds.getMin() + offset;
                    long max = bounds.getMax() + offset;
                    while (key <= max) {
                        onBucket.accept(key);
                        key = nextKey(prepared, key);
                    }
                }
            } else {
                if (bounds.getMin() != null) {
                    long key = bounds.getMin() + offset;
                    if (key < firstBucket.key) {
                        while (key < firstBucket.key) {
                            onBucket.accept(key);
                            key = nextKey(prepared, key);
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
                long key = nextKey(prepared, lastBucket.key);
                while (key < nextBucket.key) {
                    onBucket.accept(key);
                    key = nextKey(prepared, key);
                }
                assert key == nextBucket.key : "key: " + key + ", nextBucket.key: " + nextBucket.key;
            }
            lastBucket = iter.next();
        }

        // finally, adding the empty buckets *after* the actual data (based on the extended_bounds.max requested by the user)
        if (bounds != null && lastBucket != null && bounds.getMax() != null && bounds.getMax() + offset > lastBucket.key) {
            long key = nextKey(prepared, lastBucket.key);
            long max = bounds.getMax() + offset;
            while (key <= max) {
                onBucket.accept(key);
                key = nextKey(prepared, key);
            }
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
                    return InternalDateHistogram.this.createBucket(key, docCount, aggregations);
                }
            };

            @Override
            public void accept(InternalAggregation aggregation) {
                InternalDateHistogram dateHistogram = (InternalDateHistogram) aggregation;
                for (Bucket bucket : dateHistogram.buckets) {
                    reducer.accept(bucket.key, bucket);
                }
            }

            @Override
            public InternalAggregation get() {
                List<Bucket> reducedBuckets = reducer.get();
                if (reduceContext.isFinalReduce()) {
                    reducedBuckets.sort(Comparator.comparingLong(b -> b.key));
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
                return new InternalDateHistogram(
                    getName(),
                    reducedBuckets,
                    order,
                    minDocCount,
                    offset,
                    emptyBucketInfo,
                    format,
                    keyed,
                    downsampledResultsOffset,
                    getMetadata()
                );
            }

            @Override
            public void close() {
                Releasables.close(reducer);
            }
        };
    }

    @Override
    public InternalAggregation finalizeSampling(SamplingContext samplingContext) {
        return new InternalDateHistogram(
            getName(),
            buckets.stream().map(b -> b.finalizeSampling(samplingContext)).toList(),
            order,
            minDocCount,
            offset,
            emptyBucketInfo,
            format,
            keyed,
            downsampledResultsOffset,
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
        if (downsampledResultsOffset) {
            // Indicates that the dates reported in the buckets over downsampled indexes are offset
            // to match the intervals at UTC, since downsampling always uses UTC-based intervals
            // to calculate aggregated values.
            builder.field("downsampled_results_offset", Boolean.TRUE);
        }
        return builder;
    }

    // HistogramFactory method impls

    @Override
    public Number getKey(MultiBucketsAggregation.Bucket bucket) {
        return ((Bucket) bucket).key;
    }

    Rounding.Prepared createPrepared(long min, long max) {
        return emptyBucketInfo.rounding.prepare(min - offset, max - offset);
    }

    /** Given a key returned by {@link #getKey}, compute the lowest key that is
     *  greater than it. */
    long nextKey(Rounding.Prepared prepared, long key) {
        return prepared.nextRoundingValue(key - offset) + offset;
    }

    @Override
    public InternalAggregation createAggregation(List<MultiBucketsAggregation.Bucket> buckets) {
        // convert buckets to the right type
        List<Bucket> buckets2 = new ArrayList<>(buckets.size());
        for (Object b : buckets) {
            buckets2.add((Bucket) b);
        }
        buckets2 = Collections.unmodifiableList(buckets2);
        return new InternalDateHistogram(
            name,
            buckets2,
            order,
            minDocCount,
            offset,
            emptyBucketInfo,
            format,
            keyed,
            downsampledResultsOffset,
            getMetadata()
        );
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
