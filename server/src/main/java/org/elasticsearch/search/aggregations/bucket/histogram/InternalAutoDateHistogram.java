/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.aggregations.bucket.histogram;

import org.apache.lucene.util.PriorityQueue;
import org.elasticsearch.common.Rounding;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.InternalMultiBucketAggregation;
import org.elasticsearch.search.aggregations.KeyComparable;
import org.elasticsearch.search.aggregations.bucket.IteratorAndCurrent;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;
import org.elasticsearch.search.aggregations.bucket.histogram.AutoDateHistogramAggregationBuilder.RoundingInfo;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Objects;

/**
 * Implementation of {@link Histogram}.
 */
public final class InternalAutoDateHistogram extends
        InternalMultiBucketAggregation<InternalAutoDateHistogram, InternalAutoDateHistogram.Bucket> implements Histogram, HistogramFactory {

    public static class Bucket extends InternalMultiBucketAggregation.InternalBucket implements Histogram.Bucket, KeyComparable<Bucket> {

        final long key;
        final long docCount;
        final InternalAggregations aggregations;
        protected final transient DocValueFormat format;

        public Bucket(long key, long docCount, DocValueFormat format,
                InternalAggregations aggregations) {
            this.format = format;
            this.key = key;
            this.docCount = docCount;
            this.aggregations = aggregations;
        }

        /**
         * Read from a stream.
         */
        public Bucket(StreamInput in, DocValueFormat format) throws IOException {
            this.format = format;
            key = in.readLong();
            docCount = in.readVLong();
            aggregations = InternalAggregations.readFrom(in);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null || obj.getClass() != InternalAutoDateHistogram.Bucket.class) {
                return false;
            }
            InternalAutoDateHistogram.Bucket that = (InternalAutoDateHistogram.Bucket) obj;
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
            builder.startObject();
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
    }

    static class BucketInfo {

        final RoundingInfo[] roundingInfos;
        final int roundingIdx;
        final InternalAggregations emptySubAggregations;

        BucketInfo(RoundingInfo[] roundings, int roundingIdx, InternalAggregations subAggregations) {
            this.roundingInfos = roundings;
            this.roundingIdx = roundingIdx;
            this.emptySubAggregations = subAggregations;
        }

        BucketInfo(StreamInput in) throws IOException {
            int size = in.readVInt();
            roundingInfos = new RoundingInfo[size];
            for (int i = 0; i < size; i++) {
                roundingInfos[i] = new RoundingInfo(in);
            }
            roundingIdx = in.readVInt();
            emptySubAggregations = InternalAggregations.readFrom(in);
        }

        void writeTo(StreamOutput out) throws IOException {
            out.writeVInt(roundingInfos.length);
            for (RoundingInfo roundingInfo : roundingInfos) {
                roundingInfo.writeTo(out);
            }
            out.writeVInt(roundingIdx);
            emptySubAggregations.writeTo(out);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            BucketInfo that = (BucketInfo) obj;
            return Objects.deepEquals(roundingInfos, that.roundingInfos)
                    && Objects.equals(roundingIdx, that.roundingIdx)
                    && Objects.equals(emptySubAggregations, that.emptySubAggregations);
        }

        @Override
        public int hashCode() {
            return Objects.hash(getClass(), Arrays.hashCode(roundingInfos), roundingIdx, emptySubAggregations);
        }
    }

    private final List<Bucket> buckets;
    private final DocValueFormat format;
    private final BucketInfo bucketInfo;
    private final int targetBuckets;
    /**
     * The interval within the rounding that the buckets are using.
     */
    private final long bucketInnerInterval;

    InternalAutoDateHistogram(String name, List<Bucket> buckets, int targetBuckets, BucketInfo emptyBucketInfo, DocValueFormat formatter,
                              Map<String, Object> metadata, long bucketInnerInterval) {
        super(name, metadata);
        this.buckets = buckets;
        this.bucketInfo = emptyBucketInfo;
        this.format = formatter;
        this.targetBuckets = targetBuckets;
        this.bucketInnerInterval = bucketInnerInterval;
    }

    /**
     * Stream from a stream.
     */
    public InternalAutoDateHistogram(StreamInput in) throws IOException {
        super(in);
        bucketInfo = new BucketInfo(in);
        format = in.readNamedWriteable(DocValueFormat.class);
        buckets = in.readList(stream -> new Bucket(stream, format));
        this.targetBuckets = in.readVInt();
        bucketInnerInterval = 1; // Calculated on merge.
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        bucketInfo.writeTo(out);
        out.writeNamedWriteable(format);
        out.writeList(buckets);
        out.writeVInt(targetBuckets);
    }

    public DateHistogramInterval getInterval() {

        RoundingInfo roundingInfo = this.bucketInfo.roundingInfos[this.bucketInfo.roundingIdx];
        String unitAbbreviation = roundingInfo.unitAbbreviation;
        return new DateHistogramInterval(Long.toString(bucketInnerInterval) + unitAbbreviation);
    }

    @Override
    public String getWriteableName() {
        return AutoDateHistogramAggregationBuilder.NAME;
    }

    @Override
    public List<InternalAutoDateHistogram.Bucket> getBuckets() {
        return Collections.unmodifiableList(buckets);
    }

    DocValueFormat getFormatter() {
        return format;
    }

    public int getTargetBuckets() {
        return targetBuckets;
    }

    public BucketInfo getBucketInfo() {
        return bucketInfo;
    }

    @Override
    public InternalAutoDateHistogram create(List<Bucket> buckets) {
        return new InternalAutoDateHistogram(name, buckets, targetBuckets, bucketInfo, format, metadata, bucketInnerInterval);
    }

    @Override
    public Bucket createBucket(InternalAggregations aggregations, Bucket prototype) {
        return new Bucket(prototype.key, prototype.docCount, prototype.format, aggregations);
    }

    /**
     * This method works almost exactly the same as
     * InternalDateHistogram#reduceBuckets(List, ReduceContext), the different
     * here is that we need to round all the keys we see using the highest level
     * rounding returned across all the shards so the resolution of the buckets
     * is the same and they can be reduced together.
     */
    private BucketReduceResult reduceBuckets(List<InternalAggregation> aggregations, ReduceContext reduceContext) {

        // First we need to find the highest level rounding used across all the
        // shards
        int reduceRoundingIdx = 0;
        long min = Long.MAX_VALUE;
        long max = Long.MIN_VALUE;
        for (InternalAggregation aggregation : aggregations) {
            InternalAutoDateHistogram agg = ((InternalAutoDateHistogram) aggregation);
            reduceRoundingIdx = Math.max(agg.bucketInfo.roundingIdx, reduceRoundingIdx);
            if (false == agg.buckets.isEmpty()) {
                min = Math.min(min, agg.buckets.get(0).key);
                max = Math.max(max, agg.buckets.get(agg.buckets.size() - 1).key);
            }
        }
        Rounding.Prepared reduceRounding = prepare(reduceRoundingIdx, min, max);

        final PriorityQueue<IteratorAndCurrent<Bucket>> pq = new PriorityQueue<>(aggregations.size()) {
            @Override
            protected boolean lessThan(IteratorAndCurrent<Bucket> a, IteratorAndCurrent<Bucket> b) {
                return a.current().key < b.current().key;
            }
        };
        for (InternalAggregation aggregation : aggregations) {
            InternalAutoDateHistogram histogram = (InternalAutoDateHistogram) aggregation;
            if (histogram.buckets.isEmpty() == false) {
                pq.add(new IteratorAndCurrent<>(histogram.buckets.iterator()));
            }
        }

        List<Bucket> reducedBuckets = new ArrayList<>();
        if (pq.size() > 0) {
            // list of buckets coming from different shards that have the same key
            List<Bucket> currentBuckets = new ArrayList<>();
            long key = reduceRounding.round(pq.top().current().key);

            do {
                final IteratorAndCurrent<Bucket> top = pq.top();

                if (reduceRounding.round(top.current().key) != key) {
                    // the key changes, reduce what we already buffered and reset the buffer for current buckets
                    final Bucket reduced = reduceBucket(currentBuckets, reduceContext);
                    reducedBuckets.add(reduced);
                    currentBuckets.clear();
                    key = reduceRounding.round(top.current().key);
                }

                currentBuckets.add(top.current());

                if (top.hasNext()) {
                    top.next();
                    assert top.current().key > key: "shards must return data sorted by key";
                    pq.updateTop();
                } else {
                    pq.pop();
                }
            } while (pq.size() > 0);

            if (currentBuckets.isEmpty() == false) {
                final Bucket reduced = reduceBucket(currentBuckets, reduceContext);
                reducedBuckets.add(reduced);
            }
        }

        return mergeBucketsIfNeeded(
            new BucketReduceResult(reducedBuckets, reduceRoundingIdx, 1, reduceRounding, min, max),
            reduceContext
        );
    }

    private BucketReduceResult mergeBucketsIfNeeded(BucketReduceResult firstPassResult, ReduceContext reduceContext) {
        int idx = firstPassResult.roundingIdx;
        RoundingInfo info = bucketInfo.roundingInfos[idx];
        List<Bucket> buckets = firstPassResult.buckets;
        Rounding.Prepared prepared = firstPassResult.preparedRounding;
        while (buckets.size() > (targetBuckets * info.getMaximumInnerInterval())
                && idx < bucketInfo.roundingInfos.length - 1) {
            idx++;
            info = bucketInfo.roundingInfos[idx];
            prepared = prepare(idx, firstPassResult.min, firstPassResult.max);
            buckets = mergeBuckets(buckets, prepared, reduceContext);
        }
        return new BucketReduceResult(buckets, idx, 1, prepared, firstPassResult.min, firstPassResult.max);
    }

    private Rounding.Prepared prepare(int idx, long min, long max) {
        Rounding rounding = bucketInfo.roundingInfos[idx].rounding;
        return min <= max ? rounding.prepare(min, max) : rounding.prepareForUnknown();
    }

    private List<Bucket> mergeBuckets(List<Bucket> reducedBuckets, Rounding.Prepared reduceRounding, ReduceContext reduceContext) {
        List<Bucket> mergedBuckets = new ArrayList<>();

        List<Bucket> sameKeyedBuckets = new ArrayList<>();
        double key = Double.NaN;
        for (Bucket bucket : reducedBuckets) {
            long roundedBucketKey = reduceRounding.round(bucket.key);
            if (Double.isNaN(key)) {
                key = roundedBucketKey;
                sameKeyedBuckets.add(createBucket(key, bucket.docCount, bucket.aggregations));
            } else if (roundedBucketKey == key) {
                sameKeyedBuckets.add(createBucket(key, bucket.docCount, bucket.aggregations));
            } else {
                mergedBuckets.add(reduceBucket(sameKeyedBuckets, reduceContext));
                sameKeyedBuckets.clear();
                key = roundedBucketKey;
                sameKeyedBuckets.add(createBucket(key, bucket.docCount, bucket.aggregations));
            }
        }
        if (sameKeyedBuckets.isEmpty() == false) {
            mergedBuckets.add(reduceBucket(sameKeyedBuckets, reduceContext));
        }
        reducedBuckets = mergedBuckets;
        return reducedBuckets;
    }

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
        return new InternalAutoDateHistogram.Bucket(buckets.get(0).key, docCount, format, aggs);
    }

    private static class BucketReduceResult {
        final List<Bucket> buckets;
        final int roundingIdx;
        final long innerInterval;
        final Rounding.Prepared preparedRounding;
        final long min;
        final long max;

        BucketReduceResult(
            List<Bucket> buckets,
            int roundingIdx,
            long innerInterval,
            Rounding.Prepared preparedRounding,
            long min,
            long max
        ) {
            this.buckets = buckets;
            this.roundingIdx = roundingIdx;
            this.innerInterval = innerInterval;
            this.preparedRounding = preparedRounding;
            this.min = min;
            this.max = max;
        }
    }

    private BucketReduceResult addEmptyBuckets(BucketReduceResult current, ReduceContext reduceContext) {
        List<Bucket> list = current.buckets;
        if (list.isEmpty()) {
            return current;
        }
        int roundingIdx = getAppropriateRounding(
            list.get(0).key,
            list.get(list.size() - 1).key,
            current.roundingIdx,
            bucketInfo.roundingInfos,
            targetBuckets
        );
        Rounding.Prepared rounding = current.roundingIdx == roundingIdx
            ? current.preparedRounding
            : prepare(roundingIdx, current.min, current.max);
        // merge buckets using the new rounding
        list = mergeBuckets(list, rounding, reduceContext);

        Bucket lastBucket = null;
        ListIterator<Bucket> iter = list.listIterator();
        InternalAggregations reducedEmptySubAggs = InternalAggregations.reduce(List.of(bucketInfo.emptySubAggregations), reduceContext);

        // Add the empty buckets within the data,
        // e.g. if the data series is [1,2,3,7] there're 3 empty buckets that will be created for 4,5,6
        while (iter.hasNext()) {
            Bucket nextBucket = list.get(iter.nextIndex());
            if (lastBucket != null) {
                long key = rounding.nextRoundingValue(lastBucket.key);
                while (key < nextBucket.key) {
                    iter.add(new InternalAutoDateHistogram.Bucket(key, 0, format, reducedEmptySubAggs));
                    key = rounding.nextRoundingValue(key);
                }
                assert key == nextBucket.key : "key: " + key + ", nextBucket.key: " + nextBucket.key;
            }
            lastBucket = iter.next();
        }
        return new BucketReduceResult(list, roundingIdx, 1, rounding, current.min, current.max);
    }

    static int getAppropriateRounding(long minKey, long maxKey, int roundingIdx,
                                              RoundingInfo[] roundings, int targetBuckets) {
        if (roundingIdx == roundings.length - 1) {
            return roundingIdx;
        }
        int currentRoundingIdx = roundingIdx;

        // Getting the accurate number of required buckets can be slow for large
        // ranges at low roundings so get a rough estimate of the rounding first
        // so we are at most 1 away from the correct rounding and then get the
        // accurate rounding value
        for (int i = currentRoundingIdx + 1; i < roundings.length; i++) {
            long dataDuration = maxKey - minKey;
            long roughEstimateRequiredBuckets = dataDuration / roundings[i].getRoughEstimateDurationMillis();
            if (roughEstimateRequiredBuckets < targetBuckets * roundings[i].getMaximumInnerInterval()) {
                currentRoundingIdx = i - 1;
                break;
            } else if (i == roundingIdx - 1) {
                currentRoundingIdx = i;
                break;
            }
        }

        int requiredBuckets = 0;
        do {
            Rounding currentRounding = roundings[currentRoundingIdx].rounding;
            long currentKey = minKey;
            requiredBuckets = 0;
            while (currentKey < maxKey) {
                requiredBuckets++;
                currentKey = currentRounding.nextRoundingValue(currentKey);
            }
            currentRoundingIdx++;
        } while (requiredBuckets > (targetBuckets * roundings[currentRoundingIdx - 1].getMaximumInnerInterval())
                && currentRoundingIdx < roundings.length);
        // The loop will increase past the correct rounding index here so we
        // need to subtract one to get the rounding index we need
        return currentRoundingIdx - 1;
    }

    @Override
    public InternalAggregation reduce(List<InternalAggregation> aggregations, ReduceContext reduceContext) {
        BucketReduceResult reducedBucketsResult = reduceBuckets(aggregations, reduceContext);

        if (reduceContext.isFinalReduce()) {
            // adding empty buckets if needed
            reducedBucketsResult = addEmptyBuckets(reducedBucketsResult, reduceContext);

            // Adding empty buckets may have tipped us over the target so merge the buckets again if needed
            reducedBucketsResult = mergeBucketsIfNeeded(reducedBucketsResult, reduceContext);

            // Now finally see if we need to merge consecutive buckets together to make a coarser interval at the same rounding
            reducedBucketsResult = maybeMergeConsecutiveBuckets(reducedBucketsResult, reduceContext);
        }
        reduceContext.consumeBucketsAndMaybeBreak(reducedBucketsResult.buckets.size());
        BucketInfo bucketInfo = new BucketInfo(this.bucketInfo.roundingInfos, reducedBucketsResult.roundingIdx,
                this.bucketInfo.emptySubAggregations);

        return new InternalAutoDateHistogram(getName(), reducedBucketsResult.buckets, targetBuckets, bucketInfo, format,
                getMetadata(), reducedBucketsResult.innerInterval);
    }

    private BucketReduceResult maybeMergeConsecutiveBuckets(BucketReduceResult current, ReduceContext reduceContext) {
        List<Bucket> buckets = current.buckets;
        RoundingInfo roundingInfo = bucketInfo.roundingInfos[current.roundingIdx];
        if (buckets.size() > targetBuckets) {
            for (int interval : roundingInfo.innerIntervals) {
                int resultingBuckets = buckets.size() / interval;
                if (buckets.size() % interval != 0) {
                    resultingBuckets++;
                }
                if (resultingBuckets <= targetBuckets) {
                    return mergeConsecutiveBuckets(current, interval, reduceContext);
                }
            }
        }
        return current;
    }

    private BucketReduceResult mergeConsecutiveBuckets(BucketReduceResult current, int mergeInterval, ReduceContext reduceContext) {
        List<Bucket> mergedBuckets = new ArrayList<>();
        List<Bucket> sameKeyedBuckets = new ArrayList<>();

        double key = current.preparedRounding.round(current.buckets.get(0).key);
        for (int i = 0; i < current.buckets.size(); i++) {
            Bucket bucket = current.buckets.get(i);
            if (i % mergeInterval == 0 && sameKeyedBuckets.isEmpty() == false) {
                mergedBuckets.add(reduceBucket(sameKeyedBuckets, reduceContext));
                sameKeyedBuckets.clear();
                key = current.preparedRounding.round(bucket.key);
            }
            sameKeyedBuckets.add(new Bucket(Math.round(key), bucket.docCount, format, bucket.aggregations));
        }
        if (sameKeyedBuckets.isEmpty() == false) {
            mergedBuckets.add(reduceBucket(sameKeyedBuckets, reduceContext));
        }
        return new BucketReduceResult(
            mergedBuckets,
            current.roundingIdx,
            mergeInterval,
            current.preparedRounding,
            current.min,
            current.max
        );
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        builder.startArray(CommonFields.BUCKETS.getPreferredName());
        for (Bucket bucket : buckets) {
            bucket.toXContent(builder, params);
        }
        builder.endArray();
        builder.field("interval", getInterval().toString());
        return builder;
    }

    // HistogramFactory method impls

    @Override
    public Number getKey(MultiBucketsAggregation.Bucket bucket) {
        return ((Bucket) bucket).key;
    }

    @Override
    public Number nextKey(Number key) {
        return bucketInfo.roundingInfos[bucketInfo.roundingIdx].rounding.nextRoundingValue(key.longValue());
    }

    @Override
    public InternalAggregation createAggregation(List<MultiBucketsAggregation.Bucket> buckets) {
        // convert buckets to the right type
        List<Bucket> buckets2 = new ArrayList<>(buckets.size());
        for (Object b : buckets) {
            buckets2.add((Bucket) b);
        }
        buckets2 = Collections.unmodifiableList(buckets2);
        return new InternalAutoDateHistogram(name, buckets2, targetBuckets, bucketInfo, format, getMetadata(), bucketInnerInterval);
    }

    @Override
    public Bucket createBucket(Number key, long docCount, InternalAggregations aggregations) {
        return new Bucket(key.longValue(), docCount, format, aggregations);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        if (super.equals(obj) == false) return false;

        InternalAutoDateHistogram that = (InternalAutoDateHistogram) obj;
        return Objects.equals(buckets, that.buckets)
                && Objects.equals(format, that.format)
                && Objects.equals(bucketInfo, that.bucketInfo);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), buckets, format, bucketInfo);
    }
}
