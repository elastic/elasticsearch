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

import org.apache.lucene.util.PriorityQueue;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.rounding.Rounding;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.InternalMultiBucketAggregation;
import org.elasticsearch.search.aggregations.KeyComparable;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
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
            aggregations = InternalAggregations.readAggregations(in);
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

        Bucket reduce(List<Bucket> buckets, Rounding rounding, ReduceContext context) {
            List<InternalAggregations> aggregations = new ArrayList<>(buckets.size());
            long docCount = 0;
            for (Bucket bucket : buckets) {
                docCount += bucket.docCount;
                aggregations.add((InternalAggregations) bucket.getAggregations());
            }
            InternalAggregations aggs = InternalAggregations.reduce(aggregations, context);
            return new InternalAutoDateHistogram.Bucket(rounding.round(key), docCount, format, aggs);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            String keyAsString = format.format(key);
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

        final Rounding[] roundings;
        final int roundingIdx;
        final InternalAggregations emptySubAggregations;

        BucketInfo(Rounding[] roundings, int roundingIdx, InternalAggregations subAggregations) {
            this.roundings = roundings;
            this.roundingIdx = roundingIdx;
            this.emptySubAggregations = subAggregations;
        }

        BucketInfo(StreamInput in) throws IOException {
            int size = in.readVInt();
            roundings = new Rounding[size];
            for (int i = 0; i < size; i++) {
                roundings[i] = Rounding.Streams.read(in);
            }
            roundingIdx = in.readVInt();
            emptySubAggregations = InternalAggregations.readAggregations(in);
        }

        void writeTo(StreamOutput out) throws IOException {
            out.writeVInt(roundings.length);
            for (Rounding rounding : roundings) {
                Rounding.Streams.write(rounding, out);
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
            return Objects.deepEquals(roundings, that.roundings)
                    && Objects.equals(roundingIdx, that.roundingIdx)
                    && Objects.equals(emptySubAggregations, that.emptySubAggregations);
        }

        @Override
        public int hashCode() {
            return Objects.hash(getClass(), Arrays.hashCode(roundings), roundingIdx, emptySubAggregations);
        }
    }

    private final List<Bucket> buckets;
    private final DocValueFormat format;
    private final BucketInfo bucketInfo;
    private final int targetBuckets;


    InternalAutoDateHistogram(String name, List<Bucket> buckets, int targetBuckets, BucketInfo emptyBucketInfo, DocValueFormat formatter,
            List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData) {
        super(name, pipelineAggregators, metaData);
        this.buckets = buckets;
        this.bucketInfo = emptyBucketInfo;
        this.format = formatter;
        this.targetBuckets = targetBuckets;
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
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        bucketInfo.writeTo(out);
        out.writeNamedWriteable(format);
        out.writeList(buckets);
        out.writeVInt(targetBuckets);
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
        return new InternalAutoDateHistogram(name, buckets, targetBuckets, bucketInfo, format, pipelineAggregators(), metaData);
    }

    @Override
    public Bucket createBucket(InternalAggregations aggregations, Bucket prototype) {
        return new Bucket(prototype.key, prototype.docCount, prototype.format, aggregations);
    }

    private static class IteratorAndCurrent {

        private final Iterator<Bucket> iterator;
        private Bucket current;

        IteratorAndCurrent(Iterator<Bucket> iterator) {
            this.iterator = iterator;
            current = iterator.next();
        }

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
        for (InternalAggregation aggregation : aggregations) {
            int aggRoundingIdx = ((InternalAutoDateHistogram) aggregation).bucketInfo.roundingIdx;
            if (aggRoundingIdx > reduceRoundingIdx) {
                reduceRoundingIdx = aggRoundingIdx;
            }
        }
        // This rounding will be used to reduce all the buckets
        Rounding reduceRounding = bucketInfo.roundings[reduceRoundingIdx];

        final PriorityQueue<IteratorAndCurrent> pq = new PriorityQueue<IteratorAndCurrent>(aggregations.size()) {
            @Override
            protected boolean lessThan(IteratorAndCurrent a, IteratorAndCurrent b) {
                return a.current.key < b.current.key;
            }
        };
        for (InternalAggregation aggregation : aggregations) {
            InternalAutoDateHistogram histogram = (InternalAutoDateHistogram) aggregation;
            if (histogram.buckets.isEmpty() == false) {
                pq.add(new IteratorAndCurrent(histogram.buckets.iterator()));
            }
        }

        List<Bucket> reducedBuckets = new ArrayList<>();
        if (pq.size() > 0) {
            // list of buckets coming from different shards that have the same key
            List<Bucket> currentBuckets = new ArrayList<>();
            double key = reduceRounding.round(pq.top().current.key);

            do {
                final IteratorAndCurrent top = pq.top();

                if (reduceRounding.round(top.current.key) != key) {
                    // the key changes, reduce what we already buffered and reset the buffer for current buckets
                    final Bucket reduced = currentBuckets.get(0).reduce(currentBuckets, reduceRounding, reduceContext);
                    reducedBuckets.add(reduced);
                    currentBuckets.clear();
                    key = reduceRounding.round(top.current.key);
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
                final Bucket reduced = currentBuckets.get(0).reduce(currentBuckets, reduceRounding, reduceContext);
                reducedBuckets.add(reduced);
            }
        }

        return mergeBucketsIfNeeded(reducedBuckets, reduceRoundingIdx, reduceRounding, reduceContext);
    }

    private BucketReduceResult mergeBucketsIfNeeded(List<Bucket> reducedBuckets, int reduceRoundingIdx, Rounding reduceRounding,
            ReduceContext reduceContext) {
        while (reducedBuckets.size() > targetBuckets && reduceRoundingIdx < bucketInfo.roundings.length - 1) {
            reduceRoundingIdx++;
            reduceRounding = bucketInfo.roundings[reduceRoundingIdx];
            reducedBuckets = mergeBuckets(reducedBuckets, reduceRounding, reduceContext);
        }
        return new BucketReduceResult(reducedBuckets, reduceRounding, reduceRoundingIdx);
    }

    private List<Bucket> mergeBuckets(List<Bucket> reducedBuckets, Rounding reduceRounding, ReduceContext reduceContext) {
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
                mergedBuckets.add(sameKeyedBuckets.get(0).reduce(sameKeyedBuckets, reduceRounding, reduceContext));
                sameKeyedBuckets.clear();
                key = roundedBucketKey;
                sameKeyedBuckets.add(createBucket(key, bucket.docCount, bucket.aggregations));
            }
        }
        if (sameKeyedBuckets.isEmpty() == false) {
            mergedBuckets.add(sameKeyedBuckets.get(0).reduce(sameKeyedBuckets, reduceRounding, reduceContext));
        }
        reducedBuckets = mergedBuckets;
        return reducedBuckets;
    }

    private static class BucketReduceResult {
        List<Bucket> buckets;
        Rounding rounding;
        int roundingIdx;

        BucketReduceResult(List<Bucket> buckets, Rounding rounding, int roundingIdx) {
            this.buckets = buckets;
            this.rounding = rounding;
            this.roundingIdx = roundingIdx;

        }
    }

    private BucketReduceResult addEmptyBuckets(BucketReduceResult currentResult, ReduceContext reduceContext) {
        List<Bucket> list = currentResult.buckets;
        if (list.isEmpty()) {
            return currentResult;
        }
        int roundingIdx = getAppropriateRounding(list.get(0).key, list.get(list.size() - 1).key, currentResult.roundingIdx,
                bucketInfo.roundings);
        Rounding rounding = bucketInfo.roundings[roundingIdx];
        // merge buckets using the new rounding
        list = mergeBuckets(list, rounding, reduceContext);

        Bucket lastBucket = null;
        ListIterator<Bucket> iter = list.listIterator();
        InternalAggregations reducedEmptySubAggs = InternalAggregations.reduce(Collections.singletonList(bucketInfo.emptySubAggregations),
                reduceContext);

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
        return new BucketReduceResult(list, rounding, roundingIdx);
    }

    private int getAppropriateRounding(long minKey, long maxKey, int roundingIdx, Rounding[] roundings) {
        if (roundingIdx == roundings.length - 1) {
            return roundingIdx;
        }
        int currentRoundingIdx = roundingIdx;
        int requiredBuckets = 0;
        do {
            Rounding currentRounding = roundings[currentRoundingIdx];
            long currentKey = minKey;
            requiredBuckets = 0;
            while (currentKey < maxKey) {
                requiredBuckets++;
                currentKey = currentRounding.nextRoundingValue(currentKey);
            }
            currentRoundingIdx++;
        } while (requiredBuckets > targetBuckets && currentRoundingIdx < roundings.length);
        // The loop will increase past the correct rounding index here so we
        // need to subtract one to get the rounding index we need
        return currentRoundingIdx - 1;
    }

    @Override
    public InternalAggregation doReduce(List<InternalAggregation> aggregations, ReduceContext reduceContext) {
        BucketReduceResult reducedBucketsResult = reduceBuckets(aggregations, reduceContext);

        // adding empty buckets if needed
        reducedBucketsResult = addEmptyBuckets(reducedBucketsResult, reduceContext);

        // Adding empty buckets may have tipped us over the target so merge the buckets again if needed
        reducedBucketsResult = mergeBucketsIfNeeded(reducedBucketsResult.buckets, reducedBucketsResult.roundingIdx,
                reducedBucketsResult.rounding, reduceContext);

        BucketInfo bucketInfo = new BucketInfo(this.bucketInfo.roundings, reducedBucketsResult.roundingIdx,
                this.bucketInfo.emptySubAggregations);

        return new InternalAutoDateHistogram(getName(), reducedBucketsResult.buckets, targetBuckets, bucketInfo, format,
                pipelineAggregators(), getMetaData());
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        builder.startArray(CommonFields.BUCKETS.getPreferredName());
        for (Bucket bucket : buckets) {
            bucket.toXContent(builder, params);
        }
        builder.endArray();
        return builder;
    }

    // HistogramFactory method impls

    @Override
    public Number getKey(MultiBucketsAggregation.Bucket bucket) {
        return ((Bucket) bucket).key;
    }

    @Override
    public Number nextKey(Number key) {
        return bucketInfo.roundings[bucketInfo.roundingIdx].nextRoundingValue(key.longValue());
    }

    @Override
    public InternalAggregation createAggregation(List<MultiBucketsAggregation.Bucket> buckets) {
        // convert buckets to the right type
        List<Bucket> buckets2 = new ArrayList<>(buckets.size());
        for (Object b : buckets) {
            buckets2.add((Bucket) b);
        }
        buckets2 = Collections.unmodifiableList(buckets2);
        return new InternalAutoDateHistogram(name, buckets2, targetBuckets, bucketInfo, format, pipelineAggregators(), getMetaData());
    }

    @Override
    public Bucket createBucket(Number key, long docCount, InternalAggregations aggregations) {
        return new Bucket(key.longValue(), docCount, format, aggregations);
    }

    @Override
    protected boolean doEquals(Object obj) {
        InternalAutoDateHistogram that = (InternalAutoDateHistogram) obj;
        return Objects.equals(buckets, that.buckets)
                && Objects.equals(format, that.format)
                && Objects.equals(bucketInfo, that.bucketInfo);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(buckets, format, bucketInfo);
    }
}
