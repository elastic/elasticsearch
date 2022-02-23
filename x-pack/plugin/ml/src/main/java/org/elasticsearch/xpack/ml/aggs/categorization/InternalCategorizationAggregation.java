/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.categorization;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.PriorityQueue;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.util.BytesRefHash;
import org.elasticsearch.search.aggregations.AggregationExecutionException;
import org.elasticsearch.search.aggregations.AggregationReduceContext;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.InternalMultiBucketAggregation;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;
import org.elasticsearch.search.aggregations.support.SamplingContext;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.ml.aggs.categorization.CategorizationBytesRefHash.WILD_CARD_REF;

public class InternalCategorizationAggregation extends InternalMultiBucketAggregation<
    InternalCategorizationAggregation,
    InternalCategorizationAggregation.Bucket> {

    // Carries state allowing for delayed reduction of the bucket
    // This allows us to keep from accidentally calling "reduce" on the sub-aggs more than once
    private static class DelayedCategorizationBucket {
        private final BucketKey key;
        private long docCount;
        private final List<Bucket> toReduce;

        DelayedCategorizationBucket(BucketKey key, List<Bucket> toReduce, long docCount) {
            this.key = key;
            this.toReduce = new ArrayList<>(toReduce);
            this.docCount = docCount;
        }

        public long getDocCount() {
            return docCount;
        }

        public Bucket reduce(BucketKey bucketKey, AggregationReduceContext reduceContext) {
            List<InternalAggregations> innerAggs = new ArrayList<>(toReduce.size());
            long totalDocCount = 0;
            for (Bucket bucket : toReduce) {
                innerAggs.add(bucket.aggregations);
                totalDocCount += bucket.docCount;
            }
            return new Bucket(bucketKey, totalDocCount, InternalAggregations.reduce(innerAggs, reduceContext));
        }

        public DelayedCategorizationBucket add(Bucket bucket) {
            this.docCount += bucket.docCount;
            this.toReduce.add(bucket);
            return this;
        }

        public DelayedCategorizationBucket add(DelayedCategorizationBucket bucket) {
            this.docCount += bucket.docCount;
            this.toReduce.addAll(bucket.toReduce);
            return this;
        }
    }

    static class BucketCountPriorityQueue extends PriorityQueue<Bucket> {
        BucketCountPriorityQueue(int size) {
            super(size);
        }

        @Override
        protected boolean lessThan(Bucket a, Bucket b) {
            return a.docCount < b.docCount;
        }
    }

    static class BucketKey implements ToXContentFragment, Writeable, Comparable<BucketKey> {

        private final BytesRef[] key;

        static BucketKey withCollapsedWildcards(BytesRef[] key) {
            if (key.length <= 1) {
                return new BucketKey(key);
            }
            List<BytesRef> collapsedWildCards = new ArrayList<>();
            boolean previousTokenWildCard = false;
            for (BytesRef token : key) {
                if (token.equals(WILD_CARD_REF)) {
                    if (previousTokenWildCard == false) {
                        previousTokenWildCard = true;
                        collapsedWildCards.add(WILD_CARD_REF);
                    }
                } else {
                    previousTokenWildCard = false;
                    collapsedWildCards.add(token);
                }
            }
            if (collapsedWildCards.size() == key.length) {
                return new BucketKey(key);
            }
            return new BucketKey(collapsedWildCards.toArray(BytesRef[]::new));
        }

        BucketKey(BytesRef[] key) {
            this.key = key;
        }

        BucketKey(StreamInput in) throws IOException {
            key = in.readArray(StreamInput::readBytesRef, BytesRef[]::new);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return builder.value(asString());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeArray(StreamOutput::writeBytesRef, key);
        }

        public String asString() {
            StringBuilder builder = new StringBuilder();
            for (int i = 0; i < key.length - 1; i++) {
                builder.append(key[i].utf8ToString()).append(" ");
            }
            builder.append(key[key.length - 1].utf8ToString());
            return builder.toString();
        }

        @Override
        public String toString() {
            return asString();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            BucketKey bucketKey = (BucketKey) o;
            return Arrays.equals(key, bucketKey.key);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(key);
        }

        public BytesRef[] keyAsTokens() {
            return key;
        }

        @Override
        public int compareTo(BucketKey o) {
            return Arrays.compare(key, o.key);
        }

    }

    public static class Bucket extends InternalMultiBucketAggregation.InternalBucket
        implements
            MultiBucketsAggregation.Bucket,
            Comparable<Bucket> {
        // Used on the shard level to keep track of sub aggregations
        long bucketOrd;

        final BucketKey key;
        final long docCount;
        InternalAggregations aggregations;

        public Bucket(BucketKey key, long docCount, InternalAggregations aggregations) {
            this.key = key;
            this.docCount = docCount;
            this.aggregations = aggregations;
        }

        public Bucket(StreamInput in) throws IOException {
            key = new BucketKey(in);
            docCount = in.readVLong();
            aggregations = InternalAggregations.readFrom(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            key.writeTo(out);
            out.writeVLong(getDocCount());
            aggregations.writeTo(out);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(CommonFields.DOC_COUNT.getPreferredName(), docCount);
            builder.field(CommonFields.KEY.getPreferredName());
            key.toXContent(builder, params);
            aggregations.toXContentInternal(builder, params);
            builder.endObject();
            return builder;
        }

        BucketKey getRawKey() {
            return key;
        }

        @Override
        public Object getKey() {
            return key;
        }

        @Override
        public String getKeyAsString() {
            return key.asString();
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
        public String toString() {
            return "Bucket{" + "key=" + getKeyAsString() + ", docCount=" + docCount + ", aggregations=" + aggregations.asMap() + "}\n";
        }

        @Override
        public int compareTo(Bucket o) {
            return key.compareTo(o.key);
        }

    }

    private final List<Bucket> buckets;
    private final int maxUniqueTokens;
    private final int similarityThreshold;
    private final int maxMatchTokens;
    private final int requiredSize;
    private final long minDocCount;

    protected InternalCategorizationAggregation(
        String name,
        int requiredSize,
        long minDocCount,
        int maxUniqueTokens,
        int maxMatchTokens,
        int similarityThreshold,
        Map<String, Object> metadata
    ) {
        this(name, requiredSize, minDocCount, maxUniqueTokens, maxMatchTokens, similarityThreshold, metadata, new ArrayList<>());
    }

    protected InternalCategorizationAggregation(
        String name,
        int requiredSize,
        long minDocCount,
        int maxUniqueTokens,
        int maxMatchTokens,
        int similarityThreshold,
        Map<String, Object> metadata,
        List<Bucket> buckets
    ) {
        super(name, metadata);
        this.buckets = buckets;
        this.maxUniqueTokens = maxUniqueTokens;
        this.maxMatchTokens = maxMatchTokens;
        this.similarityThreshold = similarityThreshold;
        this.minDocCount = minDocCount;
        this.requiredSize = requiredSize;
    }

    public InternalCategorizationAggregation(StreamInput in) throws IOException {
        super(in);
        this.maxUniqueTokens = in.readVInt();
        this.maxMatchTokens = in.readVInt();
        this.similarityThreshold = in.readVInt();
        this.buckets = in.readList(Bucket::new);
        this.requiredSize = readSize(in);
        this.minDocCount = in.readVLong();
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeVInt(maxUniqueTokens);
        out.writeVInt(maxMatchTokens);
        out.writeVInt(similarityThreshold);
        out.writeList(buckets);
        writeSize(requiredSize, out);
        out.writeVLong(minDocCount);
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

    @Override
    public InternalCategorizationAggregation create(List<Bucket> bucketList) {
        return new InternalCategorizationAggregation(
            name,
            requiredSize,
            minDocCount,
            maxUniqueTokens,
            maxMatchTokens,
            similarityThreshold,
            super.metadata,
            bucketList
        );
    }

    @Override
    public Bucket createBucket(InternalAggregations aggregations, Bucket prototype) {
        return new Bucket(prototype.key, prototype.docCount, aggregations);
    }

    @Override
    protected Bucket reduceBucket(List<Bucket> buckets, AggregationReduceContext context) {
        throw new IllegalArgumentException("For optimization purposes, typical bucket path is not supported");
    }

    @Override
    public List<Bucket> getBuckets() {
        return buckets;
    }

    @Override
    public String getWriteableName() {
        return CategorizeTextAggregationBuilder.NAME;
    }

    @Override
    public InternalAggregation reduce(List<InternalAggregation> aggregations, AggregationReduceContext reduceContext) {
        try (CategorizationBytesRefHash hash = new CategorizationBytesRefHash(new BytesRefHash(1L, reduceContext.bigArrays()))) {
            CategorizationTokenTree categorizationTokenTree = new CategorizationTokenTree(
                maxUniqueTokens,
                maxMatchTokens,
                similarityThreshold
            );
            // TODO: Could we do a merge sort similar to terms?
            // It would require us returning partial reductions sorted by key, not by doc_count
            // First, make sure we have all the counts for equal categorizations
            Map<BucketKey, DelayedCategorizationBucket> reduced = new HashMap<>();
            for (InternalAggregation aggregation : aggregations) {
                InternalCategorizationAggregation categorizationAggregation = (InternalCategorizationAggregation) aggregation;
                for (Bucket bucket : categorizationAggregation.buckets) {
                    reduced.computeIfAbsent(bucket.key, key -> new DelayedCategorizationBucket(key, new ArrayList<>(1), 0L)).add(bucket);
                }
            }

            reduced.values().stream().sorted(Comparator.comparing(DelayedCategorizationBucket::getDocCount).reversed()).forEach(bucket ->
            // Parse tokens takes document count into account and merging on smallest groups
            categorizationTokenTree.parseTokens(hash.getIds(bucket.key.keyAsTokens()), bucket.docCount));
            categorizationTokenTree.mergeSmallestChildren();
            Map<BucketKey, DelayedCategorizationBucket> mergedBuckets = new HashMap<>();
            for (DelayedCategorizationBucket delayedBucket : reduced.values()) {
                TextCategorization group = categorizationTokenTree.parseTokensConst(hash.getIds(delayedBucket.key.keyAsTokens()))
                    .orElseThrow(
                        () -> new AggregationExecutionException(
                            "Unexpected null categorization group for bucket [" + delayedBucket.key.asString() + "]"
                        )
                    );
                BytesRef[] categoryTokens = hash.getDeeps(group.getCategorization());

                BucketKey key = reduceContext.isFinalReduce()
                    ? BucketKey.withCollapsedWildcards(categoryTokens)
                    : new BucketKey(categoryTokens);
                mergedBuckets.computeIfAbsent(
                    key,
                    k -> new DelayedCategorizationBucket(k, new ArrayList<>(delayedBucket.toReduce.size()), 0L)
                ).add(delayedBucket);
            }

            final int size = reduceContext.isFinalReduce() == false ? mergedBuckets.size() : Math.min(requiredSize, mergedBuckets.size());
            final PriorityQueue<Bucket> pq = new BucketCountPriorityQueue(size);
            for (Map.Entry<BucketKey, DelayedCategorizationBucket> keyAndBuckets : mergedBuckets.entrySet()) {
                final BucketKey key = keyAndBuckets.getKey();
                DelayedCategorizationBucket bucket = keyAndBuckets.getValue();
                Bucket newBucket = bucket.reduce(key, reduceContext);
                if ((newBucket.docCount >= minDocCount) || reduceContext.isFinalReduce() == false) {
                    Bucket removed = pq.insertWithOverflow(newBucket);
                    if (removed == null) {
                        reduceContext.consumeBucketsAndMaybeBreak(1);
                    } else {
                        reduceContext.consumeBucketsAndMaybeBreak(-countInnerBucket(removed));
                    }
                } else {
                    reduceContext.consumeBucketsAndMaybeBreak(-countInnerBucket(newBucket));
                }
            }
            Bucket[] bucketList = new Bucket[pq.size()];
            for (int i = pq.size() - 1; i >= 0; i--) {
                bucketList[i] = pq.pop();
            }
            // Keep the top categories top, but then sort by the key for those with duplicate counts
            if (reduceContext.isFinalReduce()) {
                Arrays.sort(bucketList, Comparator.comparing(Bucket::getDocCount).reversed().thenComparing(Bucket::getRawKey));
            }
            return new InternalCategorizationAggregation(
                name,
                requiredSize,
                minDocCount,
                maxUniqueTokens,
                maxMatchTokens,
                similarityThreshold,
                metadata,
                Arrays.asList(bucketList)
            );
        }
    }

    @Override
    public InternalAggregation finalizeSampling(SamplingContext samplingContext) {
        return new InternalCategorizationAggregation(
            name,
            requiredSize,
            minDocCount,
            maxUniqueTokens,
            maxMatchTokens,
            similarityThreshold,
            metadata,
            buckets.stream()
                .map(
                    b -> new Bucket(
                        b.key,
                        samplingContext.scaleUp(b.docCount),
                        InternalAggregations.finalizeSampling(b.aggregations, samplingContext)
                    )
                )
                .collect(Collectors.toList())
        );
    }

    public int getMaxUniqueTokens() {
        return maxUniqueTokens;
    }

    public int getSimilarityThreshold() {
        return similarityThreshold;
    }

    public int getMaxMatchTokens() {
        return maxMatchTokens;
    }

    public int getRequiredSize() {
        return requiredSize;
    }

    public long getMinDocCount() {
        return minDocCount;
    }
}
