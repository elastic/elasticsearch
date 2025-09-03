/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.categorization;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.BytesRefHash;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.aggregations.AggregationReduceContext;
import org.elasticsearch.search.aggregations.AggregatorReducer;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.InternalMultiBucketAggregation;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;
import org.elasticsearch.search.aggregations.support.SamplingContext;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.ml.job.results.CategoryDefinition;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class InternalCategorizationAggregation extends InternalMultiBucketAggregation<
    InternalCategorizationAggregation,
    InternalCategorizationAggregation.Bucket> {

    static class BucketKey implements ToXContentFragment, Comparable<BucketKey> {

        private final BytesRef[] key;

        BucketKey(SerializableTokenListCategory serializableCategory) {
            this.key = serializableCategory.getKeyTokens();
        }

        BucketKey(BytesRef[] key) {
            this.key = key;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return builder.value(toString());
        }

        @Override
        public String toString() {
            return Arrays.stream(key).map(BytesRef::utf8ToString).collect(Collectors.joining(" "));
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(key);
        }

        @Override
        public boolean equals(Object other) {
            if (other == this) {
                return true;
            }
            if (other == null || getClass() != other.getClass()) {
                return false;
            }
            BucketKey that = (BucketKey) other;
            return Arrays.equals(this.key, that.key);
        }

        public BytesRef[] keyAsTokens() {
            return key;
        }

        @Override
        public int compareTo(BucketKey o) {
            return Arrays.compare(key, o.key);
        }
    }

    public static class Bucket extends InternalMultiBucketAggregation.InternalBucketWritable
        implements
            MultiBucketsAggregation.Bucket,
            Comparable<Bucket> {

        private final SerializableTokenListCategory serializableCategory;
        private final BucketKey key;
        private long bucketOrd;
        private InternalAggregations aggregations;

        public Bucket(SerializableTokenListCategory serializableCategory, long bucketOrd) {
            this(serializableCategory, bucketOrd, InternalAggregations.EMPTY);
        }

        public Bucket(SerializableTokenListCategory serializableCategory, long bucketOrd, InternalAggregations aggregations) {
            this.serializableCategory = serializableCategory;
            this.key = new BucketKey(serializableCategory);
            this.bucketOrd = bucketOrd;
            this.aggregations = Objects.requireNonNull(aggregations);
        }

        public Bucket(StreamInput in) throws IOException {
            // Disallow this aggregation in mixed version clusters that cross the algorithm change boundary.
            if (in.getTransportVersion().before(CategorizeTextAggregationBuilder.ALGORITHM_CHANGED_VERSION)) {
                throw new ElasticsearchStatusException(
                    "["
                        + CategorizeTextAggregationBuilder.NAME
                        + "] aggregation cannot be used in a cluster where some nodes have version ["
                        + CategorizeTextAggregationBuilder.ALGORITHM_CHANGED_VERSION.toReleaseVersion()
                        + "] or higher and others have a version before this",
                    RestStatus.BAD_REQUEST
                );
            }
            serializableCategory = new SerializableTokenListCategory(in);
            key = new BucketKey(serializableCategory);
            bucketOrd = -1;
            aggregations = InternalAggregations.readFrom(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            // Disallow this aggregation in mixed version clusters that cross the algorithm change boundary.
            if (out.getTransportVersion().before(CategorizeTextAggregationBuilder.ALGORITHM_CHANGED_VERSION)) {
                throw new ElasticsearchStatusException(
                    "["
                        + CategorizeTextAggregationBuilder.NAME
                        + "] aggregation cannot be used in a cluster where some nodes have version ["
                        + CategorizeTextAggregationBuilder.ALGORITHM_CHANGED_VERSION.toReleaseVersion()
                        + "] or higher and others have a version before this",
                    RestStatus.BAD_REQUEST
                );
            }
            serializableCategory.writeTo(out);
            aggregations.writeTo(out);
        }

        private void bucketToXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(CommonFields.DOC_COUNT.getPreferredName(), serializableCategory.getNumMatches());
            builder.field(CommonFields.KEY.getPreferredName());
            key.toXContent(builder, params);
            builder.field(CategoryDefinition.REGEX.getPreferredName(), serializableCategory.getRegex());
            builder.field(CategoryDefinition.MAX_MATCHING_LENGTH.getPreferredName(), serializableCategory.maxMatchingStringLen());
            aggregations.toXContentInternal(builder, params);
            builder.endObject();
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
            return key.toString();
        }

        @Override
        public long getDocCount() {
            return serializableCategory.getNumMatches();
        }

        @Override
        public InternalAggregations getAggregations() {
            return aggregations;
        }

        void setAggregations(InternalAggregations aggregations) {
            this.aggregations = aggregations;
        }

        long getBucketOrd() {
            return bucketOrd;
        }

        SerializableTokenListCategory getSerializableCategory() {
            return serializableCategory;
        }

        @Override
        public String toString() {
            return "Bucket{key="
                + getKeyAsString()
                + ", docCount="
                + serializableCategory.getNumMatches()
                + ", aggregations="
                + aggregations.asList()
                + "}\n";
        }

        @Override
        public int compareTo(Bucket other) {
            return Long.signum(this.serializableCategory.getNumMatches() - other.serializableCategory.getNumMatches());
        }
    }

    private final List<Bucket> buckets;
    private final int similarityThreshold;
    private final int requiredSize;
    private final long minDocCount;

    protected InternalCategorizationAggregation(
        String name,
        int requiredSize,
        long minDocCount,
        int similarityThreshold,
        Map<String, Object> metadata
    ) {
        this(name, requiredSize, minDocCount, similarityThreshold, metadata, new ArrayList<>());
    }

    protected InternalCategorizationAggregation(
        String name,
        int requiredSize,
        long minDocCount,
        int similarityThreshold,
        Map<String, Object> metadata,
        List<Bucket> buckets
    ) {
        super(name, metadata);
        this.buckets = buckets;
        this.similarityThreshold = similarityThreshold;
        this.minDocCount = minDocCount;
        this.requiredSize = requiredSize;
    }

    public InternalCategorizationAggregation(StreamInput in) throws IOException {
        super(in);
        // Disallow this aggregation in mixed version clusters that cross the algorithm change boundary.
        if (in.getTransportVersion().before(CategorizeTextAggregationBuilder.ALGORITHM_CHANGED_VERSION)) {
            throw new ElasticsearchStatusException(
                "["
                    + CategorizeTextAggregationBuilder.NAME
                    + "] aggregation cannot be used in a cluster where some nodes have version ["
                    + CategorizeTextAggregationBuilder.ALGORITHM_CHANGED_VERSION.toReleaseVersion()
                    + "] or higher and others have a version before this",
                RestStatus.BAD_REQUEST
            );
        }
        this.similarityThreshold = in.readVInt();
        this.buckets = in.readCollectionAsList(Bucket::new);
        this.requiredSize = readSize(in);
        this.minDocCount = in.readVLong();
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        // Disallow this aggregation in mixed version clusters that cross the algorithm change boundary.
        if (out.getTransportVersion().before(CategorizeTextAggregationBuilder.ALGORITHM_CHANGED_VERSION)) {
            throw new ElasticsearchStatusException(
                "["
                    + CategorizeTextAggregationBuilder.NAME
                    + "] aggregation cannot be used in a cluster where some nodes have version ["
                    + CategorizeTextAggregationBuilder.ALGORITHM_CHANGED_VERSION.toReleaseVersion()
                    + "] or higher and others have a version before this",
                RestStatus.BAD_REQUEST
            );
        }
        out.writeVInt(similarityThreshold);
        out.writeCollection(buckets);
        writeSize(requiredSize, out);
        out.writeVLong(minDocCount);
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        builder.startArray(CommonFields.BUCKETS.getPreferredName());
        for (Bucket bucket : buckets) {
            bucket.bucketToXContent(builder, params);
        }
        builder.endArray();
        return builder;
    }

    @Override
    public InternalCategorizationAggregation create(List<Bucket> buckets) {
        return new InternalCategorizationAggregation(name, requiredSize, minDocCount, similarityThreshold, super.metadata, buckets);
    }

    @Override
    public Bucket createBucket(InternalAggregations aggregations, Bucket prototype) {
        return new Bucket(prototype.serializableCategory, prototype.bucketOrd, aggregations);
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
    protected AggregatorReducer getLeaderReducer(AggregationReduceContext reduceContext, int size) {
        return new AggregatorReducer() {

            final List<Bucket> buckets = new ArrayList<>();

            @Override
            public void accept(InternalAggregation aggregation) {
                buckets.addAll(((InternalCategorizationAggregation) aggregation).buckets);
            }

            @Override
            public InternalAggregation get() {
                try (CategorizationBytesRefHash hash = new CategorizationBytesRefHash(new BytesRefHash(1L, reduceContext.bigArrays()))) {
                    TokenListCategorizer categorizer = new TokenListCategorizer(
                        hash,
                        null, // part-of-speech dictionary is not needed for the reduce phase as weights are already decided
                        (float) similarityThreshold / 100.0f
                    );
                    // Merge all the categories into the newly created empty categorizer to combine them
                    for (Bucket bucket : buckets) {
                        categorizer.mergeWireCategory(bucket.serializableCategory).addSubAggs(bucket.getAggregations());
                        if (reduceContext.isCanceled().get()) {
                            break;
                        }
                    }
                    final int size = reduceContext.isFinalReduce()
                        ? Math.min(requiredSize, categorizer.getCategoryCount())
                        : categorizer.getCategoryCount();
                    Bucket[] mergedBuckets = categorizer.toOrderedBuckets(
                        size,
                        reduceContext.isFinalReduce() ? minDocCount : 0,
                        reduceContext
                    );
                    // TODO: not sure if this next line is correct - if we discarded some categories due to size or minDocCount is this
                    // handled?
                    reduceContext.consumeBucketsAndMaybeBreak(mergedBuckets.length);
                    // Keep the top categories top, but then sort by the key for those with duplicate counts
                    if (reduceContext.isFinalReduce()) {
                        Arrays.sort(mergedBuckets, Comparator.comparing(Bucket::getDocCount).reversed().thenComparing(Bucket::getRawKey));
                    }
                    return new InternalCategorizationAggregation(
                        name,
                        requiredSize,
                        minDocCount,
                        similarityThreshold,
                        metadata,
                        Arrays.asList(mergedBuckets)
                    );
                }
            }
        };
    }

    @Override
    public InternalAggregation finalizeSampling(SamplingContext samplingContext) {
        final List<Bucket> buckets = new ArrayList<>(this.buckets.size());
        for (Bucket bucket : this.buckets) {
            buckets.add(
                new Bucket(
                    new SerializableTokenListCategory(bucket.getSerializableCategory(), samplingContext.scaleUp(bucket.getDocCount())),
                    bucket.getBucketOrd(),
                    InternalAggregations.finalizeSampling(bucket.aggregations, samplingContext)
                )
            );
        }
        return new InternalCategorizationAggregation(name, requiredSize, minDocCount, similarityThreshold, metadata, buckets);
    }

    public int getSimilarityThreshold() {
        return similarityThreshold;
    }

    public int getRequiredSize() {
        return requiredSize;
    }

    public long getMinDocCount() {
        return minDocCount;
    }
}
