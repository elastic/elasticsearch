/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.aggregations.bucket.terms;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.ObjectObjectPagedHashMap;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.AggregationReduceContext;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorReducer;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.InternalMultiBucketAggregation;
import org.elasticsearch.search.aggregations.bucket.BucketReducer;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;
import org.elasticsearch.search.aggregations.bucket.terms.heuristic.SignificanceHeuristic;
import org.elasticsearch.search.aggregations.support.SamplingContext;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Result of the significant terms aggregation.
 */
public abstract class InternalSignificantTerms<A extends InternalSignificantTerms<A, B>, B extends InternalSignificantTerms.Bucket<B>>
    extends InternalMultiBucketAggregation<A, B>
    implements
        SignificantTerms {

    public static final String SCORE = "score";
    public static final String BG_COUNT = "bg_count";

    @SuppressWarnings("PMD.ConstructorCallsOverridableMethod")
    public abstract static class Bucket<B extends Bucket<B>> extends InternalMultiBucketAggregation.InternalBucket
        implements
            SignificantTerms.Bucket {
        /**
         * Reads a bucket. Should be a constructor reference.
         */
        @FunctionalInterface
        public interface Reader<B extends Bucket<B>> {
            B read(StreamInput in, long subsetSize, long supersetSize, DocValueFormat format) throws IOException;
        }

        long subsetDf;
        long subsetSize;
        long supersetDf;
        long supersetSize;
        /**
         * Ordinal of the bucket while it is being built. Not used after it is
         * returned from {@link Aggregator#buildAggregations(long[])} and not
         * serialized.
         */
        transient long bucketOrd;
        double score;
        protected InternalAggregations aggregations;
        final transient DocValueFormat format;

        protected Bucket(
            long subsetDf,
            long subsetSize,
            long supersetDf,
            long supersetSize,
            InternalAggregations aggregations,
            DocValueFormat format
        ) {
            this.subsetSize = subsetSize;
            this.supersetSize = supersetSize;
            this.subsetDf = subsetDf;
            this.supersetDf = supersetDf;
            this.aggregations = aggregations;
            this.format = format;
        }

        /**
         * Read from a stream.
         */
        protected Bucket(StreamInput in, long subsetSize, long supersetSize, DocValueFormat format) {
            this.subsetSize = subsetSize;
            this.supersetSize = supersetSize;
            this.format = format;
        }

        @Override
        public long getSubsetDf() {
            return subsetDf;
        }

        @Override
        public long getSupersetDf() {
            return supersetDf;
        }

        @Override
        public long getSupersetSize() {
            return supersetSize;
        }

        @Override
        public long getSubsetSize() {
            return subsetSize;
        }

        // TODO we should refactor to remove this, since buckets should be immutable after they are generated.
        // This can lead to confusing bugs if the bucket is re-created (via createBucket() or similar) without
        // the score
        void updateScore(SignificanceHeuristic significanceHeuristic) {
            score = significanceHeuristic.getScore(subsetDf, subsetSize, supersetDf, supersetSize);
        }

        @Override
        public long getDocCount() {
            return subsetDf;
        }

        @Override
        public InternalAggregations getAggregations() {
            return aggregations;
        }

        @Override
        public double getSignificanceScore() {
            return score;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            Bucket<?> that = (Bucket<?>) o;
            return Double.compare(that.score, score) == 0
                && Objects.equals(aggregations, that.aggregations)
                && Objects.equals(format, that.format);
        }

        @Override
        public int hashCode() {
            return Objects.hash(getClass(), aggregations, score, format);
        }

        @Override
        public final XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            keyToXContent(builder);
            builder.field(CommonFields.DOC_COUNT.getPreferredName(), getDocCount());
            builder.field(SCORE, score);
            builder.field(BG_COUNT, supersetDf);
            aggregations.toXContentInternal(builder, params);
            builder.endObject();
            return builder;
        }

        protected abstract XContentBuilder keyToXContent(XContentBuilder builder) throws IOException;
    }

    protected final int requiredSize;
    protected final long minDocCount;

    protected InternalSignificantTerms(String name, int requiredSize, long minDocCount, Map<String, Object> metadata) {
        super(name, metadata);
        this.requiredSize = requiredSize;
        this.minDocCount = minDocCount;
    }

    /**
     * Read from a stream.
     */
    protected InternalSignificantTerms(StreamInput in) throws IOException {
        super(in);
        requiredSize = readSize(in);
        minDocCount = in.readVLong();
    }

    protected final void doWriteTo(StreamOutput out) throws IOException {
        writeSize(requiredSize, out);
        out.writeVLong(minDocCount);
        writeTermTypeInfoTo(out);
    }

    protected abstract void writeTermTypeInfoTo(StreamOutput out) throws IOException;

    @Override
    public abstract List<B> getBuckets();

    @Override
    protected AggregatorReducer getLeaderReducer(AggregationReduceContext reduceContext, int size) {
        return new AggregatorReducer() {
            long globalSubsetSize = 0;
            long globalSupersetSize = 0;
            final ObjectObjectPagedHashMap<String, ReducerAndExtraInfo<B>> buckets = new ObjectObjectPagedHashMap<>(
                getBuckets().size(),
                reduceContext.bigArrays()
            );

            @Override
            public void accept(InternalAggregation aggregation) {
                @SuppressWarnings("unchecked")
                final InternalSignificantTerms<A, B> terms = (InternalSignificantTerms<A, B>) aggregation;
                // Compute the overall result set size and the corpus size using the
                // top-level Aggregations from each shard
                globalSubsetSize += terms.getSubsetSize();
                globalSupersetSize += terms.getSupersetSize();
                for (B bucket : terms.getBuckets()) {
                    ReducerAndExtraInfo<B> reducerAndExtraInfo = buckets.get(bucket.getKeyAsString());
                    if (reducerAndExtraInfo == null) {
                        reducerAndExtraInfo = new ReducerAndExtraInfo<>(new BucketReducer<>(bucket, reduceContext, size));
                        boolean success = false;
                        try {
                            buckets.put(bucket.getKeyAsString(), reducerAndExtraInfo);
                            success = true;
                        } finally {
                            if (success == false) {
                                Releasables.close(reducerAndExtraInfo.reducer);
                            }
                        }
                    }
                    reducerAndExtraInfo.reducer.accept(bucket);
                    reducerAndExtraInfo.subsetDf[0] += bucket.subsetDf;
                    reducerAndExtraInfo.supersetDf[0] += bucket.supersetDf;
                }
            }

            @Override
            public InternalAggregation get() {
                final SignificanceHeuristic heuristic = getSignificanceHeuristic().rewrite(reduceContext);
                final int size = (int) (reduceContext.isFinalReduce() == false ? buckets.size() : Math.min(requiredSize, buckets.size()));
                try (BucketSignificancePriorityQueue<B> ordered = new BucketSignificancePriorityQueue<>(size, reduceContext.bigArrays())) {
                    buckets.forEach(entry -> {
                        final B b = createBucket(
                            entry.value.subsetDf[0],
                            globalSubsetSize,
                            entry.value.supersetDf[0],
                            globalSupersetSize,
                            entry.value.reducer.getAggregations(),
                            entry.value.reducer.getProto()
                        );
                        b.updateScore(heuristic);
                        if (((b.score > 0) && (b.subsetDf >= minDocCount)) || reduceContext.isFinalReduce() == false) {
                            final B removed = ordered.insertWithOverflow(b);
                            if (removed == null) {
                                reduceContext.consumeBucketsAndMaybeBreak(1);
                            } else {
                                reduceContext.consumeBucketsAndMaybeBreak(-countInnerBucket(removed));
                            }
                        } else {
                            reduceContext.consumeBucketsAndMaybeBreak(-countInnerBucket(b));
                        }
                    });
                    final B[] list = createBucketsArray((int) ordered.size());
                    for (int i = (int) ordered.size() - 1; i >= 0; i--) {
                        list[i] = ordered.pop();
                    }
                    return create(globalSubsetSize, globalSupersetSize, Arrays.asList(list));
                }
            }

            @Override
            public void close() {
                buckets.forEach(entry -> Releasables.close(entry.value.reducer));
                Releasables.close(buckets);
            }
        };
    }

    private record ReducerAndExtraInfo<B extends MultiBucketsAggregation.Bucket>(
        BucketReducer<B> reducer,
        long[] subsetDf,
        long[] supersetDf
    ) {
        private ReducerAndExtraInfo(BucketReducer<B> reducer) {
            this(reducer, new long[] { 0 }, new long[] { 0 });
        }
    }

    @Override
    public InternalAggregation finalizeSampling(SamplingContext samplingContext) {
        long supersetSize = samplingContext.scaleUp(getSupersetSize());
        long subsetSize = samplingContext.scaleUp(getSubsetSize());
        return create(
            subsetSize,
            supersetSize,
            getBuckets().stream()
                .map(
                    b -> createBucket(
                        samplingContext.scaleUp(b.subsetDf),
                        subsetSize,
                        samplingContext.scaleUp(b.supersetDf),
                        supersetSize,
                        InternalAggregations.finalizeSampling(b.aggregations, samplingContext),
                        b
                    )
                )
                .toList()
        );
    }

    abstract B createBucket(
        long subsetDf,
        long subsetSize,
        long supersetDf,
        long supersetSize,
        InternalAggregations aggregations,
        B prototype
    );

    protected abstract A create(long subsetSize, long supersetSize, List<B> buckets);

    /**
     * Create an array to hold some buckets. Used in collecting the results.
     */
    protected abstract B[] createBucketsArray(int size);

    protected abstract long getSubsetSize();

    protected abstract long getSupersetSize();

    protected abstract SignificanceHeuristic getSignificanceHeuristic();

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), minDocCount, requiredSize);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        if (super.equals(obj) == false) return false;

        InternalSignificantTerms<?, ?> that = (InternalSignificantTerms<?, ?>) obj;
        return Objects.equals(minDocCount, that.minDocCount) && Objects.equals(requiredSize, that.requiredSize);
    }
}
