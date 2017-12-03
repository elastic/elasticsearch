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
package org.elasticsearch.search.aggregations.bucket.significant;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.InternalMultiBucketAggregation;
import org.elasticsearch.search.aggregations.bucket.significant.heuristics.SignificanceHeuristic;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Result of the significant terms aggregation.
 */
public abstract class InternalSignificantTerms<A extends InternalSignificantTerms<A, B>, B extends InternalSignificantTerms.Bucket<B>>
        extends InternalMultiBucketAggregation<A, B> implements SignificantTerms {

    public static final String SCORE = "score";
    public static final String BG_COUNT = "bg_count";

    @SuppressWarnings("PMD.ConstructorCallsOverridableMethod")
    public abstract static class Bucket<B extends Bucket<B>> extends InternalMultiBucketAggregation.InternalBucket
            implements SignificantTerms.Bucket {
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
        long bucketOrd;
        double score;
        protected InternalAggregations aggregations;
        final transient DocValueFormat format;

        protected Bucket(long subsetDf, long subsetSize, long supersetDf, long supersetSize,
                InternalAggregations aggregations, DocValueFormat format) {
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

        void updateScore(SignificanceHeuristic significanceHeuristic) {
            score = significanceHeuristic.getScore(subsetDf, subsetSize, supersetDf, supersetSize);
        }

        @Override
        public long getDocCount() {
            return subsetDf;
        }

        @Override
        public Aggregations getAggregations() {
            return aggregations;
        }

        public B reduce(List<B> buckets, ReduceContext context) {
            long subsetDf = 0;
            long supersetDf = 0;
            List<InternalAggregations> aggregationsList = new ArrayList<>(buckets.size());
            for (B bucket : buckets) {
                subsetDf += bucket.subsetDf;
                supersetDf += bucket.supersetDf;
                aggregationsList.add(bucket.aggregations);
            }
            InternalAggregations aggs = InternalAggregations.reduce(aggregationsList, context);
            return newBucket(subsetDf, subsetSize, supersetDf, supersetSize, aggs);
        }

        abstract B newBucket(long subsetDf, long subsetSize, long supersetDf, long supersetSize, InternalAggregations aggregations);

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
            return bucketOrd == that.bucketOrd &&
                    Double.compare(that.score, score) == 0 &&
                    Objects.equals(aggregations, that.aggregations) &&
                    Objects.equals(format, that.format);
        }

        @Override
        public int hashCode() {
            return Objects.hash(getClass(), bucketOrd, aggregations, score, format);
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

    protected InternalSignificantTerms(String name, int requiredSize, long minDocCount, List<PipelineAggregator> pipelineAggregators,
            Map<String, Object> metaData) {
        super(name, pipelineAggregators, metaData);
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
    public InternalAggregation doReduce(List<InternalAggregation> aggregations, ReduceContext reduceContext) {
        long globalSubsetSize = 0;
        long globalSupersetSize = 0;
        // Compute the overall result set size and the corpus size using the
        // top-level Aggregations from each shard
        for (InternalAggregation aggregation : aggregations) {
            @SuppressWarnings("unchecked")
            InternalSignificantTerms<A, B> terms = (InternalSignificantTerms<A, B>) aggregation;
            globalSubsetSize += terms.getSubsetSize();
            globalSupersetSize += terms.getSupersetSize();
        }
        Map<String, List<B>> buckets = new HashMap<>();
        for (InternalAggregation aggregation : aggregations) {
            @SuppressWarnings("unchecked")
            InternalSignificantTerms<A, B> terms = (InternalSignificantTerms<A, B>) aggregation;
            for (B bucket : terms.getBuckets()) {
                List<B> existingBuckets = buckets.get(bucket.getKeyAsString());
                if (existingBuckets == null) {
                    existingBuckets = new ArrayList<>(aggregations.size());
                    buckets.put(bucket.getKeyAsString(), existingBuckets);
                }
                // Adjust the buckets with the global stats representing the
                // total size of the pots from which the stats are drawn
                existingBuckets.add(bucket.newBucket(bucket.getSubsetDf(), globalSubsetSize, bucket.getSupersetDf(), globalSupersetSize,
                        bucket.aggregations));
            }
        }
        SignificanceHeuristic heuristic = getSignificanceHeuristic().rewrite(reduceContext);
        final int size = reduceContext.isFinalReduce() == false ? buckets.size() : Math.min(requiredSize, buckets.size());
        BucketSignificancePriorityQueue<B> ordered = new BucketSignificancePriorityQueue<>(size);
        for (Map.Entry<String, List<B>> entry : buckets.entrySet()) {
            List<B> sameTermBuckets = entry.getValue();
            final B b = sameTermBuckets.get(0).reduce(sameTermBuckets, reduceContext);
            b.updateScore(heuristic);
            if (((b.score > 0) && (b.subsetDf >= minDocCount)) || reduceContext.isFinalReduce() == false) {
                ordered.insertWithOverflow(b);
            }
        }
        B[] list = createBucketsArray(ordered.size());
        for (int i = ordered.size() - 1; i >= 0; i--) {
            list[i] = ordered.pop();
        }
        return create(globalSubsetSize, globalSupersetSize, Arrays.asList(list));
    }

    protected abstract A create(long subsetSize, long supersetSize, List<B> buckets);

    /**
     * Create an array to hold some buckets. Used in collecting the results.
     */
    protected abstract B[] createBucketsArray(int size);

    protected abstract long getSubsetSize();

    protected abstract long getSupersetSize();

    protected abstract SignificanceHeuristic getSignificanceHeuristic();

    @Override
    protected int doHashCode() {
        return Objects.hash(minDocCount, requiredSize);
    }

    @Override
    protected boolean doEquals(Object obj) {
        InternalSignificantTerms<?, ?> that = (InternalSignificantTerms<?, ?>) obj;
        return Objects.equals(minDocCount, that.minDocCount)
                && Objects.equals(requiredSize, that.requiredSize);
    }
}
