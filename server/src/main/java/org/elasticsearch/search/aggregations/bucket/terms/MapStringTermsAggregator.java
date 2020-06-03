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
package org.elasticsearch.search.aggregations.bucket.terms;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.PriorityQueue;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.util.BytesRefHash;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalMultiBucketAggregation;
import org.elasticsearch.search.aggregations.InternalOrder;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.LeafBucketCollectorBase;
import org.elasticsearch.search.aggregations.bucket.terms.heuristic.SignificanceHeuristic;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Function;

/**
 * An aggregator of string values that hashes the strings on the fly rather
 * than up front like the {@link GlobalOrdinalsStringTermsAggregator}.
 */
public class MapStringTermsAggregator extends AbstractStringTermsAggregator {
    private final ResultStrategy<?, ?> resultStrategy;
    private final ValuesSource valuesSource;
    private final BytesRefHash bucketOrds;
    private final IncludeExclude.StringFilter includeExclude;

    public MapStringTermsAggregator(
        String name,
        AggregatorFactories factories,
        Function<MapStringTermsAggregator, ResultStrategy<?, ?>> resultStrategy,
        ValuesSource valuesSource,
        BucketOrder order,
        DocValueFormat format,
        BucketCountThresholds bucketCountThresholds,
        IncludeExclude.StringFilter includeExclude,
        SearchContext context,
        Aggregator parent,
        SubAggCollectionMode collectionMode,
        boolean showTermDocCountError,
        Map<String, Object> metadata
    ) throws IOException {
        super(name, factories, context, parent, order, format, bucketCountThresholds, collectionMode, showTermDocCountError, metadata);
        this.resultStrategy = resultStrategy.apply(this); // ResultStrategy needs a reference to the Aggregator to do its job.
        this.valuesSource = valuesSource;
        this.includeExclude = includeExclude;
        bucketOrds = new BytesRefHash(1, context.bigArrays());
    }

    @Override
    public ScoreMode scoreMode() {
        if (valuesSource != null && valuesSource.needsScores()) {
            return ScoreMode.COMPLETE;
        }
        return super.scoreMode();
    }

    @Override
    public LeafBucketCollector getLeafCollector(LeafReaderContext ctx,
            final LeafBucketCollector sub) throws IOException {
        SortedBinaryDocValues values = valuesSource.bytesValues(ctx);
        return resultStrategy.wrapCollector(new LeafBucketCollectorBase(sub, values) {
            final BytesRefBuilder previous = new BytesRefBuilder();

            @Override
            public void collect(int doc, long bucket) throws IOException {
                assert bucket == 0;
                if (values.advanceExact(doc)) {
                    final int valuesCount = values.docValueCount();

                    // SortedBinaryDocValues don't guarantee uniqueness so we
                    // need to take care of dups
                    previous.clear();
                    for (int i = 0; i < valuesCount; ++i) {
                        final BytesRef bytes = values.nextValue();
                        if (includeExclude != null && false == includeExclude.accept(bytes)) {
                            continue;
                        }
                        if (i > 0 && previous.get().equals(bytes)) {
                            continue;
                        }
                        long bucketOrdinal = bucketOrds.add(bytes);
                        if (bucketOrdinal < 0) { // already seen
                            bucketOrdinal = -1 - bucketOrdinal;
                            collectExistingBucket(sub, doc, bucketOrdinal);
                        } else {
                            collectBucket(sub, doc, bucketOrdinal);
                        }
                        previous.copyBytes(bytes);
                    }
                }
            }
        });
    }

    @Override
    public InternalAggregation[] buildAggregations(long[] owningBucketOrds) throws IOException {
        return resultStrategy.buildAggregations(owningBucketOrds);
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return buildEmptyTermsAggregation();
    }

    @Override
    public void collectDebugInfo(BiConsumer<String, Object> add) {
        super.collectDebugInfo(add);
        add.accept("result_strategy", resultStrategy.describe());
    }

    @Override
    public void doClose() {
        Releasables.close(bucketOrds, resultStrategy);
    }

    /**
     * Strategy for building results.
     */
    abstract class ResultStrategy<R extends InternalAggregation, B extends InternalMultiBucketAggregation.InternalBucket>
        implements
            Releasable {

        private InternalAggregation[] buildAggregations(long[] owningBucketOrds) throws IOException {
            assert owningBucketOrds.length == 1 && owningBucketOrds[0] == 0;

            collectZeroDocEntriesIfNeeded();

            int size = (int) Math.min(bucketOrds.size(), bucketCountThresholds.getShardSize());

            long otherDocCount = 0;
            PriorityQueue<B> ordered = buildPriorityQueue(size);
            B spare = null;
            for (int bucketOrd = 0; bucketOrd < bucketOrds.size(); bucketOrd++) {
                long docCount = bucketDocCount(bucketOrd);
                otherDocCount += docCount;
                if (docCount < bucketCountThresholds.getShardMinDocCount()) {
                    continue;
                }
                if (spare == null) {
                    spare = buildEmptyBucket();
                }
                updateBucket(spare, bucketOrd, docCount);
                spare = ordered.insertWithOverflow(spare);
            }

            B[] topBuckets = buildBuckets(ordered.size());
            for (int i = ordered.size() - 1; i >= 0; --i) {
                topBuckets[i] = ordered.pop();
                otherDocCount -= topBuckets[i].getDocCount();
                finalizeBucket(topBuckets[i]);
            }

            buildSubAggs(topBuckets);
            return new InternalAggregation[] {
                buildResult(topBuckets, otherDocCount)
            };
        }

        /**
         * Short description of the collection mechanism added to the profile
         * output to help with debugging.
         */
        abstract String describe();

        /**
         * Wrap the "standard" numeric terms collector to collect any more
         * information that this result type may need.
         */
        abstract LeafBucketCollector wrapCollector(LeafBucketCollector primary);

        /**
         * Collect extra entries for "zero" hit documents if they were requested
         * and required.
         */
        abstract void collectZeroDocEntriesIfNeeded() throws IOException;

        /**
         * Build an empty temporary bucket.
         */
        abstract B buildEmptyBucket();

        /**
         * Build a {@link PriorityQueue} to sort the buckets. After we've
         * collected all of the buckets we'll collect all entries in the queue.
         */
        abstract PriorityQueue<B> buildPriorityQueue(int size);

        /**
         * Update fields in {@code spare} to reflect information collected for
         * this bucket ordinal.
         */
        abstract void updateBucket(B spare, long bucketOrd, long docCount) throws IOException;

        /**
         * Build an array of buckets for a particular ordinal to collect the
         * results. The populated list is passed to {@link #buildResult}.
         */
        abstract B[] buildBuckets(int size);

        /**
         * Finalize building a bucket. Called once we know that the bucket will
         * be included in the results.
         */
        abstract void finalizeBucket(B bucket);

        /**
         * Build the sub-aggregations into the buckets. This will usually
         * delegate to {@link #buildSubAggsForAllBuckets}.
         */
        abstract void buildSubAggs(B[] topBuckets) throws IOException;

        /**
         * Turn the buckets into an aggregation result.
         */
        abstract R buildResult(B[] topBuckets, long otherDocCount);

        /**
         * Build an "empty" result. Only called if there isn't any data on this
         * shard.
         */
        abstract R buildEmptyResult();
    }

    /**
     * Builds results for the standard {@code terms} aggregation.
     */
    class StandardTermsResults extends ResultStrategy<StringTerms, StringTerms.Bucket> {
        @Override
        String describe() {
            return "terms";
        }

        @Override
        LeafBucketCollector wrapCollector(LeafBucketCollector primary) {
            return primary;
        }

        @Override
        void collectZeroDocEntriesIfNeeded() throws IOException {
            if (bucketCountThresholds.getMinDocCount() != 0) {
                return;
            }
            if (InternalOrder.isCountDesc(order) && bucketOrds.size() >= bucketCountThresholds.getRequiredSize()) {
                return;
            }
            // we need to fill-in the blanks
            for (LeafReaderContext ctx : context.searcher().getTopReaderContext().leaves()) {
                SortedBinaryDocValues values = valuesSource.bytesValues(ctx);
                // brute force
                for (int docId = 0; docId < ctx.reader().maxDoc(); ++docId) {
                    if (values.advanceExact(docId)) {
                        int valueCount = values.docValueCount();
                        for (int i = 0; i < valueCount; ++i) {
                            BytesRef term = values.nextValue();
                            if (includeExclude == null || includeExclude.accept(term)) {
                                bucketOrds.add(term);
                            }
                        }
                    }
                }
            }
        }

        @Override
        StringTerms.Bucket buildEmptyBucket() {
            return new StringTerms.Bucket(new BytesRef(), 0, null, showTermDocCountError, 0, format);
        }

        @Override
        PriorityQueue<StringTerms.Bucket> buildPriorityQueue(int size) {
            return new BucketPriorityQueue<>(size, partiallyBuiltBucketComparator);
        }

        @Override
        void updateBucket(StringTerms.Bucket spare, long bucketOrd, long docCount) throws IOException {
            bucketOrds.get(bucketOrd, spare.termBytes);
            spare.docCount = docCount;
            spare.bucketOrd = bucketOrd;
        }

        @Override
        StringTerms.Bucket[] buildBuckets(int size) {
            return new StringTerms.Bucket[size];
        }

        @Override
        void finalizeBucket(StringTerms.Bucket bucket) {
            /*
             * termBytes contains a reference to the bytes held by the
             * bucketOrds which will be invalid once the aggregation is
             * closed so we have to copy it.
             */
            bucket.termBytes = BytesRef.deepCopyOf(bucket.termBytes);
        }

        @Override
        void buildSubAggs(StringTerms.Bucket[] topBuckets) throws IOException {
            buildSubAggsForBuckets(topBuckets, b -> b.bucketOrd, (b, a) -> b.aggregations = a);
        }

        @Override
        StringTerms buildResult(StringTerms.Bucket[] topBuckets, long otherDocCount) {
            return new StringTerms(name, order, bucketCountThresholds.getRequiredSize(), bucketCountThresholds.getMinDocCount(),
                metadata(), format, bucketCountThresholds.getShardSize(), showTermDocCountError, otherDocCount,
                Arrays.asList(topBuckets), 0);
        }

        @Override
        StringTerms buildEmptyResult() {
            return buildEmptyTermsAggregation();
        }

        @Override
        public void close() {}
    }

    /**
     * Builds results for the {@code significant_terms} aggregation.
     */
    class SignificantTermsResults extends ResultStrategy<SignificantStringTerms, SignificantStringTerms.Bucket> {
        // TODO a reference to the factory is weird - probably should be reference to what we need from it.
        private final SignificantTermsAggregatorFactory termsAggFactory;
        private final SignificanceHeuristic significanceHeuristic;

        private long subsetSize = 0;

        SignificantTermsResults(SignificantTermsAggregatorFactory termsAggFactory, SignificanceHeuristic significanceHeuristic) {
            this.termsAggFactory = termsAggFactory;
            this.significanceHeuristic = significanceHeuristic;
        }

        @Override
        String describe() {
            return "significant_terms";
        }

        @Override
        LeafBucketCollector wrapCollector(LeafBucketCollector primary) {
            return new LeafBucketCollectorBase(primary, null) {
                @Override
                public void collect(int doc, long owningBucketOrd) throws IOException {
                    super.collect(doc, owningBucketOrd);
                    subsetSize++;
                }
            };
        }

        @Override
        void collectZeroDocEntriesIfNeeded() throws IOException {}

        @Override
        SignificantStringTerms.Bucket buildEmptyBucket() {
            return new SignificantStringTerms.Bucket(new BytesRef(), 0, 0, 0, 0, null, format, 0);
        }

        @Override
        PriorityQueue<SignificantStringTerms.Bucket> buildPriorityQueue(int size) {
            return new BucketSignificancePriorityQueue<>(size);
        }

        @Override
        void updateBucket(SignificantStringTerms.Bucket spare, long bucketOrd, long docCount) throws IOException {
            bucketOrds.get(bucketOrd, spare.termBytes);
            spare.subsetDf = docCount;
            spare.bucketOrd = bucketOrd;
            spare.subsetSize = subsetSize;
            spare.supersetDf = termsAggFactory.getBackgroundFrequency(spare.termBytes);
            spare.supersetSize = termsAggFactory.getSupersetNumDocs();
            /*
             * During shard-local down-selection we use subset/superset stats
             * that are for this shard only. Back at the central reducer these
             * properties will be updated with global stats.
             */
            spare.updateScore(significanceHeuristic);
        }

        @Override
        SignificantStringTerms.Bucket[] buildBuckets(int size) {
            return new SignificantStringTerms.Bucket[size];
        }

        @Override
        void finalizeBucket(SignificantStringTerms.Bucket bucket) {
            /*
             * termBytes contains a reference to the bytes held by the
             * bucketOrds which will be invalid once the aggregation is
             * closed so we have to copy it.
             */
            bucket.termBytes = BytesRef.deepCopyOf(bucket.termBytes);
        }

        @Override
        void buildSubAggs(SignificantStringTerms.Bucket[] topBuckets) throws IOException {
            buildSubAggsForBuckets(topBuckets, b -> b.bucketOrd, (b, a) -> b.aggregations = a);
        }

        @Override
        SignificantStringTerms buildResult(SignificantStringTerms.Bucket[] topBuckets, long otherDocCount) {
            return new SignificantStringTerms(name, bucketCountThresholds.getRequiredSize(),
                bucketCountThresholds.getMinDocCount(),
                metadata(), format, subsetSize, termsAggFactory.getSupersetNumDocs(), significanceHeuristic, Arrays.asList(topBuckets));
        }

        @Override
        SignificantStringTerms buildEmptyResult() {
            return buildEmptySignificantTermsAggregation(significanceHeuristic);
        }

        @Override
        public void close() {
            termsAggFactory.close();
        }
    }
}

