/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.bucket.sampler.random;

import org.apache.lucene.search.CollectionTerminatedException;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.Bits;
import org.elasticsearch.common.CheckedSupplier;
import org.elasticsearch.search.aggregations.AggregationExecutionContext;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.CardinalityUpperBound;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.bucket.BucketsAggregator;
import org.elasticsearch.search.aggregations.bucket.SingleBucketAggregator;
import org.elasticsearch.search.aggregations.support.AggregationContext;

import java.io.IOException;
import java.util.Map;

public class RandomSamplerAggregator extends BucketsAggregator implements SingleBucketAggregator {

    private final int seed;
    private final Integer shardSeed;
    private final double probability;
    private final CheckedSupplier<Weight, IOException> weightSupplier;

    RandomSamplerAggregator(
        String name,
        int seed,
        Integer shardSeed,
        double probability,
        CheckedSupplier<Weight, IOException> weightSupplier,
        AggregatorFactories factories,
        AggregationContext context,
        Aggregator parent,
        CardinalityUpperBound cardinalityUpperBound,
        Map<String, Object> metadata
    ) throws IOException {
        super(name, factories, context, parent, cardinalityUpperBound, metadata);
        this.seed = seed;
        this.probability = probability;
        if (this.subAggregators().length == 0) {
            throw new IllegalArgumentException(
                RandomSamplerAggregationBuilder.NAME + " aggregation [" + name + "] must have sub aggregations configured"
            );
        }
        this.weightSupplier = weightSupplier;
        this.shardSeed = shardSeed;
    }

    @Override
    public InternalAggregation[] buildAggregations(long[] owningBucketOrds) throws IOException {
        return buildAggregationsForSingleBucket(
            owningBucketOrds,
            (owningBucketOrd, subAggregationResults) -> new InternalRandomSampler(
                name,
                bucketDocCount(owningBucketOrd),
                seed,
                shardSeed,
                probability,
                subAggregationResults,
                metadata()
            )
        );
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return new InternalRandomSampler(name, 0, seed, shardSeed, probability, buildEmptySubAggregations(), metadata());
    }

    /**
     * This is an optimized leaf collector that iterates the documents provided the {@link RandomSamplingQuery} directly.
     *
     * Instead of sampling in the foreground (i.e. iterating the documents as they are matched
     * by the {@link RandomSamplerAggregator#topLevelQuery()}), iterating the document set returned by {@link RandomSamplingQuery} directly
     * allows this aggregation to sample documents in the background. This provides a dramatic speed improvement, especially when a
     * non-trivial {@link RandomSamplerAggregator#topLevelQuery()} is provided.
     *
     * @param aggCtx aggregation context
     * @param sub collector
     * @return returns {@link LeafBucketCollector#NO_OP_COLLECTOR} if sampling was done. Otherwise, it is a simple pass through collector
     * @throws IOException when building the query or extracting docs fails
     */
    @Override
    protected LeafBucketCollector getLeafCollector(AggregationExecutionContext aggCtx, LeafBucketCollector sub) throws IOException {
        // Certain leaf collectors can aggregate values without seeing any documents, even when sampled
        // To handle this, exit early if the sub collector is a no-op
        if (sub.isNoop()) {
            return LeafBucketCollector.NO_OP_COLLECTOR;
        }
        // No sampling is being done, collect all docs
        if (probability >= 1.0) {
            grow(1);
            return new LeafBucketCollector() {
                @Override
                public void collect(int doc, long owningBucketOrd) throws IOException {
                    collectExistingBucket(sub, doc, 0);
                }
            };
        }
        // TODO know when sampling would be much slower and skip sampling: https://github.com/elastic/elasticsearch/issues/84353
        Scorer scorer = weightSupplier.get().scorer(aggCtx.getLeafReaderContext());
        // This means there are no docs to iterate, possibly due to the fields not existing
        if (scorer == null) {
            return LeafBucketCollector.NO_OP_COLLECTOR;
        }
        final DocIdSetIterator docIt = scorer.iterator();
        final Bits liveDocs = aggCtx.getLeafReaderContext().reader().getLiveDocs();
        try {
            grow(1);
            // Iterate every document provided by the scorer iterator
            for (int docId = docIt.nextDoc(); docId != DocIdSetIterator.NO_MORE_DOCS; docId = docIt.nextDoc()) {
                // If liveDocs is null, that means that every doc is a live doc, no need to check if it has been deleted or not
                if (liveDocs == null || liveDocs.get(docIt.docID())) {
                    collectExistingBucket(sub, docIt.docID(), 0);
                }
            }
            // This collector could throw `CollectionTerminatedException` if the last leaf collector has stopped collecting
            // So, catch here and indicate no-op
        } catch (CollectionTerminatedException e) {
            return LeafBucketCollector.NO_OP_COLLECTOR;
        }
        // Since we have done our own collection, there is nothing for the leaf collector to do
        return LeafBucketCollector.NO_OP_COLLECTOR;
    }

}
