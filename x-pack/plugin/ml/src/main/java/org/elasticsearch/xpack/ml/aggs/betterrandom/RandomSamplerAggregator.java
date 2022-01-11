/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.aggs.betterrandom;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.ConjunctionUtils;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.Bits;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.CardinalityUpperBound;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.bucket.BucketsAggregator;
import org.elasticsearch.search.aggregations.bucket.SingleBucketAggregator;
import org.elasticsearch.search.aggregations.bucket.sampler.InternalSampler;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.xpack.ml.aggs.FastGeometric;
import org.elasticsearch.xpack.ml.aggs.PCG;
import org.elasticsearch.xpack.ml.randomsampling.RandomSamplingQuery;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.function.IntSupplier;

/**
 * Aggregate random docs on the shard
 */
public class RandomSamplerAggregator extends BucketsAggregator implements SingleBucketAggregator {

    private final FastGeometric geometric;
    private final double probability;
    private final PCG rng;

    RandomSamplerAggregator(
        String name,
        int seed,
        double probability,
        AggregatorFactories factories,
        AggregationContext context,
        Aggregator parent,
        CardinalityUpperBound cardinalityUpperBound,
        Map<String, Object> metadata
    ) throws IOException {
        super(name, factories, context, parent, cardinalityUpperBound, metadata);
        this.geometric = new FastGeometric(new IntSupplier() {
            private final PCG rng = new PCG(seed, context.shardRandomSeed());

            @Override
            public int getAsInt() {
                return rng.nextInt();
            }
        }, probability);
        this.probability = probability;
        this.rng = new PCG(seed, context.shardRandomSeed());
        if (this.subAggregators().length == 0) {
            throw new IllegalArgumentException("must have sub aggs");
        }
    }

    @Override
    public InternalAggregation[] buildAggregations(long[] owningBucketOrds) throws IOException {
        return buildAggregationsForSingleBucket(
            owningBucketOrds,
            (owningBucketOrd, subAggregationResults) -> new InternalSampler(
                name,
                bucketDocCount(owningBucketOrd),
                subAggregationResults,
                metadata()
            )
        );
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return new InternalSampler(name, 0, buildEmptySubAggregations(), metadata());
    }

    @Override
    protected LeafBucketCollector getLeafCollector(LeafReaderContext ctx, LeafBucketCollector sub) throws IOException {
        // TODO Can we optimize this by joining the query results with a random_sample query????
        Query query = searcher().rewrite(topLevelQuery());
        Weight innerWeight = searcher().createWeight(searcher().rewrite(topLevelQuery()), ScoreMode.COMPLETE_NO_SCORES, 1f);
        Weight weight = new ConstantScoreWeight(query, 1f) {
            @Override
            public Scorer scorer(LeafReaderContext context) throws IOException {
                int maxDoc = context.reader().maxDoc();
                Scorer scorer = new ConstantScoreScorer(
                    this,
                    score(),
                    ScoreMode.COMPLETE_NO_SCORES,
                    new RandomSamplingQuery.RandomSamplingIterator(maxDoc, probability, rng::nextInt)
                );
                Scorer queryScorer = innerWeight.scorer(context);
                if (queryScorer == null) {
                    return null;
                }
                return new ConstantScoreScorer(
                    this,
                    score(),
                    ScoreMode.COMPLETE_NO_SCORES,
                    ConjunctionUtils.intersectScorers(List.of(scorer, queryScorer))
                );
            }
            @Override
            public boolean isCacheable(LeafReaderContext ctx) {
                return false;
            }
        };
        Scorer scorer = weight.scorer(ctx);
        if (scorer == null) {
            return LeafBucketCollector.NO_OP_COLLECTOR;
        }
        final DocIdSetIterator docIt = scorer.iterator();
        final Bits liveDocs = ctx.reader().getLiveDocs();
        for (int docId = docIt.nextDoc(); docId != DocIdSetIterator.NO_MORE_DOCS; docId = docIt.nextDoc()) {
            if (liveDocs == null || liveDocs.get(docIt.docID())) {
                collectBucket(sub, docIt.docID(), 0);
            }
        }
        return LeafBucketCollector.NO_OP_COLLECTOR;
    }

}
