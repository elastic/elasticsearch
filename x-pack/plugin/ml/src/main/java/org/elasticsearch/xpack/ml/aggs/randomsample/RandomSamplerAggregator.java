/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.randomsample;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.DocIdSetIterator;
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
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.xpack.ml.randomsample.RandomSamplingQuery;

import java.io.IOException;
import java.util.Map;

public class RandomSamplerAggregator extends BucketsAggregator implements SingleBucketAggregator {

    private final double probability;
    private final int seed;
    private final int hash;

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
        this.probability = probability;
        this.seed = seed;
        this.hash = context.shardRandomSeed();
        if (this.subAggregators().length == 0) {
            throw new IllegalArgumentException("must have sub aggs");
        }
    }

    @Override
    public InternalAggregation[] buildAggregations(long[] owningBucketOrds) throws IOException {
        return buildAggregationsForSingleBucket(
            owningBucketOrds,
            (owningBucketOrd, subAggregationResults) -> new InternalRandomSampler(
                name,
                bucketDocCount(owningBucketOrd),
                seed,
                subAggregationResults,
                metadata()
            )
        );
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return new InternalRandomSampler(name, 0, seed, buildEmptySubAggregations(), metadata());
    }

    @Override
    protected LeafBucketCollector getLeafCollector(LeafReaderContext ctx, LeafBucketCollector sub) throws IOException {
        RandomSamplingQuery query = new RandomSamplingQuery(probability, seed, hash);
        BooleanQuery booleanQuery = new BooleanQuery.Builder()
            .add(query, BooleanClause.Occur.FILTER)
            .add(topLevelQuery(), BooleanClause.Occur.FILTER)
            .build();
        Weight weight = searcher().createWeight(searcher().rewrite(booleanQuery), ScoreMode.COMPLETE_NO_SCORES, 1f);
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
