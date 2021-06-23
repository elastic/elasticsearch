/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.aggregations.bucket.global;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.BulkScorer;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.Weight;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.CardinalityUpperBound;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.bucket.BucketsAggregator;
import org.elasticsearch.search.aggregations.bucket.SingleBucketAggregator;
import org.elasticsearch.search.aggregations.support.AggregationContext;

import java.io.IOException;
import java.util.Map;

public class GlobalAggregator extends BucketsAggregator implements SingleBucketAggregator {
    private final Weight weight;

    public GlobalAggregator(String name, AggregatorFactories subFactories, AggregationContext context, Map<String, Object> metadata)
        throws IOException {

        super(name, subFactories, context, null, CardinalityUpperBound.ONE, metadata);
        weight = context.filterQuery(new MatchAllDocsQuery()).createWeight(context.searcher(), scoreMode(), 1.0f);
    }

    @Override
    public LeafBucketCollector getLeafCollector(LeafReaderContext ctx, LeafBucketCollector sub) throws IOException {
        // Run sub-aggregations on child documents
        BulkScorer scorer = weight.bulkScorer(ctx);
        if (scorer == null) {
            return LeafBucketCollector.NO_OP_COLLECTOR;
        }
        scorer.score(new LeafCollector() {
            @Override
            public void collect(int doc) throws IOException {
                collectBucket(sub, doc, 0);
            }

            @Override
            public void setScorer(Scorable scorer) throws IOException {
                sub.setScorer(scorer);
            }
        }, ctx.reader().getLiveDocs());
        return LeafBucketCollector.NO_OP_COLLECTOR;
    }

    @Override
    public InternalAggregation[] buildAggregations(long[] owningBucketOrds) throws IOException {
        assert owningBucketOrds.length == 1 && owningBucketOrds[0] == 0: "global aggregator can only be a top level aggregator";
        return buildAggregationsForSingleBucket(owningBucketOrds, (owningBucketOrd, subAggregationResults) ->
            new InternalGlobal(name, bucketDocCount(owningBucketOrd), subAggregationResults, metadata())
        );
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        throw new UnsupportedOperationException(
                "global aggregations cannot serve as sub-aggregations, hence should never be called on #buildEmptyAggregations");
    }
}
