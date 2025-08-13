/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.search.aggregations.bucket.global;

import org.apache.lucene.search.BulkScorer;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.Weight;
import org.elasticsearch.common.util.LongArray;
import org.elasticsearch.search.aggregations.AggregationExecutionContext;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.CardinalityUpperBound;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.bucket.BucketsAggregator;
import org.elasticsearch.search.aggregations.bucket.SingleBucketAggregator;
import org.elasticsearch.search.aggregations.support.AggregationContext;

import java.io.IOException;
import java.util.Map;

public final class GlobalAggregator extends BucketsAggregator implements SingleBucketAggregator {
    private final Weight weight;

    public GlobalAggregator(String name, AggregatorFactories subFactories, AggregationContext context, Map<String, Object> metadata)
        throws IOException {

        super(name, subFactories, context, null, CardinalityUpperBound.ONE, metadata);
        weight = context.searcher()
            .rewrite(context.filterQuery(new MatchAllDocsQuery()))
            .createWeight(context.searcher(), scoreMode(), 1.0f);
    }

    @Override
    public LeafBucketCollector getLeafCollector(AggregationExecutionContext aggCtx, LeafBucketCollector sub) throws IOException {
        // Run sub-aggregations on child documents
        BulkScorer scorer = weight.bulkScorer(aggCtx.getLeafReaderContext());
        if (scorer == null) {
            return LeafBucketCollector.NO_OP_COLLECTOR;
        }
        grow(1);

        scorer.score(new LeafCollector() {
            @Override
            public void collect(int doc) throws IOException {
                collectExistingBucket(sub, doc, 0);
            }

            @Override
            public void setScorer(Scorable scorer) throws IOException {
                sub.setScorer(scorer);
            }
        }, aggCtx.getLeafReaderContext().reader().getLiveDocs(), 0, DocIdSetIterator.NO_MORE_DOCS);
        return LeafBucketCollector.NO_OP_COLLECTOR;
    }

    @Override
    public InternalAggregation[] buildAggregations(LongArray owningBucketOrds) throws IOException {
        assert owningBucketOrds.size() == 1 && owningBucketOrds.get(0) == 0 : "global aggregator can only be a top level aggregator";
        return buildAggregationsForSingleBucket(
            owningBucketOrds,
            (owningBucketOrd, subAggregationResults) -> new InternalGlobal(
                name,
                bucketDocCount(owningBucketOrd),
                subAggregationResults,
                metadata()
            )
        );
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        throw new UnsupportedOperationException(
            "global aggregations cannot serve as sub-aggregations, hence should never be called on #buildEmptyAggregations"
        );
    }
}
