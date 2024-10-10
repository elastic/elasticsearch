/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.search.aggregations.bucket.nested;

import org.apache.lucene.index.IndexReaderContext;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.join.BitSetProducer;
import org.apache.lucene.util.BitSet;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.index.mapper.NestedObjectMapper;
import org.elasticsearch.search.aggregations.AggregationExecutionContext;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.CardinalityUpperBound;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.LeafBucketCollectorBase;
import org.elasticsearch.search.aggregations.bucket.BucketsAggregator;
import org.elasticsearch.search.aggregations.bucket.SingleBucketAggregator;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.xcontent.ParseField;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class NestedAggregator extends BucketsAggregator implements SingleBucketAggregator {

    static final ParseField PATH_FIELD = new ParseField("path");

    private final BitSetProducer parentFilter;
    private final Query childFilter;
    private final boolean collectsFromSingleBucket;

    private BufferingNestedLeafBucketCollector bufferingNestedLeafBucketCollector;

    NestedAggregator(
        String name,
        AggregatorFactories factories,
        NestedObjectMapper parentObjectMapper,
        NestedObjectMapper childObjectMapper,
        AggregationContext context,
        Aggregator parent,
        CardinalityUpperBound cardinality,
        Map<String, Object> metadata
    ) throws IOException {
        super(name, factories, context, parent, cardinality, metadata);

        Query parentFilter = parentObjectMapper != null
            ? parentObjectMapper.nestedTypeFilter()
            : Queries.newNonNestedFilter(context.getIndexSettings().getIndexVersionCreated());
        this.parentFilter = context.bitsetFilterCache().getBitSetProducer(parentFilter);
        this.childFilter = childObjectMapper.nestedTypeFilter();
        this.collectsFromSingleBucket = cardinality.map(estimate -> estimate < 2);
    }

    @Override
    public LeafBucketCollector getLeafCollector(final AggregationExecutionContext aggCtx, final LeafBucketCollector sub)
        throws IOException {
        IndexReaderContext topLevelContext = ReaderUtil.getTopLevelContext(aggCtx.getLeafReaderContext());
        IndexSearcher searcher = new IndexSearcher(topLevelContext);
        searcher.setQueryCache(null);
        Weight weight = searcher.createWeight(searcher.rewrite(childFilter), ScoreMode.COMPLETE_NO_SCORES, 1f);
        Scorer childDocsScorer = weight.scorer(aggCtx.getLeafReaderContext());

        final BitSet parentDocs = parentFilter.getBitSet(aggCtx.getLeafReaderContext());
        final DocIdSetIterator childDocs = childDocsScorer != null ? childDocsScorer.iterator() : null;
        if (collectsFromSingleBucket) {
            return new LeafBucketCollectorBase(sub, null) {
                @Override
                public void collect(int parentDoc, long bucket) throws IOException {
                    // if parentDoc is 0 then this means that this parent doesn't have child docs (b/c these appear always before the parent
                    // doc), so we can skip:
                    if (parentDoc == 0 || parentDocs == null || childDocs == null) {
                        return;
                    }

                    final int prevParentDoc = parentDocs.prevSetBit(parentDoc - 1);
                    int childDocId = childDocs.docID();
                    if (childDocId <= prevParentDoc) {
                        childDocId = childDocs.advance(prevParentDoc + 1);
                    }

                    for (; childDocId < parentDoc; childDocId = childDocs.nextDoc()) {
                        collectBucket(sub, childDocId, bucket);
                    }
                }
            };
        } else {
            return bufferingNestedLeafBucketCollector = new BufferingNestedLeafBucketCollector(sub, parentDocs, childDocs);
        }
    }

    @Override
    protected void preGetSubLeafCollectors(LeafReaderContext ctx) throws IOException {
        super.preGetSubLeafCollectors(ctx);
        processBufferedDocs();
    }

    @Override
    protected void doPostCollection() throws IOException {
        processBufferedDocs();
    }

    private void processBufferedDocs() throws IOException {
        if (bufferingNestedLeafBucketCollector != null) {
            bufferingNestedLeafBucketCollector.processBufferedChildBuckets();
        }
    }

    @Override
    public InternalAggregation[] buildAggregations(long[] owningBucketOrds) throws IOException {
        return buildAggregationsForSingleBucket(
            owningBucketOrds,
            (owningBucketOrd, subAggregationResults) -> new InternalNested(
                name,
                bucketDocCount(owningBucketOrd),
                subAggregationResults,
                metadata()
            )
        );
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return new InternalNested(name, 0, buildEmptySubAggregations(), metadata());
    }

    class BufferingNestedLeafBucketCollector extends LeafBucketCollectorBase {

        final BitSet parentDocs;
        final LeafBucketCollector sub;
        final DocIdSetIterator childDocs;
        final List<Long> bucketBuffer = new ArrayList<>();

        Scorable scorer;
        int currentParentDoc = -1;
        final CachedScorable cachedScorer = new CachedScorable();

        BufferingNestedLeafBucketCollector(LeafBucketCollector sub, BitSet parentDocs, DocIdSetIterator childDocs) {
            super(sub, null);
            this.sub = sub;
            this.parentDocs = parentDocs;
            this.childDocs = childDocs;
        }

        @Override
        public void setScorer(Scorable scorer) throws IOException {
            this.scorer = scorer;
            super.setScorer(cachedScorer);
        }

        @Override
        public void collect(int parentDoc, long bucket) throws IOException {
            // if parentDoc is 0 then this means that this parent doesn't have child docs (b/c these appear always before the parent
            // doc), so we can skip:
            if (parentDoc == 0 || parentDocs == null || childDocs == null) {
                return;
            }

            if (currentParentDoc != parentDoc) {
                processBufferedChildBuckets();
                if (scoreMode().needsScores()) {
                    // cache the score of the current parent
                    cachedScorer.score = scorer.score();
                }
                currentParentDoc = parentDoc;

            }
            bucketBuffer.add(bucket);
        }

        void processBufferedChildBuckets() throws IOException {
            if (bucketBuffer.isEmpty()) {
                return;
            }

            final int prevParentDoc = parentDocs.prevSetBit(currentParentDoc - 1);
            int childDocId = childDocs.docID();
            if (childDocId <= prevParentDoc) {
                childDocId = childDocs.advance(prevParentDoc + 1);
            }

            for (; childDocId < currentParentDoc; childDocId = childDocs.nextDoc()) {
                for (var bucket : bucketBuffer) {
                    collectBucket(sub, childDocId, bucket);
                }
            }
            bucketBuffer.clear();
        }
    }

    private static class CachedScorable extends Scorable {
        float score;

        @Override
        public final float score() {
            return score;
        }
    }

}
