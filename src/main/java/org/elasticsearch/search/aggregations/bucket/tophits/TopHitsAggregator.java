package org.elasticsearch.search.aggregations.bucket.tophits;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.*;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.lucene.ScorerAware;
import org.elasticsearch.common.util.LongObjectPagedHashMap;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.bucket.BucketsAggregator;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.fetch.FetchPhase;
import org.elasticsearch.search.fetch.FetchSearchResult;
import org.elasticsearch.search.internal.InternalSearchHit;

import java.io.IOException;

/**
 */
public class TopHitsAggregator extends BucketsAggregator implements ScorerAware {

    private final FetchPhase fetchPhase;
    private final TopHitsContext topHitsContext;
    private final LongObjectPagedHashMap<TopDocsCollector> topFields;

    private Scorer currentScorer;
    private AtomicReaderContext currentContext;

    public TopHitsAggregator(FetchPhase fetchPhase, TopHitsContext topHitsContext, String name, long estimatedBucketsCount, AggregationContext context, Aggregator parent) {
        super(name, BucketAggregationMode.MULTI_BUCKETS, AggregatorFactories.EMPTY, estimatedBucketsCount, context, parent);
        this.fetchPhase = fetchPhase;
        topFields = new LongObjectPagedHashMap<>(estimatedBucketsCount, context.bigArrays());
        this.topHitsContext = topHitsContext;
        context.registerScorerAware(this);
    }

    @Override
    public boolean shouldCollect() {
        return true;
    }

    @Override
    public InternalAggregation buildAggregation(long owningBucketOrdinal) {
        TopDocsCollector topDocsCollector = topFields.get(owningBucketOrdinal);
        if (topDocsCollector == null) {
            return buildEmptyAggregation();
        } else {
            TopDocs topDocs = topDocsCollector.topDocs();
            if (topDocs.totalHits == 0) {
                return buildEmptyAggregation();
            }

            topHitsContext.queryResult().topDocs(topDocs);
            int[] docIdsToLoad = new int[topDocs.scoreDocs.length];
            for (int i = 0; i < topDocs.scoreDocs.length; i++) {
                docIdsToLoad[i] = topDocs.scoreDocs[i].doc;
            }
            topHitsContext.docIdsToLoad(docIdsToLoad, 0, docIdsToLoad.length);
            fetchPhase.execute(topHitsContext);
            FetchSearchResult fetchResult = topHitsContext.fetchResult();
            InternalSearchHit[] internalHits = fetchResult.fetchResult().hits().internalHits();
            for (int i = 0; i < internalHits.length; i++) {
                InternalSearchHit searchHitFields = internalHits[i];
                searchHitFields.shard(topHitsContext.shardTarget());
                searchHitFields.score(topDocs.scoreDocs[i].score);
            }
            return new InternalTopHits(name, topHitsContext.size(), topHitsContext.sort(), topDocs, fetchResult.hits());
        }
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return new InternalTopHits();
    }

    @Override
    public void collect(int docId, long bucketOrdinal) throws IOException {
        TopDocsCollector topDocsCollector = topFields.get(bucketOrdinal);
        if (topDocsCollector == null) {
            Sort sort = topHitsContext.sort();
            int size = topHitsContext.size();
            topFields.put(
                    bucketOrdinal,
                    topDocsCollector = sort != null ? TopFieldCollector.create(sort, size, true, false, true, false) : TopScoreDocCollector.create(size, false)
            );
            topDocsCollector.setNextReader(currentContext);
            topDocsCollector.setScorer(currentScorer);
        }
        topDocsCollector.collect(docId);
    }

    @Override
    public void setNextReader(AtomicReaderContext context) {
        this.currentContext = context;
        for (LongObjectPagedHashMap.Cursor<TopDocsCollector> cursor : topFields) {
            try {
                cursor.value.setNextReader(context);
            } catch (IOException e) {
                throw ExceptionsHelper.convertToElastic(e);
            }
        }
    }

    @Override
    public void setScorer(Scorer scorer) {
        this.currentScorer = scorer;
        for (LongObjectPagedHashMap.Cursor<TopDocsCollector> cursor : topFields) {
            try {
                cursor.value.setScorer(scorer);
            } catch (IOException e) {
                throw ExceptionsHelper.convertToElastic(e);
            }
        }
    }

    @Override
    protected void doClose() {
        Releasables.close(topFields);
    }

    public static class Factory extends AggregatorFactory {

        private final FetchPhase fetchPhase;
        private final TopHitsContext topHitsContext;

        public Factory(String name, FetchPhase fetchPhase, TopHitsContext topHitsContext) {
            super(name, InternalTopHits.TYPE.name());
            this.fetchPhase = fetchPhase;
            this.topHitsContext = topHitsContext;
        }

        @Override
        public Aggregator create(AggregationContext aggregationContext, Aggregator parent, long expectedBucketsCount) {
            return new TopHitsAggregator(fetchPhase, topHitsContext, name, expectedBucketsCount, aggregationContext, parent);
        }

    }
}
