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

package org.elasticsearch.search.aggregations.metrics.tophits;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.*;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.lucene.ScorerAware;
import org.elasticsearch.common.util.LongObjectPagedHashMap;
import org.elasticsearch.search.aggregations.*;
import org.elasticsearch.search.aggregations.metrics.MetricsAggregator;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.fetch.FetchPhase;
import org.elasticsearch.search.fetch.FetchSearchResult;
import org.elasticsearch.search.internal.InternalSearchHit;
import org.elasticsearch.search.internal.InternalSearchHits;

import java.io.IOException;

/**
 */
public class TopHitsAggregator extends MetricsAggregator implements ScorerAware {

    private final FetchPhase fetchPhase;
    private final TopHitsContext topHitsContext;
    private final LongObjectPagedHashMap<TopDocsCollector> topDocsCollectors;

    private Scorer currentScorer;
    private AtomicReaderContext currentContext;

    public TopHitsAggregator(FetchPhase fetchPhase, TopHitsContext topHitsContext, String name, long estimatedBucketsCount, AggregationContext context, Aggregator parent) {
        super(name, estimatedBucketsCount, context, parent);
        this.fetchPhase = fetchPhase;
        topDocsCollectors = new LongObjectPagedHashMap<>(estimatedBucketsCount, context.bigArrays());
        this.topHitsContext = topHitsContext;
        context.registerScorerAware(this);
    }

    @Override
    public boolean shouldCollect() {
        return true;
    }

    @Override
    public InternalAggregation buildAggregation(long owningBucketOrdinal) {
        TopDocsCollector topDocsCollector = topDocsCollectors.get(owningBucketOrdinal);
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
                ScoreDoc scoreDoc = topDocs.scoreDocs[i];
                InternalSearchHit searchHitFields = internalHits[i];
                searchHitFields.shard(topHitsContext.shardTarget());
                searchHitFields.score(scoreDoc.score);
                if (scoreDoc instanceof FieldDoc) {
                    FieldDoc fieldDoc = (FieldDoc) scoreDoc;
                    searchHitFields.sortValues(fieldDoc.fields);
                }
            }
            return new InternalTopHits(name, topHitsContext.from(), topHitsContext.size(), topDocs, fetchResult.hits());
        }
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return new InternalTopHits(name, topHitsContext.from(), topHitsContext.size(), Lucene.EMPTY_TOP_DOCS, InternalSearchHits.empty());
    }

    @Override
    public void collect(int docId, long bucketOrdinal) throws IOException {
        TopDocsCollector topDocsCollector = topDocsCollectors.get(bucketOrdinal);
        if (topDocsCollector == null) {
            Sort sort = topHitsContext.sort();
            int topN = topHitsContext.from() + topHitsContext.size();
            topDocsCollectors.put(
                    bucketOrdinal,
                    topDocsCollector = sort != null ? TopFieldCollector.create(sort, topN, true, topHitsContext.trackScores(), topHitsContext.trackScores(), false) : TopScoreDocCollector.create(topN, false)
            );
            topDocsCollector.setNextReader(currentContext);
            topDocsCollector.setScorer(currentScorer);
        }
        topDocsCollector.collect(docId);
    }

    @Override
    public void setNextReader(AtomicReaderContext context) {
        this.currentContext = context;
        for (LongObjectPagedHashMap.Cursor<TopDocsCollector> cursor : topDocsCollectors) {
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
        for (LongObjectPagedHashMap.Cursor<TopDocsCollector> cursor : topDocsCollectors) {
            try {
                cursor.value.setScorer(scorer);
            } catch (IOException e) {
                throw ExceptionsHelper.convertToElastic(e);
            }
        }
    }

    @Override
    protected void doClose() {
        Releasables.close(topDocsCollectors);
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

        @Override
        public AggregatorFactory subFactories(AggregatorFactories subFactories) {
            throw new AggregationInitializationException("Aggregator [" + name + "] of type [" + type + "] cannot accept sub-aggregations");
        }

    }
}
