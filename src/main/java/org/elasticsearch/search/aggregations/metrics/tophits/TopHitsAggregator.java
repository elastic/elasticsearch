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

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopDocsCollector;
import org.apache.lucene.search.TopFieldCollector;
import org.apache.lucene.search.TopFieldDocs;
import org.apache.lucene.search.TopScoreDocCollector;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.util.LongObjectPagedHashMap;
import org.elasticsearch.search.aggregations.AggregationInitializationException;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.LeafBucketCollectorBase;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.metrics.MetricsAggregator;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.fetch.FetchPhase;
import org.elasticsearch.search.fetch.FetchSearchResult;
import org.elasticsearch.search.internal.InternalSearchHit;
import org.elasticsearch.search.internal.InternalSearchHits;
import org.elasticsearch.search.internal.SubSearchContext;

import java.io.IOException;
import java.util.Map;

/**
 */
public class TopHitsAggregator extends MetricsAggregator {

    /** Simple wrapper around a top-level collector and the current leaf collector. */
    private static class TopDocsAndLeafCollector {
        final TopDocsCollector<?> topLevelCollector;
        LeafCollector leafCollector;

        TopDocsAndLeafCollector(TopDocsCollector<?> topLevelCollector) {
            this.topLevelCollector = topLevelCollector;
        }
    }

    final FetchPhase fetchPhase;
    final SubSearchContext subSearchContext;
    final LongObjectPagedHashMap<TopDocsAndLeafCollector> topDocsCollectors;

    public TopHitsAggregator(FetchPhase fetchPhase, SubSearchContext subSearchContext, String name, AggregationContext context, Aggregator parent, Map<String, Object> metaData) throws IOException {
        super(name, context, parent, metaData);
        this.fetchPhase = fetchPhase;
        topDocsCollectors = new LongObjectPagedHashMap<>(1, context.bigArrays());
        this.subSearchContext = subSearchContext;
    }

    @Override
    public boolean needsScores() {
        Sort sort = subSearchContext.sort();
        if (sort != null) {
            return sort.needsScores() || subSearchContext.trackScores();
        } else {
            // sort by score
            return true;
        }
    }

    @Override
    public LeafBucketCollector getLeafCollector(final LeafReaderContext ctx,
            final LeafBucketCollector sub) throws IOException {

        for (LongObjectPagedHashMap.Cursor<TopDocsAndLeafCollector> cursor : topDocsCollectors) {
            cursor.value.leafCollector = cursor.value.topLevelCollector.getLeafCollector(ctx);
        }

        return new LeafBucketCollectorBase(sub, null) {

            Scorer scorer;

            @Override
            public void setScorer(Scorer scorer) throws IOException {
                this.scorer = scorer;
                for (LongObjectPagedHashMap.Cursor<TopDocsAndLeafCollector> cursor : topDocsCollectors) {
                    cursor.value.leafCollector.setScorer(scorer);
                }
                super.setScorer(scorer);
            }

            @Override
            public void collect(int docId, long bucket) throws IOException {
                TopDocsAndLeafCollector collectors = topDocsCollectors.get(bucket);
                if (collectors == null) {
                    Sort sort = subSearchContext.sort();
                    int topN = subSearchContext.from() + subSearchContext.size();
                    TopDocsCollector<?> topLevelCollector = sort != null ? TopFieldCollector.create(sort, topN, true, subSearchContext.trackScores(), subSearchContext.trackScores()) : TopScoreDocCollector.create(topN);
                    collectors = new TopDocsAndLeafCollector(topLevelCollector);
                    collectors.leafCollector = collectors.topLevelCollector.getLeafCollector(ctx);
                    collectors.leafCollector.setScorer(scorer);
                    topDocsCollectors.put(bucket, collectors);
                }
                collectors.leafCollector.collect(docId);
            }
        };
    }

    @Override
    public InternalAggregation buildAggregation(long owningBucketOrdinal) {
        TopDocsAndLeafCollector topDocsCollector = topDocsCollectors.get(owningBucketOrdinal);
        final InternalTopHits topHits;
        if (topDocsCollector == null) {
            topHits = buildEmptyAggregation();
        } else {
            final TopDocs topDocs = topDocsCollector.topLevelCollector.topDocs();

            subSearchContext.queryResult().topDocs(topDocs);
            int[] docIdsToLoad = new int[topDocs.scoreDocs.length];
            for (int i = 0; i < topDocs.scoreDocs.length; i++) {
                docIdsToLoad[i] = topDocs.scoreDocs[i].doc;
            }
            subSearchContext.docIdsToLoad(docIdsToLoad, 0, docIdsToLoad.length);
            fetchPhase.execute(subSearchContext);
            FetchSearchResult fetchResult = subSearchContext.fetchResult();
            InternalSearchHit[] internalHits = fetchResult.fetchResult().hits().internalHits();
            for (int i = 0; i < internalHits.length; i++) {
                ScoreDoc scoreDoc = topDocs.scoreDocs[i];
                InternalSearchHit searchHitFields = internalHits[i];
                searchHitFields.shard(subSearchContext.shardTarget());
                searchHitFields.score(scoreDoc.score);
                if (scoreDoc instanceof FieldDoc) {
                    FieldDoc fieldDoc = (FieldDoc) scoreDoc;
                    searchHitFields.sortValues(fieldDoc.fields);
                }
            }
            topHits = new InternalTopHits(name, subSearchContext.from(), subSearchContext.size(), topDocs, fetchResult.hits());
        }
        return topHits;
    }

    @Override
    public InternalTopHits buildEmptyAggregation() {
        TopDocs topDocs;
        if (subSearchContext.sort() != null) {
            topDocs = new TopFieldDocs(0, new FieldDoc[0], subSearchContext.sort().getSort(), Float.NaN);
        } else {
            topDocs = Lucene.EMPTY_TOP_DOCS;
        }
        return new InternalTopHits(name, subSearchContext.from(), subSearchContext.size(), topDocs, InternalSearchHits.empty());
    }

    @Override
    protected void doClose() {
        Releasables.close(topDocsCollectors);
    }

    public static class Factory extends AggregatorFactory {

        private final FetchPhase fetchPhase;
        private final SubSearchContext subSearchContext;

        public Factory(String name, FetchPhase fetchPhase, SubSearchContext subSearchContext) {
            super(name, InternalTopHits.TYPE.name());
            this.fetchPhase = fetchPhase;
            this.subSearchContext = subSearchContext;
        }

        @Override
        public Aggregator createInternal(AggregationContext aggregationContext, Aggregator parent, boolean collectsFromSingleBucket, Map<String, Object> metaData) throws IOException {
            return new TopHitsAggregator(fetchPhase, subSearchContext, name, aggregationContext, parent, metaData);
        }

        @Override
        public AggregatorFactory subFactories(AggregatorFactories subFactories) {
            throw new AggregationInitializationException("Aggregator [" + name + "] of type [" + type + "] cannot accept sub-aggregations");
        }
    }
}
