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

import com.carrotsearch.hppc.LongObjectHashMap;
import com.carrotsearch.hppc.cursors.ObjectCursor;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopDocsCollector;
import org.apache.lucene.search.TopFieldCollector;
import org.apache.lucene.search.TopFieldDocs;
import org.apache.lucene.search.TopScoreDocCollector;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.util.LongObjectPagedHashMap;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.LeafBucketCollectorBase;
import org.elasticsearch.search.aggregations.metrics.MetricsAggregator;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.fetch.FetchPhase;
import org.elasticsearch.search.fetch.FetchSearchResult;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.internal.SubSearchContext;
import org.elasticsearch.search.rescore.RescoreContext;
import org.elasticsearch.search.sort.SortAndFormats;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class TopHitsAggregator extends MetricsAggregator {

    private final FetchPhase fetchPhase;
    private final SubSearchContext subSearchContext;
    private final LongObjectPagedHashMap<TopDocsCollector<?>> topDocsCollectors;

    TopHitsAggregator(FetchPhase fetchPhase, SubSearchContext subSearchContext, String name, SearchContext context,
            Aggregator parent, List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData) throws IOException {
        super(name, context, parent, pipelineAggregators, metaData);
        this.fetchPhase = fetchPhase;
        topDocsCollectors = new LongObjectPagedHashMap<>(1, context.bigArrays());
        this.subSearchContext = subSearchContext;
    }

    @Override
    public boolean needsScores() {
        SortAndFormats sort = subSearchContext.sort();
        if (sort != null) {
            return sort.sort.needsScores() || subSearchContext.trackScores();
        } else {
            // sort by score
            return true;
        }
    }

    @Override
    public LeafBucketCollector getLeafCollector(LeafReaderContext ctx, LeafBucketCollector sub) throws IOException {
        // Create leaf collectors here instead of at the aggregator level. Otherwise in case this collector get invoked
        // when post collecting then we have already replaced the leaf readers on the aggregator level have already been
        // replaced with the next leaf readers and then post collection pushes docids of the previous segement, which
        // then causes assertions to trip or incorrect top docs to be computed.
        final LongObjectHashMap<LeafCollector> leafCollectors = new LongObjectHashMap<>(1);
        return new LeafBucketCollectorBase(sub, null) {

            Scorer scorer;

            @Override
            public void setScorer(Scorer scorer) throws IOException {
                this.scorer = scorer;
                super.setScorer(scorer);
                for (ObjectCursor<LeafCollector> cursor : leafCollectors.values()) {
                    cursor.value.setScorer(scorer);
                }
            }

            @Override
            public void collect(int docId, long bucket) throws IOException {
                TopDocsCollector<?> topDocsCollector = topDocsCollectors.get(bucket);
                if (topDocsCollector == null) {
                    SortAndFormats sort = subSearchContext.sort();
                    int topN = subSearchContext.from() + subSearchContext.size();
                    if (sort == null) {
                        for (RescoreContext rescoreContext : context.rescore()) {
                            topN = Math.max(rescoreContext.getWindowSize(), topN);
                        }
                    }
                    // In the QueryPhase we don't need this protection, because it is build into the IndexSearcher,
                    // but here we create collectors ourselves and we need prevent OOM because of crazy an offset and size.
                    topN = Math.min(topN, subSearchContext.searcher().getIndexReader().maxDoc());
                    if (sort == null) {
                        topDocsCollector = TopScoreDocCollector.create(topN);
                    } else {
                        topDocsCollector = TopFieldCollector.create(sort.sort, topN, true, subSearchContext.trackScores(),
                            subSearchContext.trackScores());
                    }
                    topDocsCollectors.put(bucket, topDocsCollector);
                }

                final LeafCollector leafCollector;
                final int key = leafCollectors.indexOf(bucket);
                if (key < 0) {
                    leafCollector = topDocsCollector.getLeafCollector(ctx);
                    if (scorer != null) {
                        leafCollector.setScorer(scorer);
                    }
                    leafCollectors.indexInsert(key, bucket, leafCollector);
                } else {
                    leafCollector = leafCollectors.indexGet(key);
                }
                leafCollector.collect(docId);
            }
        };
    }

    @Override
    public InternalAggregation buildAggregation(long owningBucketOrdinal) {
        TopDocsCollector<?> topDocsCollector = topDocsCollectors.get(owningBucketOrdinal);
        final InternalTopHits topHits;
        if (topDocsCollector == null) {
            topHits = buildEmptyAggregation();
        } else {
            TopDocs topDocs = topDocsCollector.topDocs();
            if (subSearchContext.sort() == null) {
                for (RescoreContext ctx : context().rescore()) {
                    try {
                        topDocs = ctx.rescorer().rescore(topDocs, context.searcher(), ctx);
                    } catch (IOException e) {
                        throw new ElasticsearchException("Rescore TopHits Failed", e);
                    }
                }
            }
            subSearchContext.queryResult().topDocs(topDocs,
                subSearchContext.sort() == null ? null : subSearchContext.sort().formats);
            int[] docIdsToLoad = new int[topDocs.scoreDocs.length];
            for (int i = 0; i < topDocs.scoreDocs.length; i++) {
                docIdsToLoad[i] = topDocs.scoreDocs[i].doc;
            }
            subSearchContext.docIdsToLoad(docIdsToLoad, 0, docIdsToLoad.length);
            fetchPhase.execute(subSearchContext);
            FetchSearchResult fetchResult = subSearchContext.fetchResult();
            SearchHit[] internalHits = fetchResult.fetchResult().hits().getHits();
            for (int i = 0; i < internalHits.length; i++) {
                ScoreDoc scoreDoc = topDocs.scoreDocs[i];
                SearchHit searchHitFields = internalHits[i];
                searchHitFields.shard(subSearchContext.shardTarget());
                searchHitFields.score(scoreDoc.score);
                if (scoreDoc instanceof FieldDoc) {
                    FieldDoc fieldDoc = (FieldDoc) scoreDoc;
                    searchHitFields.sortValues(fieldDoc.fields, subSearchContext.sort().formats);
                }
            }
            topHits = new InternalTopHits(name, subSearchContext.from(), subSearchContext.size(), topDocs, fetchResult.hits(),
                    pipelineAggregators(), metaData());
        }
        return topHits;
    }

    @Override
    public InternalTopHits buildEmptyAggregation() {
        TopDocs topDocs;
        if (subSearchContext.sort() != null) {
            topDocs = new TopFieldDocs(0, new FieldDoc[0], subSearchContext.sort().sort.getSort(), Float.NaN);
        } else {
            topDocs = Lucene.EMPTY_TOP_DOCS;
        }
        return new InternalTopHits(name, subSearchContext.from(), subSearchContext.size(), topDocs, SearchHits.empty(),
                pipelineAggregators(), metaData());
    }

    @Override
    protected void doClose() {
        Releasables.close(topDocsCollectors);
    }
}
