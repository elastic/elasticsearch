/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.rank;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.SimpleCollector;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopDocsCollector;
import org.apache.lucene.search.TopFieldCollector;
import org.apache.lucene.search.TopScoreDocCollector;
import org.elasticsearch.action.search.SearchPhaseController;
import org.elasticsearch.action.search.SearchPhaseController.SortedTopDocs;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.query.QueryCollectorContext;
import org.elasticsearch.search.query.QuerySearchResult;
import org.elasticsearch.search.sort.SortAndFormats;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

public class RRFRankContext implements RankContext {

    public static class RankScorerConsumer implements Consumer<Scorer> {

        protected TopDocsCollector<?> topDocsCollector;
        protected LeafCollector leafCollector;

        protected Scorer scorer;

        @Override
        public void accept(Scorer scorer) {
            this.scorer = scorer;
        }
    }

    public static class RRFRankCollector extends SimpleCollector {

        private final List<RankScorerConsumer> rankScorerConsumers = new ArrayList<>();
        private final ScoreMode scoreMode;

        public RRFRankCollector(int windowSize, Query query, SearchContext searchContext) {
            assert query instanceof RRFRankQuery;
            RRFRankQuery RRFRankQuery = (RRFRankQuery) query;

            for (Consumer<Scorer> consumer : RRFRankQuery.getConsumers()) {
                assert consumer instanceof RankScorerConsumer;
                RankScorerConsumer rankScorerConsumer = (RankScorerConsumer) consumer;
                SortAndFormats sortAndFormats = searchContext.sort();
                FieldDoc searchAfter = searchContext.searchAfter();
                if (searchContext.sort() == null) {
                    rankScorerConsumer.topDocsCollector = TopScoreDocCollector.create(windowSize, searchAfter, 1);
                } else {
                    rankScorerConsumer.topDocsCollector = TopFieldCollector.create(sortAndFormats.sort, windowSize, searchAfter, 1);
                }
                rankScorerConsumers.add(rankScorerConsumer);
            }
            scoreMode = searchContext.sort() == null ? ScoreMode.TOP_SCORES : ScoreMode.TOP_DOCS;
        }

        @Override
        public ScoreMode scoreMode() {
            return scoreMode;
        }

        @Override
        public void doSetNextReader(LeafReaderContext context) throws IOException {
            for (RankScorerConsumer rankScorerConsumer : rankScorerConsumers) {
                rankScorerConsumer.leafCollector = rankScorerConsumer.topDocsCollector.getLeafCollector(context);
            }
        }

        @Override
        public void setScorer(Scorable scorer) throws IOException {
            for (RankScorerConsumer rankScorerConsumer : rankScorerConsumers) {
                rankScorerConsumer.leafCollector.setScorer(rankScorerConsumer.scorer);
            }
        }

        @Override
        public void collect(int doc) throws IOException {
            for (RankScorerConsumer rankScorerConsumer : rankScorerConsumers) {
                if (rankScorerConsumer.scorer.docID() == doc) {
                    rankScorerConsumer.leafCollector.collect(doc);
                }
            }
        }
    }

    private final int windowSize;
    private final int rankConstant;

    public RRFRankContext(int windowSize, int rankConstant) {
        this.windowSize = windowSize;
        this.rankConstant = rankConstant;
    }

    @Override
    public Query updateQuery(Query query) {
        assert query instanceof BooleanQuery;
        BooleanQuery booleanQuery = (BooleanQuery) query;
        List<Consumer<Scorer>> rankScorerConsumers = new ArrayList<>();
        List<Query> queries = new ArrayList<>();
        for (BooleanClause booleanClause : booleanQuery.clauses()) {
            assert booleanClause.getOccur() == BooleanClause.Occur.SHOULD;
            rankScorerConsumers.add(new RankScorerConsumer());
            queries.add(booleanClause.getQuery());
        }
        return new RRFRankQuery(rankScorerConsumers, queries);
    }

    @Override
    public QueryCollectorContext createQueryCollectorContext(Query query, SearchContext searchContext) {
        return new QueryCollectorContext("rrf") {

            RRFRankCollector rrfRankCollector;

            @Override
            public Collector create(Collector in) throws IOException {
                rrfRankCollector = new RRFRankCollector(windowSize, query, searchContext);
                return rrfRankCollector;
            }

            @Override
            protected void postProcess(QuerySearchResult result) throws IOException {
                List<TopDocs> topDocs = new ArrayList<>();
                for (RankScorerConsumer rankScorerConsumer : rrfRankCollector.rankScorerConsumers) {
                    topDocs.add(rankScorerConsumer.topDocsCollector.topDocs());
                }
                result.setRankShardResult(new RRFRankShardResult(topDocs));
            }
        };
    }

    @Override
    public SortedTopDocs rank(List<QuerySearchResult> querySearchResults) {
        List<List<TopDocs>> unmergedTopDocs = new ArrayList<>();
        int size = -1;

        for (QuerySearchResult querySearchResult : querySearchResults) {
            RankShardResult rankShardResult = querySearchResult.getRankShardResult();
            assert rankShardResult instanceof RRFRankShardResult;
            RRFRankShardResult rrfRankShardResult = (RRFRankShardResult) rankShardResult;
            List<TopDocs> shardTopDocs = rrfRankShardResult.getTopDocs();

            if (size == -1) {
                size = shardTopDocs.size();
                for (int index = 0; index < size; ++index) {
                    unmergedTopDocs.add(new ArrayList<>());
                }
            }
            assert size == shardTopDocs.size();

            for (int index = 0; index < size; ++index) {
                TopDocs std = shardTopDocs.get(index);
                SearchPhaseController.setShardIndex(std, querySearchResult.getShardIndex());
                unmergedTopDocs.get(index).add(std);
            }
        }

        List<SortedTopDocs> mergedTopDocs = new ArrayList<>();
        for (List<TopDocs> utd : unmergedTopDocs) {
            mergedTopDocs.add(SearchPhaseController.sortDocs(false, utd, 0, windowSize, List.of()));
        }

        Map<String, RRFRankResult> docsToRankResults = new HashMap<>();

        // TODO: ensure sorted top docs have the same sort values

        int index = 0;
        for (SortedTopDocs mtd : mergedTopDocs) {
            int rank = 0;
            for (ScoreDoc scoreDoc : mtd.scoreDocs()) {
                ++rank;
                final int findex = index;
                final int frank = rank;
                docsToRankResults.compute(scoreDoc.doc + ":" + scoreDoc.shardIndex, (key, value) -> {
                    if (value == null) {
                        value = new RRFRankResult(scoreDoc.doc, scoreDoc.shardIndex, mergedTopDocs.size());
                    }

                    value.score += 1.0f / (rankConstant + frank);
                    value.positions[findex] = frank;
                    value.scores[findex] = scoreDoc.score;

                    return value;
                });
            }
            ++index;
        }

        RRFRankResult[] rankResults = docsToRankResults.values().toArray(RRFRankResult[]::new);
        Arrays.sort(rankResults, Comparator.comparingDouble(rr -> -rr.score));
        for (int rank = 0; rank < rankResults.length; ++rank) {
            rankResults[rank].rank = rank + 1;
        }

        SortedTopDocs copy = mergedTopDocs.get(1);
        return new SortedTopDocs(
            rankResults,
            copy.isSortedByField(),
            copy.sortFields(),
            copy.collapseField(),
            copy.collapseValues(),
            copy.numberOfCompletionsSuggestions()
        );
    }

    public void decorateSearchHit(ScoreDoc scoreDoc, SearchHit searchHit) {
        assert scoreDoc instanceof RRFRankResult;
        RRFRankResult rankResult = (RRFRankResult) scoreDoc;
        searchHit.setRank(rankResult.rank);
        searchHit.setRankResult(rankResult);
    }
}
