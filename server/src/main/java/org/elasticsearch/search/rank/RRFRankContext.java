/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.rank;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.queryparser.xml.builders.BooleanQueryBuilder;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.SimpleCollector;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopDocsCollector;
import org.apache.lucene.search.TopFieldCollector;
import org.apache.lucene.search.TopScoreDocCollector;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.query.QueryCollectorContext;
import org.elasticsearch.search.query.QuerySearchResult;
import org.elasticsearch.search.sort.SortAndFormats;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

public class RRFRankContext implements RankContext {

    public static class RankScorerConsumer implements Consumer<Scorer> {

        protected TopDocsCollector<?> topDocsCollector;
        protected LeafCollector leafCollector;

        protected Scorer scorer;

        public RankScorerConsumer() {
        }

        @Override
        public void accept(Scorer scorer) {
            this.scorer = scorer;
        }
    }

    public static class RRFRankCollector extends SimpleCollector {

        private List<RankScorerConsumer> rankScorerConsumers = new ArrayList<>();

        public RRFRankCollector(int windowSize, Query query, SearchContext searchContext) {
            assert query instanceof BooleanQuery;
            BooleanQuery booleanQuery = (BooleanQuery) query;

            for (BooleanClause booleanClause : booleanQuery.clauses()) {
                assert booleanClause.getQuery() instanceof RankWrapperQuery;
                assert booleanClause.getOccur() == BooleanClause.Occur.SHOULD;
                RankWrapperQuery rankWrapperQuery = (RankWrapperQuery)booleanClause.getQuery();
                assert rankWrapperQuery.getConsumer() instanceof RankScorerConsumer;
                RankScorerConsumer rankScorerConsumer = (RankScorerConsumer)rankWrapperQuery.getConsumer();
                SortAndFormats sortAndFormats = searchContext.sort();
                FieldDoc searchAfter = searchContext.searchAfter();
                if (searchContext.sort() == null) {
                    rankScorerConsumer.topDocsCollector = TopScoreDocCollector.create(windowSize, searchAfter, 1);
                } else {
                    rankScorerConsumer.topDocsCollector = TopFieldCollector.create(sortAndFormats.sort, windowSize, searchAfter, 1);
                }
                rankScorerConsumers.add(rankScorerConsumer);
            }
        }

        @Override
        public ScoreMode scoreMode() {
            return ScoreMode.COMPLETE;
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
                result.setRankResultContext(new RRFRankShardResult(topDocs));
            }
        };
    }

    @Override
    public Query applyRankWrappers(Query query) {
        assert query instanceof BooleanQuery;
        BooleanQuery booleanQuery = (BooleanQuery)query;
        BooleanQuery.Builder booleanQueryBuilder = new BooleanQuery.Builder();
        for (BooleanClause booleanClause : booleanQuery.clauses()) {
            assert booleanClause.getOccur() == BooleanClause.Occur.SHOULD;
            booleanQueryBuilder.add(new RankWrapperQuery(new RankScorerConsumer(), booleanClause.getQuery()), BooleanClause.Occur.SHOULD);
        }
        return booleanQueryBuilder.build();
    }
}
