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
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopDocsCollector;
import org.apache.lucene.search.TopFieldCollector;
import org.apache.lucene.search.TopScoreDocCollector;
import org.apache.lucene.search.Weight;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.query.QueryCollectorContext;
import org.elasticsearch.search.query.QuerySearchResult;
import org.elasticsearch.search.sort.SortAndFormats;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class RRFRankContext implements RankContext {

    public static class RRFRankCollector extends RankCollector {

        private final List<TopDocsCollector<?>> topDocsCollectors = new ArrayList<>();

        private List<LeafCollector> leafCollectors;
        private List<Scorable> scorables;

        public RRFRankCollector(Collector childCollector, int windowSize, SearchContext searchContext) {
            super(childCollector);

            assert searchContext.query() instanceof BooleanQuery;
            BooleanQuery booleanQuery = (BooleanQuery) searchContext.query();

            for (BooleanClause booleanClause : booleanQuery.clauses()) {
                assert booleanClause.getOccur() == BooleanClause.Occur.SHOULD;

                SortAndFormats sortAndFormats = searchContext.sort();
                FieldDoc searchAfter = searchContext.searchAfter();
                if (searchContext.sort() == null) {
                    topDocsCollectors.add(TopScoreDocCollector.create(windowSize, searchAfter, 1));
                } else {
                    topDocsCollectors.add(TopFieldCollector.create(sortAndFormats.sort, windowSize, searchAfter, 1));
                }
            }
        }

        @Override
        public ScoreMode scoreMode() {
            // TODO: make this work with child collector
            return ScoreMode.COMPLETE;
        }

        @Override
        public void setNextReader(LeafReaderContext context) throws IOException {
            leafCollectors = new ArrayList<>();
            for (TopDocsCollector<?> topDocsCollector : topDocsCollectors) {
                leafCollectors.add(topDocsCollector.getLeafCollector(context));
            }
        }

        public void doSetWeight(Weight weight) {

        }

        @Override
        public void doSetScorer(Scorable scorable) throws IOException {
            scorables = scorable.getChildren().stream().map(sc -> sc.child).toList();

            int index = 0;
            for (Scorable child : scorables) {
                leafCollectors.get(index++).setScorer(child);
            }
        }

        @Override
        public void doCollect(int doc) throws IOException {
            int index = 0;
            for (Scorable scorable : scorables) {
                if (scorable.docID() == doc) {
                    leafCollectors.get(index).collect(doc);
                }
                ++index;
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
    public QueryCollectorContext createQueryCollectorContext(SearchContext searchContext) {
        return new QueryCollectorContext("rrf") {

            RRFRankCollector rrfRankCollector;

            @Override
            public Collector create(Collector in) throws IOException {
                rrfRankCollector = new RRFRankCollector(in, windowSize, searchContext);
                return rrfRankCollector;
            }

            @Override
            protected void postProcess(QuerySearchResult result) throws IOException {
                List<TopDocs> topDocs = new ArrayList<>();
                for (TopDocsCollector<?> topDocsCollector : rrfRankCollector.topDocsCollectors) {
                    topDocs.add(topDocsCollector.topDocs());
                }
                result.setRankResultContext(new RRFRankShardResult(topDocs));
            }
        };
    }
}
