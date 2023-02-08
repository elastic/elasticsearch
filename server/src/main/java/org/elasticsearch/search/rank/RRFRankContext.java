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
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;
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

public class RRFRankContext implements RankContext {

    public static class RRFRankCollector implements Collector {

        private final List<TopDocsCollector<?>> topDocsCollectors = new ArrayList<>();

        public RRFRankCollector(int windowSize, SearchContext searchContext) {
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
        public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
            List<LeafCollector> leafCollectors = new ArrayList<>();
            for (TopDocsCollector<?> topDocsCollector : topDocsCollectors) {
                leafCollectors.add(topDocsCollector.getLeafCollector(context));
            }

            return new LeafCollector() {

                private List<Scorable> scorables;

                @Override
                public void setScorer(Scorable scorer) throws IOException {
                    assert scorer.getChildren().size() == leafCollectors.size();
                    scorables = scorer.getChildren().stream().map(sc -> sc.child).toList();

                    int index = 0;
                    for (Scorable scorable : scorables) {
                        leafCollectors.get(index++).setScorer(scorable);
                    }
                }

                @Override
                public void collect(int doc) throws IOException {
                    int index = 0;
                    for (Scorable scorable : scorables) {
                        if (scorable.docID() == doc) {
                            leafCollectors.get(index).collect(doc);
                        }
                        ++index;
                    }
                }
            };
        }

        @Override
        public ScoreMode scoreMode() {
            return ScoreMode.TOP_SCORES;
        }
    }

    private final int windowSize;
    private final int rankConstant;

    public RRFRankContext(int windowSize, int rankConstant) {
        this.windowSize = windowSize;
        this.rankConstant = rankConstant;
    }

    @Override
    public QueryCollectorContext createRankContextCollector(SearchContext searchContext) {
        return new QueryCollectorContext("rrf") {

            RRFRankCollector rrfRankCollector;

            @Override
            public Collector create(Collector in) throws IOException {
                rrfRankCollector = new RRFRankCollector(windowSize, searchContext);
                return rrfRankCollector;
            }

            @Override
            protected void postProcess(QuerySearchResult result) throws IOException {
                List<TopDocs> topDocs = new ArrayList<>();
                for (TopDocsCollector<?> topDocsCollector : rrfRankCollector.topDocsCollectors) {
                    topDocs.add(topDocsCollector.topDocs());
                }
                //if (true) throw new IllegalArgumentException(topDocs + "");
            }
        };
    }
}
