/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.rank;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.query.QueryCollectorContext;
import org.elasticsearch.search.query.QuerySearchResult;

import java.io.IOException;
import java.util.List;

public class RRFRankContext implements RankContext {

    @Override
    public QueryCollectorContext createRankContextCollector(SearchContext searchContext) {
        //searchContext.size(0);

        return new QueryCollectorContext("rrf") {

            @Override
            public Collector create(Collector in) throws IOException {
                return new Collector() {

                    @Override
                    public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
                        return new LeafCollector() {

                            List<Scorable> children;

                            @Override
                            public void setScorer(Scorable scorer) throws IOException {
                                children = scorer.getChildren().stream().map(sc -> sc.child).toList();
                            }

                            @Override
                            public void collect(int doc) throws IOException {
                                for (Scorable child : children) {
                                    if (child.docID() == doc) {
                                        child.score();
                                    }
                                }
                            }
                        };
                    }

                    @Override
                    public ScoreMode scoreMode() {
                        return ScoreMode.TOP_SCORES;
                    }
                };
            }

            @Override
            protected void postProcess(QuerySearchResult result) throws IOException {

            }
        };
    }
}
