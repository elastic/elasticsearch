/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.scriptrank;

import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.util.PriorityQueue;
import org.elasticsearch.action.search.SearchPhaseController;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.query.QuerySearchResult;
import org.elasticsearch.search.rank.RankCoordinatorContext;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class ScriptRankCoordinatorContext extends RankCoordinatorContext {
    List<PriorityQueue<ScoreDoc>> queues = new ArrayList<>();

    private final Script script;

    public ScriptRankCoordinatorContext(int size, int from, int windowSize, Script script) {
        super(size, from, windowSize);
        this.script = script;
    }

    protected record RankKey(int doc, int shardIndex) {
    }

    @Override
    public SearchPhaseController.SortedTopDocs rank(List<QuerySearchResult> querySearchResults, SearchPhaseController.TopDocsStats topDocStats) {


        for (QuerySearchResult querySearchResult : querySearchResults) {
            var topDocsList = ((ScriptRankShardResult) querySearchResult.getRankShardResult()).getTopDocsList();

            if (queues.isEmpty()) {
                for (int i = 0; i < topDocsList.size(); ++i) {
                    queues.add(new PriorityQueue<>(windowSize + from) {
                        @Override
                        protected boolean lessThan(ScoreDoc a, ScoreDoc b) {
                            float score1 = a.score;
                            float score2 = b.score;
                            if (score1 != score2) {
                                return score1 < score2;
                            }
                            if (a.shardIndex != b.shardIndex) {
                                return a.shardIndex > b.shardIndex;
                            }
                            return a.doc > b.doc;
                        }
                    });
                }
            }

            for (int i = 0; i <= topDocsList.size(); ++i) {
                for (ScoreDoc scoreDoc : topDocsList.get(i).scoreDocs) {
                    scoreDoc.shardIndex = querySearchResult.getShardIndex();
                    queues.get(i).add(scoreDoc);
                }
            }
        }

        var seen = new HashMap<RankKey, ScoreDoc>();

        for (PriorityQueue<ScoreDoc> priorityQueue : queues) {
            for(ScoreDoc scoreDoc: priorityQueue){
                seen.putIfAbsent(
                    new RankKey(scoreDoc.doc,scoreDoc.shardIndex),
                    scoreDoc
                );
            }
        }

    }
}
