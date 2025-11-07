/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.rescore;

import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.elasticsearch.common.lucene.search.function.ScriptScoreQuery;
import org.elasticsearch.index.query.ParsedQuery;
import org.elasticsearch.search.vectors.KnnScoreDocQuery;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import static java.util.stream.Collectors.toSet;

public class ScriptRescorer implements Rescorer {

    public static final Rescorer INSTANCE = new ScriptRescorer();

    @Override
    public TopDocs rescore(TopDocs topDocs, IndexSearcher searcher, RescoreContext rescoreContext0) throws IOException {
        assert rescoreContext0 != null;
        if (topDocs == null || topDocs.scoreDocs.length == 0) {
            return topDocs;
        }
        ScriptRescoreContext rescoreContext = (ScriptRescoreContext) rescoreContext0;
        // Take top slice of incoming docs, to be rescored:
        TopDocs topNPreviousPass = Rescorer.topN(topDocs, rescoreContext.getWindowSize());
        // Save doc IDs for which rescoring was applied to be used in score explanation
        Set<Integer> topNDocIDs = Collections.unmodifiableSet(
            Arrays.stream(topNPreviousPass.scoreDocs).map(scoreDoc -> scoreDoc.doc).collect(toSet())
        );
        rescoreContext.setRescoredDocs(topNDocIDs);
        ScriptScoreQuery query = rescoreContext.getQuery();
        // If scores are not needed, we use script_score query with match_all.
        // If scores are needed, we need to substitute match_all with a query that contains docs and scores
        // from topNPreviousPass, and KnnScoreDocQuery is a good fit for that.
        if (rescoreContext.needsScores()) {
            ScoreDoc[] topN = new ScoreDoc[topNPreviousPass.scoreDocs.length];
            System.arraycopy(topNPreviousPass.scoreDocs, 0, topN, 0, topN.length);
            Query subQuery = new KnnScoreDocQuery(topN, searcher.getIndexReader());
            query = query.cloneWithNewSubQuery(subQuery);
            rescoreContext.setExplainQuery(query);
        }
        org.apache.lucene.search.Rescorer rescorer = new org.apache.lucene.search.QueryRescorer(query) {
            @Override
            protected float combine(float firstPassScore, boolean secondPassMatches, float secondPassScore) {
                return secondPassScore;
            }
        };

        // Rescore them
        TopDocs rescored = rescorer.rescore(searcher, topNPreviousPass, rescoreContext.getWindowSize());
        // Splice back to non-topN hits and resort all of them:
        return combine(topDocs, rescored);
    }

    /** Modifies incoming TopDocs (in) by replacing the top hits with resorted's hits, and then resorting all hits. */
    private static TopDocs combine(TopDocs in, TopDocs resorted) {
        System.arraycopy(resorted.scoreDocs, 0, in.scoreDocs, 0, resorted.scoreDocs.length);
        if (in.scoreDocs.length > resorted.scoreDocs.length) {
            Arrays.sort(in.scoreDocs, SCORE_DOC_COMPARATOR);
        }
        return in;
    }

    @Override
    public Explanation explain(int topLevelDocId, IndexSearcher searcher, RescoreContext rescoreContext0, Explanation sourceExplanation)
        throws IOException {
        if (sourceExplanation == null) {
            return Explanation.noMatch("nothing matched");
        }
        if (sourceExplanation.isMatch() == false) {
            return Explanation.noMatch("no match", sourceExplanation);
        }
        ScriptRescoreContext rescoreContext = (ScriptRescoreContext) rescoreContext0;
        if (rescoreContext.isRescored(topLevelDocId)) {
            Explanation rescoreExplanation = searcher.explain(rescoreContext.getExplainQuery(), topLevelDocId);
            if (rescoreExplanation != null && rescoreExplanation.isMatch()) {
                if (rescoreContext.needsScores() == false) {
                    return rescoreExplanation;
                } else {
                    Explanation scoreExp = Explanation.match(sourceExplanation.getValue(), "_score: ", sourceExplanation);
                    return Explanation.match(
                        rescoreExplanation.getValue().floatValue(),
                        rescoreExplanation.getDescription() + ", computed as:",
                        scoreExp
                    );
                }
            }
        }
        return sourceExplanation;
    }

    public static class ScriptRescoreContext extends RescoreContext {
        private final ScriptScoreQuery query;
        private final List<ParsedQuery> parsedQueries;
        private Query explainQuery;

        public ScriptRescoreContext(ParsedQuery parsedQuery, int windowSize) {
            super(windowSize, ScriptRescorer.INSTANCE);
            assert parsedQuery != null : "query must not be null";
            this.query = (ScriptScoreQuery) parsedQuery.query();
            this.parsedQueries = List.of(parsedQuery);
        }

        @Override
        public List<ParsedQuery> getParsedQueries() {
            return parsedQueries;
        }

        public ScriptScoreQuery getQuery() {
            return query;
        }

        public boolean needsScores() {
            return query.needsScore();
        }

        public void setExplainQuery(Query explainQuery) {
            this.explainQuery = explainQuery;
        }

        public Query getExplainQuery() {
            return explainQuery != null ? explainQuery : query;
        }

    }
}
