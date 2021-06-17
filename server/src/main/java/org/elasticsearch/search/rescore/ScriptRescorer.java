/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.rescore;

import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.elasticsearch.index.query.ScriptQueryBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import static java.util.stream.Collectors.toSet;

public class ScriptRescorer implements Rescorer {

    public static final Rescorer INSTANCE = new ScriptRescorer();

    @Override
    public TopDocs rescore(TopDocs topDocs, IndexSearcher searcher, RescoreContext rescoreContext) throws IOException {

        assert rescoreContext != null;
        if (topDocs == null || topDocs.scoreDocs.length == 0) {
            return topDocs;
        }

        // First take top slice of incoming docs, to be rescored:
        TopDocs topNFirstPass = topN(topDocs, rescoreContext.getWindowSize());

        // Save doc IDs for which rescoring was applied to be used in score explanation
        Set<Integer> topNDocIDs = Collections.unmodifiableSet(
                Arrays.stream(topNFirstPass.scoreDocs).map(scoreDoc -> scoreDoc.doc).collect(toSet()));

        rescoreContext.setRescoredDocs(topNDocIDs);

        final ScriptRescoreContext rescore = (ScriptRescorer.ScriptRescoreContext) rescoreContext;
        ScriptRescorerBuilder.ScriptQuery query = rescore.query();
        if (rescore.needsScores) {
            query.setHits(topNFirstPass.scoreDocs);
        }
        org.apache.lucene.search.Rescorer rescorer = new org.apache.lucene.search.QueryRescorer(query) {
            @Override
            protected float combine(float firstPassScore, boolean secondPassMatches, float secondPassScore) {
                return secondPassScore;
            }
        };

        // Rescore them:
        TopDocs rescored = rescorer.rescore(searcher, topNFirstPass, rescoreContext.getWindowSize());

        // Splice back to non-topN hits and resort all of them:
        return rescored;
    }

    @Override
    public Explanation explain(int topLevelDocId, IndexSearcher searcher, RescoreContext rescoreContext,
                               Explanation sourceExplanation) throws IOException {
        if (sourceExplanation == null) {
            return Explanation.noMatch("nothing matched");
        }
        ScriptRescorer.ScriptRescoreContext rescore = (ScriptRescorer.ScriptRescoreContext) rescoreContext;
        Explanation rescoreExplain = searcher.explain(rescore.query(), topLevelDocId);
        Explanation prim;
        if (rescoreExplain != null && rescoreExplain.isMatch()) {
            prim = Explanation.match(rescoreExplain.getValue().floatValue(),"detail of:",
                    rescore.needsScores ? Arrays.asList(sourceExplanation, rescoreExplain)
                                        : Arrays.asList(rescoreExplain));
        } else {
            prim = Explanation.noMatch("Rescore pass did not match", sourceExplanation);
        }
        return prim;
    }

    /**
     * Returns a new {@link TopDocs} with the topN from the incoming one, or the same TopDocs if the number of hits is already &lt;=
     * topN.
     */
    private TopDocs topN(TopDocs in, int topN) {
        if (in.scoreDocs.length < topN) {
            return in;
        }

        ScoreDoc[] subset = new ScoreDoc[topN];
        System.arraycopy(in.scoreDocs, 0, subset, 0, topN);

        return new TopDocs(in.totalHits, subset);
    }

    public static class ScriptRescoreContext extends RescoreContext {
        private ScriptRescorerBuilder.ScriptQuery query;
        private boolean needsScores;

        public ScriptRescoreContext(int windowSize) {
            super(windowSize, ScriptRescorer.INSTANCE);
        }

        public void setQuery(ScriptRescorerBuilder.ScriptQuery query) {
            this.query = query;
        }

        @Override
        public List<Query> getQueries() {
            return Collections.singletonList(query);
        }

        public ScriptRescorerBuilder.ScriptQuery query() {
            return query;
        }

        public void setNeedsScores(boolean needsScores) {
            this.needsScores = needsScores;
        }
    }
}
