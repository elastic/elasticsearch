/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.lucene.search.function;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.Scorable;
import org.elasticsearch.script.DocValuesDocReader;
import org.elasticsearch.script.ExplainableScoreScript;
import org.elasticsearch.script.ScoreScript;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.lookup.SearchLookup;

import java.io.IOException;
import java.util.Objects;

public class ScriptScoreFunction extends ScoreFunction {

    private static final ScoreScript.ExplanationHolder DUMMY_EXPLAIN_HOLDER = new ScoreScript.ExplanationHolder();

    static final class CannedScorer extends Scorable {
        protected int docid;
        protected float score;

        @Override
        public int docID() {
            return docid;
        }

        @Override
        public float score() {
            return score;
        }
    }

    private final Script sScript;

    private final ScoreScript.LeafFactory script;
    private final SearchLookup lookup;

    private final int shardId;
    private final String indexName;

    private final boolean explain;

    public ScriptScoreFunction(
        Script sScript,
        ScoreScript.LeafFactory script,
        SearchLookup lookup,
        String indexName,
        int shardId,
        boolean explain
    ) {
        super(CombineFunction.REPLACE);
        this.sScript = sScript;
        this.script = script;
        this.lookup = lookup;
        this.indexName = indexName;
        this.shardId = shardId;
        this.explain = explain;
    }

    @Override
    public LeafScoreFunction getLeafScoreFunction(LeafReaderContext ctx) throws IOException {
        final ScoreScript leafScript = script.newInstance(new DocValuesDocReader(lookup, ctx));
        final CannedScorer scorer = new CannedScorer();
        leafScript.setScorer(scorer);
        leafScript._setIndexName(indexName);
        leafScript._setShard(shardId);
        return new LeafScoreFunction() {
            private String customExplanation = null;

            @Override
            public double score(int docId, float subQueryScore) throws IOException {
                leafScript.setDocument(docId);
                scorer.docid = docId;
                scorer.score = subQueryScore;
                double result;
                if (explain) {
                    ScoreScript.ExplanationHolder explanation = new ScoreScript.ExplanationHolder();
                    result = leafScript.execute(explanation);
                    customExplanation = explanation.get(0.0f, null).getDescription();
                } else {
                    result = leafScript.execute(DUMMY_EXPLAIN_HOLDER);
                }

                if (result < 0f) {
                    throw new IllegalArgumentException("script score function must not produce negative scores, but got: [" + result + "]");
                }
                return result;
            }

            @Override
            public Explanation explainScore(int docId, Explanation subQueryScore) throws IOException {
                Explanation exp;
                if (leafScript instanceof ExplainableScoreScript) {
                    leafScript.setDocument(docId);
                    scorer.docid = docId;
                    scorer.score = subQueryScore.getValue().floatValue();
                    exp = ((ExplainableScoreScript) leafScript).explain(subQueryScore);
                } else {
                    double score = score(docId, subQueryScore.getValue().floatValue());
                    // info about params already included in sScript
                    Explanation scoreExp = Explanation.match(subQueryScore.getValue(), "_score: ", subQueryScore);
                    if (customExplanation != null) {
                        return Explanation.match((float) score, customExplanation, scoreExp);
                    } else {
                        String explanation = "script score function, computed with script:\"" + sScript + "\"";
                        return Explanation.match((float) score, explanation, scoreExp);
                    }
                }
                return exp;
            }
        };
    }

    @Override
    public boolean needsScores() {
        return script.needs_score();
    }

    @Override
    public String toString() {
        return "script" + sScript.toString();
    }

    @Override
    protected boolean doEquals(ScoreFunction other) {
        ScriptScoreFunction scriptScoreFunction = (ScriptScoreFunction) other;
        return Objects.equals(this.sScript, scriptScoreFunction.sScript);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(sScript);
    }
}
