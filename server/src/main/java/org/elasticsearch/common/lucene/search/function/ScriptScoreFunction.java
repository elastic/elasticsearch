/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.lucene.search.function;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.Scorable;
import org.elasticsearch.script.DocValuesDocReader;
import org.elasticsearch.script.ExplainableScoreScript;
import org.elasticsearch.script.ScoreScript;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptTermStats;
import org.elasticsearch.search.lookup.SearchLookup;

import java.io.IOException;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.IntSupplier;

public class ScriptScoreFunction extends ScoreFunction {

    static final class CannedScorer extends Scorable {
        protected float score;

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

    private BiFunction<LeafReaderContext, IntSupplier, ScriptTermStats> termStatsFactory;

    public ScriptScoreFunction(Script sScript, ScoreScript.LeafFactory script, SearchLookup lookup, String indexName, int shardId) {
        super(CombineFunction.REPLACE);
        this.sScript = sScript;
        this.script = script;
        this.lookup = lookup;
        this.indexName = indexName;
        this.shardId = shardId;
    }

    @Override
    public LeafScoreFunction getLeafScoreFunction(LeafReaderContext ctx) throws IOException {
        final ScoreScript leafScript = script.newInstance(new DocValuesDocReader(lookup, ctx));
        final CannedScorer scorer = new CannedScorer();
        leafScript.setScorer(scorer);
        leafScript._setIndexName(indexName);
        leafScript._setShard(shardId);

        if (script.needs_termStats()) {
            assert termStatsFactory != null;
            leafScript._setTermStats(termStatsFactory.apply(ctx, leafScript::docId));
        }

        return new LeafScoreFunction() {

            private double score(int docId, float subQueryScore, ScoreScript.ExplanationHolder holder) throws IOException {
                leafScript.setDocument(docId);
                scorer.score = subQueryScore;
                double result = leafScript.execute(holder);

                if (result < 0f) {
                    throw new IllegalArgumentException("script score function must not produce negative scores, but got: [" + result + "]");
                }
                return result;
            }

            @Override
            public double score(int docId, float subQueryScore) throws IOException {
                return score(docId, subQueryScore, null);
            }

            @Override
            public Explanation explainScore(int docId, Explanation subQueryScore) throws IOException {
                Explanation exp;
                if (leafScript instanceof ExplainableScoreScript) {
                    leafScript.setDocument(docId);
                    scorer.score = subQueryScore.getValue().floatValue();
                    exp = ((ExplainableScoreScript) leafScript).explain(subQueryScore);
                } else {
                    ScoreScript.ExplanationHolder holder = new ScoreScript.ExplanationHolder();
                    double score = score(docId, subQueryScore.getValue().floatValue(), holder);
                    // info about params already included in sScript
                    Explanation scoreExp = Explanation.match(subQueryScore.getValue(), "_score: ", subQueryScore);
                    Explanation customExplanation = holder.get(score, null);
                    if (customExplanation != null) {
                        return Explanation.match((float) score, customExplanation.getDescription(), scoreExp);
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

    public boolean needsTermStats() {
        return script.needs_termStats();
    }

    public void setTermStatsFactory(BiFunction<LeafReaderContext, IntSupplier, ScriptTermStats> termStatsFactory) {
        this.termStatsFactory = termStatsFactory;
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
