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
import org.elasticsearch.script.ExplainableScoreScript;
import org.elasticsearch.script.ScoreScript;
import org.elasticsearch.script.Script;

import java.io.IOException;
import java.util.Objects;

public class ScriptScoreFunction extends ScoreFunction {

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

    private final int shardId;
    private final String indexName;

    public ScriptScoreFunction(Script sScript, ScoreScript.LeafFactory script, String indexName, int shardId) {
        super(CombineFunction.REPLACE);
        this.sScript = sScript;
        this.script = script;
        this.indexName = indexName;
        this.shardId = shardId;
    }

    @Override
    public LeafScoreFunction getLeafScoreFunction(LeafReaderContext ctx) throws IOException {
        final ScoreScript leafScript = script.newInstance(ctx);
        final CannedScorer scorer = new CannedScorer();
        leafScript.setScorer(scorer);
        leafScript._setIndexName(indexName);
        leafScript._setShard(shardId);
        return new LeafScoreFunction() {
            @Override
            public double score(int docId, float subQueryScore) throws IOException {
                leafScript.setDocument(docId);
                scorer.docid = docId;
                scorer.score = subQueryScore;
                double result = leafScript.execute(null);
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
                    String explanation = "script score function, computed with script:\"" + sScript + "\"";
                    Explanation scoreExp = Explanation.match(
                        subQueryScore.getValue(), "_score: ",
                        subQueryScore);
                    return Explanation.match(
                        (float) score, explanation,
                        scoreExp);
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
