/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.common.lucene.search.function;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.Explanation;
import org.elasticsearch.script.ExplainableSearchScript;
import org.elasticsearch.script.SearchScript;

import java.util.Map;

public class ScriptScoreFunction extends ScoreFunction {

    private final String sScript;

    private final Map<String, Object> params;

    private final SearchScript script;
    

    public ScriptScoreFunction(String sScript, Map<String, Object> params, SearchScript script) {
        super(CombineFunction.REPLACE);
        this.sScript = sScript;
        this.params = params;
        this.script = script;
    }

    @Override
    public void setNextReader(AtomicReaderContext ctx) {
        script.setNextReader(ctx);
    }

    @Override
    public double score(int docId, float subQueryScore) {
        script.setNextDocId(docId);
        script.setNextScore(subQueryScore);
        return script.runAsDouble();
    }

    @Override
    public Explanation explainScore(int docId, Explanation subQueryExpl) {
        Explanation exp;
        if (script instanceof ExplainableSearchScript) {
            script.setNextDocId(docId);
            script.setNextScore(subQueryExpl.getValue());
            exp = ((ExplainableSearchScript) script).explain(subQueryExpl);
        } else {
            double score = score(docId, subQueryExpl.getValue());
            exp = new Explanation(CombineFunction.toFloat(score), "script score function: composed of:");
            exp.addDetail(subQueryExpl);
        }
        return exp;
    }

    @Override
    public String toString() {
        return "script[" + sScript + "], params [" + params + "]";
    }

}