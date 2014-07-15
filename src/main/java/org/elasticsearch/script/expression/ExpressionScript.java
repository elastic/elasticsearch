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

package org.elasticsearch.script.expression;

import org.apache.lucene.expressions.Bindings;
import org.apache.lucene.expressions.Expression;
import org.apache.lucene.expressions.XSimpleBindings;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.search.Scorer;
import org.elasticsearch.script.SearchScript;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

/**
 * A bridge to evaluate an {@link Expression} against {@link Bindings} in the context
 * of a {@link SearchScript}.
 */
class ExpressionScript implements SearchScript {

    /** Fake scorer for a single document */
    static class CannedScorer extends Scorer {
        protected int docid;
        protected float score;

        public CannedScorer() {
            super(null);
        }

        @Override
        public int docID() {
            return docid;
        }

        @Override
        public float score() throws IOException {
            return score;
        }

        @Override
        public int freq() throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public int nextDoc() throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public int advance(int target) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public long cost() {
            return 1;
        }
    }

    final Expression expression;
    final XSimpleBindings bindings;
    final CannedScorer scorer;
    final ValueSource source;
    final Map<String, CannedScorer> context;
    final ReplaceableConstValueSource specialValue; // _value
    FunctionValues values;

    ExpressionScript(Expression e, XSimpleBindings b, ReplaceableConstValueSource v) {
        expression = e;
        bindings = b;
        scorer = new CannedScorer();
        context = Collections.singletonMap("scorer", scorer);
        source = expression.getValueSource(bindings);
        specialValue = v;
    }

    double evaluate() {
        return values.doubleVal(scorer.docid);
    }

    @Override
    public Object run() { return new Double(evaluate()); }

    @Override
    public float runAsFloat() { return (float)evaluate();}

    @Override
    public long runAsLong() { return (long)evaluate(); }

    @Override
    public double runAsDouble() { return evaluate(); }

    @Override
    public Object unwrap(Object value) { return value; }

    @Override
    public void setNextDocId(int d) {
        scorer.docid = d;
    }

    @Override
    public void setNextScore(float score) {
        // TODO: fix this API to remove setNextScore and just use a Scorer
        // Expressions know if they use the score or not, and should be able to pull from the scorer only
        // if they need it. Right now, score can only be used within a ScriptScoreFunction.  But there shouldn't
        // be any reason a script values or aggregation can't use the score.  It is also possible
        // these layers are preventing inlining of scoring into expressions.
        scorer.score = score;
    }

    @Override
    public void setNextReader(AtomicReaderContext leaf) {
        try {
            values = source.getValues(context, leaf);
        } catch (IOException e) {
            throw new ExpressionScriptExecutionException("Expression failed to bind for segment", e);
        }
    }

    @Override
    public void setScorer(Scorer s) {
         // noop: The scorer isn't actually ever set.  Instead setNextScore is called.
         // NOTE: This seems broken.  Why can't we just use the scorer and get rid of setNextScore?
    }

    @Override
    public void setNextSource(Map<String, Object> source) {
        // noop: expressions don't use source data
    }

    @Override
    public void setNextVar(String name, Object value) {
        assert(specialValue != null);
        // this should only be used for the special "_value" variable used in aggregations
        assert(name.equals("_value"));

        if (value instanceof Number) {
            specialValue.setValue(((Number)value).doubleValue());
        } else {
            throw new ExpressionScriptExecutionException("Cannot use expression with text variable");
        }
    }


}
