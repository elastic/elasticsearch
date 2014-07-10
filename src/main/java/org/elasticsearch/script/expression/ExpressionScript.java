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
import org.apache.lucene.queries.function.valuesource.DoubleConstValueSource;
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

    Expression expression;
    XSimpleBindings bindings;
    AtomicReaderContext leaf;
    CannedScorer scorer;

    ExpressionScript(Expression e, XSimpleBindings b) {
        expression = e;
        bindings = b;
        scorer = new CannedScorer();
    }

    double evaluate() {
        try {
            ValueSource vs = expression.getValueSource(bindings);
            FunctionValues fv = vs.getValues(Collections.singletonMap("scorer", scorer), leaf);
            return fv.doubleVal(scorer.docid);
        } catch (IOException e) {
            throw new ExpressionScriptExecutionException("Failed to run expression", e);
        }
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
    public void setNextScore(float score) { scorer.score = score; }

    @Override
    public void setNextReader(AtomicReaderContext l) {
        leaf = l;
    }

    @Override
    public void setScorer(Scorer s) { /* noop: score isn't actually set for scoring... */ }

    @Override
    public void setNextSource(Map<String, Object> source) {
        // noop: expressions don't use source data
    }

    @Override
    public void setNextVar(String name, Object value) {
        // this assumes that the same variable will be set for every document evaluated, thus
        // the variable never needs to be removed from the bindings, but only overwritten
        if (value instanceof Double) {
            bindings.add(name, new DoubleConstValueSource(((Double)value).doubleValue()));
        } else if (value instanceof Long) {
            bindings.add(name, new DoubleConstValueSource(((Long)value).doubleValue()));
        } else {
            throw new ExpressionScriptExecutionException("Cannot use expression with text variable");
        }
    }


}
