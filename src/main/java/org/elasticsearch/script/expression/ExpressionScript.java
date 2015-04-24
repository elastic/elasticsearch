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
import org.apache.lucene.expressions.SimpleBindings;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.search.Scorer;
import org.elasticsearch.ElasticsearchIllegalStateException;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.script.LeafSearchScript;
import org.elasticsearch.script.SearchScript;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

/**
 * A bridge to evaluate an {@link Expression} against {@link Bindings} in the context
 * of a {@link SearchScript}.
 */
class ExpressionScript implements SearchScript {

    final Expression expression;
    final SimpleBindings bindings;
    final ValueSource source;
    final ReplaceableConstValueSource specialValue; // _value
    Scorer scorer;
    int docid;

    ExpressionScript(Expression e, SimpleBindings b, ReplaceableConstValueSource v) {
        expression = e;
        bindings = b;
        source = expression.getValueSource(bindings);
        specialValue = v;
    }

    @Override
    public LeafSearchScript getLeafSearchScript(final LeafReaderContext leaf) throws IOException {
        return new LeafSearchScript() {

            FunctionValues values = source.getValues(Collections.singletonMap("scorer", Lucene.illegalScorer("Scores are not available in the current context")), leaf);

            double evaluate() {
                return values.doubleVal(docid);
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
            public void setDocument(int d) {
                docid = d;
            }

            @Override
            public void setScorer(Scorer s) {
                scorer = s;
                try {
                    // We have a new binding for the scorer so we need to reset the values
                    values = source.getValues(Collections.singletonMap("scorer", scorer), leaf);
                } catch (IOException e) {
                    throw new ElasticsearchIllegalStateException("Can't get values", e);
                }
            }

            @Override
            public void setSource(Map<String, Object> source) {
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
        };
    }

}
