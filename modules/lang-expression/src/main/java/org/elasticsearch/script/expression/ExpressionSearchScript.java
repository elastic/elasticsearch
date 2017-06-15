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
import org.apache.lucene.search.DoubleValues;
import org.apache.lucene.search.DoubleValuesSource;
import org.apache.lucene.search.Scorer;
import org.elasticsearch.script.GeneralScriptException;
import org.elasticsearch.script.SearchScript;

import java.io.IOException;

/**
 * A bridge to evaluate an {@link Expression} against {@link Bindings} in the context
 * of a {@link SearchScript}.
 */
class ExpressionSearchScript implements SearchScript.LeafFactory {

    final Expression exprScript;
    final SimpleBindings bindings;
    final DoubleValuesSource source;
    final ReplaceableConstDoubleValueSource specialValue; // _value
    final boolean needsScores;
    Scorer scorer;
    int docid;

    ExpressionSearchScript(Expression e, SimpleBindings b, ReplaceableConstDoubleValueSource v, boolean needsScores) {
        exprScript = e;
        bindings = b;
        source = exprScript.getDoubleValuesSource(bindings);
        specialValue = v;
        this.needsScores = needsScores;
    }

    @Override
    public boolean needs_score() {
        return needsScores;
    }


    @Override
    public SearchScript newInstance(final LeafReaderContext leaf) throws IOException {
        return new SearchScript(null, null, null) {
            // Fake the scorer until setScorer is called.
            DoubleValues values = source.getValues(leaf, new DoubleValues() {
                @Override
                public double doubleValue() throws IOException {
                    return getScore();
                }

                @Override
                public boolean advanceExact(int doc) throws IOException {
                    return true;
                }
            });

            @Override
            public Object run() { return Double.valueOf(runAsDouble()); }

            @Override
            public long runAsLong() { return (long)runAsDouble(); }

            @Override
            public double runAsDouble() {
                try {
                    return values.doubleValue();
                } catch (Exception exception) {
                    throw new GeneralScriptException("Error evaluating " + exprScript, exception);
                }
            }

            @Override
            public void setDocument(int d) {
                try {
                    values.advanceExact(d);
                } catch (IOException e) {
                    throw new IllegalStateException("Can't advance to doc using " + exprScript, e);
                }
            }

            @Override
            public void setNextAggregationValue(Object value) {
                // _value isn't used in script if specialValue == null
                if (specialValue != null) {
                    if (value instanceof Number) {
                        specialValue.setValue(((Number)value).doubleValue());
                    } else {
                        throw new GeneralScriptException("Cannot use expression with text variable using " + exprScript);
                    }
                }
            }

            @Override
            public void setNextVar(String name, Object value) {
                // other per-document variables aren't supported yet, even if they are numbers
                // but we shouldn't encourage this anyway.
            }
        };
    }

}
