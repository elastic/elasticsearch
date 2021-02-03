/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script.expression;

import java.io.IOException;
import org.apache.lucene.expressions.Bindings;
import org.apache.lucene.expressions.Expression;
import org.apache.lucene.expressions.SimpleBindings;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DoubleValues;
import org.apache.lucene.search.DoubleValuesSource;
import org.elasticsearch.script.AggregationScript;
import org.elasticsearch.script.GeneralScriptException;

/**
 * A bridge to evaluate an {@link Expression} against {@link Bindings} in the context
 * of a {@link AggregationScript}.
 */
class ExpressionAggregationScript implements AggregationScript.LeafFactory {

    final Expression exprScript;
    final SimpleBindings bindings;
    final DoubleValuesSource source;
    final boolean needsScore;
    final ReplaceableConstDoubleValueSource specialValue; // _value

    ExpressionAggregationScript(Expression e, SimpleBindings b, boolean n, ReplaceableConstDoubleValueSource v) {
        exprScript = e;
        bindings = b;
        source = exprScript.getDoubleValuesSource(bindings);
        needsScore = n;
        specialValue = v;
    }

    @Override
    public boolean needs_score() {
        return needsScore;
    }

    @Override
    public AggregationScript newInstance(final LeafReaderContext leaf) throws IOException {
        return new AggregationScript() {
            // Fake the scorer until setScorer is called.
            DoubleValues values = source.getValues(leaf, new DoubleValues() {
                @Override
                public double doubleValue() throws IOException {
                    return get_score().doubleValue();
                }

                @Override
                public boolean advanceExact(int doc) throws IOException {
                    return true;
                }
            });

            @Override
            public Object execute() {
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
        };
    }
}
