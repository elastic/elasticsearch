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
import org.elasticsearch.script.GeneralScriptException;
import org.elasticsearch.script.TermsSetQueryScript;

/**
 * A bridge to evaluate an {@link Expression} against {@link Bindings} in the context
 * of a {@link TermsSetQueryScript}.
 */
class ExpressionTermSetQueryScript implements TermsSetQueryScript.LeafFactory {

    final Expression exprScript;
    final SimpleBindings bindings;
    final DoubleValuesSource source;

    ExpressionTermSetQueryScript(Expression e, SimpleBindings b) {
        exprScript = e;
        bindings = b;
        source = exprScript.getDoubleValuesSource(bindings);
    }

    @Override
    public TermsSetQueryScript newInstance(final LeafReaderContext leaf) throws IOException {
        return new TermsSetQueryScript() {
            // Fake the scorer until setScorer is called.
            DoubleValues values = source.getValues(leaf, null);

            @Override
            public Number execute() {
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
        };
    }

}
