/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script.expression;

import org.apache.lucene.expressions.Bindings;
import org.apache.lucene.expressions.Expression;
import org.apache.lucene.expressions.SimpleBindings;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DoubleValues;
import org.apache.lucene.search.DoubleValuesSource;
import org.elasticsearch.script.DocReader;
import org.elasticsearch.script.GeneralScriptException;
import org.elasticsearch.script.LeafReaderContextSupplier;
import org.elasticsearch.script.ScoreScript;

import java.io.IOException;

/**
 * A bridge to evaluate an {@link Expression} against {@link Bindings} in the context
 * of a {@link ScoreScript}.
 */
class ExpressionScoreScript implements ScoreScript.LeafFactory {

    private final Expression exprScript;
    private final DoubleValuesSource source;
    private final boolean needsScores;

    ExpressionScoreScript(Expression e, SimpleBindings b, boolean needsScores) {
        this.exprScript = e;
        this.source = exprScript.getDoubleValuesSource(b);
        this.needsScores = needsScores;
    }

    @Override
    public boolean needs_score() {
        return needsScores;
    }

    @Override
    public ScoreScript newInstance(final DocReader reader) throws IOException {
        // Use DocReader to get the leaf context while transitioning to DocReader for Painless.  DocReader for expressions should follow.
        if (reader instanceof LeafReaderContextSupplier == false) {
            throw new IllegalStateException(
                "Expected LeafReaderContextSupplier when creating expression ExpressionScoreScript instead of [" + reader + "]"
            );
        }
        final LeafReaderContext leaf = ((LeafReaderContextSupplier) reader).getLeafReaderContext();

        return new ScoreScript(null, null, null) {
            // Fake the scorer until setScorer is called.
            DoubleValues values = source.getValues(leaf, new DoubleValues() {
                @Override
                public double doubleValue() throws IOException {
                    return get_score();
                }

                @Override
                public boolean advanceExact(int doc) throws IOException {
                    return true;
                }
            });

            @Override
            public double execute(ExplanationHolder explanation) {
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
