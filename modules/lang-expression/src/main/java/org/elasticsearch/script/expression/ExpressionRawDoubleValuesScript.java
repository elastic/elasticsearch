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
import org.apache.lucene.search.DoubleValues;
import org.apache.lucene.search.DoubleValuesSource;
import org.apache.lucene.search.Rescorer;
import org.apache.lucene.search.SortField;
import org.elasticsearch.script.RawDoubleValuesScript;

import java.util.function.Function;

/**
 * A factory for raw compiled {@link Expression} scripts
 * <p>
 * Instead of an execution result, we return the compiled {@link Expression} object, which
 * can later be used for other execution engines based on lucene expressions.
 */
public class ExpressionRawDoubleValuesScript implements RawDoubleValuesScript.Factory {
    private final Expression exprScript;

    ExpressionRawDoubleValuesScript(Expression e) {
        this.exprScript = e;
    }

    @Override
    public RawDoubleValuesScript newInstance() {
        return new RawDoubleValuesScript() {
            @Override
            public double execute() {
                return exprScript.evaluate(new DoubleValues[0]);
            }

            @Override
            public double evaluate(DoubleValues[] functionValues) {
                return exprScript.evaluate(functionValues);
            }

            @Override
            public DoubleValuesSource getDoubleValuesSource(Function<String, DoubleValuesSource> sourceProvider) {
                return exprScript.getDoubleValuesSource(new Bindings() {
                    @Override
                    public DoubleValuesSource getDoubleValuesSource(String name) {
                        return sourceProvider.apply(name);
                    }
                });
            }

            @Override
            public SortField getSortField(Function<String, DoubleValuesSource> sourceProvider, boolean reverse) {
                return exprScript.getSortField(new Bindings() {
                    @Override
                    public DoubleValuesSource getDoubleValuesSource(String name) {
                        return sourceProvider.apply(name);
                    }
                }, reverse);
            }

            @Override
            public Rescorer getRescorer(Function<String, DoubleValuesSource> sourceProvider) {
                return exprScript.getRescorer(new Bindings() {
                    @Override
                    public DoubleValuesSource getDoubleValuesSource(String name) {
                        return sourceProvider.apply(name);
                    }
                });
            }

            @Override
            public String sourceText() {
                return exprScript.sourceText;
            }

            @Override
            public String[] variables() {
                return exprScript.variables;
            }
        };
    }
}
