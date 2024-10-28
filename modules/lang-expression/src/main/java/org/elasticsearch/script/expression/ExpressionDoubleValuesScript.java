/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.script.expression;

import org.apache.lucene.expressions.Bindings;
import org.apache.lucene.expressions.Expression;
import org.apache.lucene.search.DoubleValues;
import org.apache.lucene.search.DoubleValuesSource;
import org.apache.lucene.search.Rescorer;
import org.apache.lucene.search.SortField;
import org.elasticsearch.script.DoubleValuesScript;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.function.Function;

/**
 * A factory for a custom compiled {@link Expression} scripts
 * <p>
 * Instead of an execution result, we return a wrapper to an {@link Expression} object, which
 * can be used for all supported double values operations.
 */
public class ExpressionDoubleValuesScript implements DoubleValuesScript.Factory {
    private final Expression exprScript;

    ExpressionDoubleValuesScript(Expression e) {
        this.exprScript = e;
    }

    @Override
    public DoubleValuesScript newInstance() {
        return new DoubleValuesScript() {
            @Override
            public double execute() {
                try {
                    return exprScript.evaluate(new DoubleValues[0]);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }

            @Override
            public double evaluate(DoubleValues[] functionValues) {
                try {
                    return exprScript.evaluate(functionValues);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
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
