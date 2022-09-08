/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script.expression;

import org.apache.lucene.expressions.Expression;
import org.elasticsearch.script.RawScript;

/**
 * A factory for raw compiled {@link Expression} scripts
 * <p>
 * Instead of an execution result, we return the compiled {@link Expression} object, which
 * can later be used for other execution engines based on lucene expressions.
 */
public class ExpressionRawScript implements RawScript.Factory<Expression> {
    private final Expression exprScript;

    ExpressionRawScript(Expression e) {
        this.exprScript = e;
    }

    @Override
    public RawScript<Expression> newInstance() {
        return new RawScript<>() {
            @Override
            public Expression execute() {
                return exprScript;
            }
        };
    }
}
