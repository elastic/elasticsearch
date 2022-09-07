/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script.expression;

import org.apache.lucene.expressions.Expression;
import org.elasticsearch.script.BinaryScript;
import org.elasticsearch.script.GeneralScriptException;

import java.io.IOException;

public class ExpressionBinaryScript implements BinaryScript.LeafFactory<Expression> {
    private final Expression exprScript;

    ExpressionBinaryScript(Expression e) {
        this.exprScript = e;
    }

    @Override
    public BinaryScript<Expression> newInstance() throws IOException {
        return new BinaryScript<>() {
            @Override
            public Expression execute() {
                try {
                    return exprScript;
                } catch (Exception exception) {
                    throw new GeneralScriptException("Error evaluating " + exprScript, exception);
                }
            }
        };
    }
}
