/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.painless.symbol;

import org.elasticsearch.queryableexpression.QueryableExpression;

import java.util.Stack;
import java.util.function.BiFunction;

/**
 * Tracks information for building the QueryableExpression of a script.
 */
public class QueryableExpressionScope {
    private Stack<QueryableExpression> expressionStack;

    public QueryableExpressionScope() {
        this.expressionStack = new Stack<>();
    }

    public void push(QueryableExpression expression) {
        this.expressionStack.push(expression);
    }

    public void consume(BiFunction<QueryableExpression, QueryableExpression, QueryableExpression> fn) {
        if (expressionStack.size() >= 2) {
            push(fn.apply(expressionStack.pop(), expressionStack.pop()));
        } else {
            unqueryable();
        }
    }

    public void unqueryable() {
        this.expressionStack = new Stack<>();
        this.expressionStack.push(QueryableExpression.UNQUERYABLE);
    }

    public QueryableExpression result() {
        if (this.expressionStack.size() == 1) {
            return this.expressionStack.pop();
        } else {
            return QueryableExpression.UNQUERYABLE;
        }
    }
}
