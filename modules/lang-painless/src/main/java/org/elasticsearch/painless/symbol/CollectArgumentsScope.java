/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.painless.symbol;

import org.elasticsearch.painless.spi.annotation.CollectArgumentAnnotation;
import org.elasticsearch.queryableexpression.QueryableExpressionBuilder;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Collection of arguments for methods annotated with {@link CollectArgumentAnnotation}.
 */
public class CollectArgumentsScope {

    private final Map<String, QueryableExpressionBuilder> collectedArguments = new HashMap<>();
    private final Deque<QueryableExpressionBuilder> expressionStack;

    private String currentTarget;

    private final Set<String> assignedVariables;
    private final Map<String, String> variableNameToFieldValue;

    public CollectArgumentsScope() {
        this.expressionStack = new ArrayDeque<>();

        this.assignedVariables = new HashSet<>();
        this.variableNameToFieldValue = new HashMap<>();
    }

    public void startCollecting(String target) {
        if (isCollecting()) {
            throw new IllegalArgumentException("can't collect arguments for two methods at once");
        }
        currentTarget = target;
    }

    public void stopCollecting(String target) {
        if (false == Objects.equals(currentTarget, target)) {
            // Paranoia
            throw new IllegalStateException(
                "attempted to stop collecting arguments for [" + target + "] while collecting arguments for [" + currentTarget + "]"
            );
        }
        QueryableExpressionBuilder result = expressionStack.size() == 1 ? expressionStack.pop() : QueryableExpressionBuilder.UNQUERYABLE;
        expressionStack.clear();
        collectedArguments.compute(target, (k, v) -> v == null ? result : QueryableExpressionBuilder.UNQUERYABLE);
        currentTarget = null;
    }

    private boolean isCollecting() {
        return currentTarget != null;
    }

    public boolean push(QueryableExpressionBuilder expression) {
        if (isCollecting()) {
            this.expressionStack.push(expression);
            return true;
        } else {
            return false;
        }
    }

    public void consume(Function<QueryableExpressionBuilder, QueryableExpressionBuilder> fn) {
        if (isCollecting()) {
            if (expressionStack.size() >= 1) {
                push(fn.apply(expressionStack.pop()));
            } else {
                unqueryable();
            }
        }
    }

    public void consume(BiFunction<QueryableExpressionBuilder, QueryableExpressionBuilder, QueryableExpressionBuilder> fn) {
        if (isCollecting()) {
            if (expressionStack.size() >= 2) {
                push(fn.apply(expressionStack.pop(), expressionStack.pop()));
            } else {
                unqueryable();
            }
        }
    }

    public boolean unqueryable() {
        if (isCollecting()) {
            this.expressionStack.clear();
            this.expressionStack.push(QueryableExpressionBuilder.UNQUERYABLE);
            return true;
        } else {
            return false;
        }
    }

    public Map<String, QueryableExpressionBuilder> collectedArguments() {
        return collectedArguments;
    }

    public void setVariableAssigned(String name) {
        assignedVariables.add(name);
    }

    public boolean isVariableAssigned(String name) {
        return assignedVariables.contains(name);
    }

    public void putVariableField(String variableName, String fieldName) {
        variableNameToFieldValue.put(variableName, fieldName);
    }

    public void removeVariableField(String variableName) {
        variableNameToFieldValue.remove(variableName);
    }

    public String getVariableField(String variableName) {
        return variableNameToFieldValue.get(variableName);
    }
}
