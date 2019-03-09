/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function;

import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.sql.expression.function.grouping.GroupingFunction;
import org.elasticsearch.xpack.sql.plan.QueryPlan;

import java.util.LinkedHashMap;
import java.util.Map;

public abstract class Functions {

    public static boolean isAggregate(Expression e) {
        return e instanceof AggregateFunction;
    }

    public static boolean isGrouping(Expression e) {
        return e instanceof GroupingFunction;
    }

    public static Map<String, Function> collectFunctions(QueryPlan<?> plan) {
        Map<String, Function> resolvedFunctions = new LinkedHashMap<>();
        plan.forEachExpressionsDown(e -> {
            if (e.resolved() && e instanceof Function) {
                Function f = (Function) e;
                resolvedFunctions.put(f.functionId(), f);
            }
        });
        return resolvedFunctions;
    }
}