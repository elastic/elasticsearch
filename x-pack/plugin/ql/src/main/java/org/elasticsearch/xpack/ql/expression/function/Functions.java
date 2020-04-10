/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ql.expression.function;

import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.ql.expression.function.grouping.GroupingFunction;

public abstract class Functions {

    public static boolean isAggregate(Expression e) {
        return e instanceof AggregateFunction;
    }

    public static boolean isGrouping(Expression e) {
        return e instanceof GroupingFunction;
    }
}