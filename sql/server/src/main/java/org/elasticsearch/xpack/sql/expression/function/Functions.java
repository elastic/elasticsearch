/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function;

import org.elasticsearch.xpack.sql.expression.Alias;
import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.NamedExpression;
import org.elasticsearch.xpack.sql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.sql.expression.function.scalar.BinaryScalarFunction;
import org.elasticsearch.xpack.sql.expression.function.scalar.UnaryScalarFunction;

public abstract class Functions {

    public static boolean isAggregateFunction(Expression e) {
        return e instanceof AggregateFunction;
    }

    public static boolean isUnaryScalarFunction(Expression e) {
        if (e instanceof BinaryScalarFunction) {
            throw new UnsupportedOperationException("not handled currently");
        }
        return e instanceof UnaryScalarFunction;
    }

    public static AggregateFunction extractAggregate(NamedExpression ne) {
        Expression e = ne;
        while (e != null) {
            if (e instanceof Alias) {
                e = ((Alias) ne).child();
            }
            else if (e instanceof UnaryScalarFunction) {
                e = ((UnaryScalarFunction) e).field();
            }
            else if (e instanceof BinaryScalarFunction) {
                throw new UnsupportedOperationException();
            }
            else if (e instanceof AggregateFunction) {
                return (AggregateFunction) e;
            }
            else {
                e = null;
            }
        }
        return null;
    }
}