/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function;

import java.util.ArrayList;
import java.util.List;

import org.elasticsearch.xpack.sql.expression.Alias;
import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.NamedExpression;
import org.elasticsearch.xpack.sql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.sql.expression.function.scalar.ColumnsProcessor;
import org.elasticsearch.xpack.sql.expression.function.scalar.ScalarFunction;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

public abstract class Functions {

    public static boolean isAggregateFunction(Expression e) {
        return e instanceof AggregateFunction;
    }

    public static boolean isScalarFunction(Expression e) {
        return e instanceof ScalarFunction;
    }

    public static AggregateFunction extractAggregate(NamedExpression ne) {
        Expression e = ne;
        while (e != null) {
            if (e instanceof Alias) {
                e = ((Alias) ne).child();
            }
            else if (e instanceof ScalarFunction) {
                e = ((ScalarFunction) e).argument();
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

    public static List<Expression> unwrapScalarFunctionWithTail(Expression e) {
        if (!(e instanceof ScalarFunction)) {
            return emptyList();
        }
        List<Expression> exps = new ArrayList<>();
        while (isScalarFunction(e)) {
            ScalarFunction scalar = (ScalarFunction) e;
            exps.add(scalar);
            e = scalar.argument();
        }
        exps.add(e);
        return exps;
    }

    public static List<ScalarFunction> unwrapScalarProcessor(Expression e) {
        if (!(e instanceof ScalarFunction)) {
            return emptyList();
        }

        // common-case (single function wrapper)
        if (e instanceof ScalarFunction && !(((ScalarFunction) e).argument() instanceof ScalarFunction)) {
            return singletonList((ScalarFunction) e);
        }

        List<ScalarFunction> exps = new ArrayList<>();
        while (e instanceof ScalarFunction) {
            ScalarFunction scalar = (ScalarFunction) e;
            exps.add(scalar);
            e = scalar.argument();
        }
        return exps;
    }

    public static ColumnsProcessor chainProcessors(List<Expression> unwrappedScalar) {
        ColumnsProcessor proc = null;
        for (Expression e : unwrappedScalar) {
            if (e instanceof ScalarFunction) {
                ScalarFunction sf = (ScalarFunction) e;
                // A(B(C)) is applied backwards first C then B then A, the last function first
                proc = proc != null ? sf.asProcessor().andThen(proc) : sf.asProcessor();
            }
            else {
                return proc;
            }
        }
        return proc;
    }
}