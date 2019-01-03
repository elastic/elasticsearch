/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function;

import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.ExpressionId;
import org.elasticsearch.xpack.sql.expression.Expressions;
import org.elasticsearch.xpack.sql.expression.NamedExpression;
import org.elasticsearch.xpack.sql.expression.Nullability;
import org.elasticsearch.xpack.sql.tree.Source;
import org.elasticsearch.xpack.sql.util.StringUtils;

import java.util.List;
import java.util.StringJoiner;

/**
 * Any SQL expression with parentheses, like {@code MAX()}, or {@code ABS()}. A
 * function is always a {@code NamedExpression}.
 */
public abstract class Function extends NamedExpression {

    private final String functionName, name;

    protected Function(Source source, List<Expression> children) {
        this(source, children, null, false);
    }

    // TODO: Functions supporting distinct should add a dedicated constructor Location, List<Expression>, boolean
    protected Function(Source source, List<Expression> children, ExpressionId id, boolean synthetic) {
        // cannot detect name yet so override the name
        super(source, null, children, id, synthetic);
        functionName = StringUtils.camelCaseToUnderscore(getClass().getSimpleName());
        name = functionName() + functionArgs();
    }

    public final List<Expression> arguments() {
        return children();
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public Nullability nullable() {
        return Expressions.nullable(children());
    }

    @Override
    public String toString() {
        return name() + "#" + id();
    }

    public String functionName() {
        return functionName;
    }

    // TODO: ExpressionId might be converted into an Int which could make the String an int as well
    public String functionId() {
        return id().toString();
    }

    protected String functionArgs() {
        StringJoiner sj = new StringJoiner(",", "(", ")");
        for (Expression child : children()) {
            String val = child instanceof NamedExpression && child.resolved() ?  Expressions.name(child) : child.toString();
            sj.add(val);
        }
        return sj.toString();
    }

    public boolean functionEquals(Function f) {
        return f != null && getClass() == f.getClass() && arguments().equals(f.arguments());
    }
}
