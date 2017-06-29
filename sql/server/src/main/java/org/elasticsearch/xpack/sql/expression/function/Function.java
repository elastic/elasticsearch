/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function;

import java.util.List;
import java.util.StringJoiner;

import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.ExpressionId;
import org.elasticsearch.xpack.sql.expression.Expressions;
import org.elasticsearch.xpack.sql.expression.NamedExpression;
import org.elasticsearch.xpack.sql.tree.Location;
import org.elasticsearch.xpack.sql.util.StringUtils;

public abstract class Function extends NamedExpression {

    private final String functionName, name;

    protected Function(Location location, List<Expression> children) {
        this(location, children, null, false);
    }

    // TODO: Functions supporting distinct should add a dedicated constructor Location, List<Expression>, boolean
    protected Function(Location location, List<Expression> children, ExpressionId id, boolean synthetic) {
        // cannot detect name yet so override the name
        super(location, null, children, id, synthetic);
        functionName = StringUtils.camelCaseToUnderscore(getClass().getSimpleName());
        name = functionName() + functionArgs();
    }

    public List<Expression> arguments() {
        return children();
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public boolean foldable() {
        return false;
    }

    @Override
    public boolean nullable() {
        return false;
    }

    @Override
    public String toString() {
        return name() + "#" + id();
    }

    public String functionName() {
        return functionName;
    }

    protected String functionArgs() {
        StringJoiner sj = new StringJoiner(",", "(", ")");
        for (Expression child : children()) {
            String val = child instanceof NamedExpression && child.resolved() ?  Expressions.name(child) : child.toString();
            sj.add(val);
        }
        return sj.toString();
    }
}