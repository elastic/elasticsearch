/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.core.expression.function;

import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.tree.Source;

import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.StringJoiner;

/**
 * Any SQL expression with parentheses, like {@code MAX()}, or {@code ABS()}. A
 * function is always a {@code NamedExpression}.
 */
public abstract class Function extends Expression {

    private final String functionName = getClass().getSimpleName().toUpperCase(Locale.ROOT);

    // TODO: Functions supporting distinct should add a dedicated constructor Location, List<Expression>, boolean
    protected Function(Source source, List<Expression> children) {
        super(source, children);
    }

    public final List<Expression> arguments() {
        return children();
    }

    public String functionName() {
        return functionName;
    }

    @Override
    public Nullability nullable() {
        return Expressions.nullable(children());
    }

    /** Return true if this function can be executed under the provided {@link XPackLicenseState}, otherwise false.*/
    public boolean checkLicense(XPackLicenseState state) {
        return true;
    }

    @Override
    public int hashCode() {
        return Objects.hash(getClass(), children());
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        Function other = (Function) obj;
        return Objects.equals(children(), other.children());
    }

    @Override
    public String nodeString() {
        StringJoiner sj = new StringJoiner(",", functionName() + "(", ")");
        for (Expression ex : arguments()) {
            sj.add(ex.nodeString());
        }
        return sj.toString();
    }
}
