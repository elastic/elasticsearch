/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.core.expression.function;

import org.elasticsearch.xpack.esql.capabilities.NonFiniteSupport;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.tree.NodeStringMapper;
import org.elasticsearch.xpack.esql.core.tree.Source;

import java.util.List;
import java.util.Locale;
import java.util.Objects;

/**
 * Any ESQL function; generally this is translated into a computation to be evaluated on arguments, including scalar functions (e.g. in
 * {@code EVAL}) or aggregation functions ({@code STATS}).
 */
public abstract class Function extends Expression {
    // TODO: Functions supporting distinct should add a dedicated constructor Location, List<Expression>, boolean
    protected Function(Source source, List<Expression> children) {
        super(source, children);
    }

    public final List<Expression> arguments() {
        return children();
    }

    public String functionName() {
        return getClass().getSimpleName().toUpperCase(Locale.ROOT);
    }

    @Override
    public Nullability nullable() {
        return Expressions.nullable(children());
    }

    /**
     * NB: the hash code is currently used for key generation, so the class is included as a variation to avoid clashes
     * between different functions over the same arguments.
     */
    @Override
    public int hashCode() {
        return this instanceof NonFiniteSupport nonFinite
            ? Objects.hash(getClass(), children(), nonFinite.allowNonFinite())
            : Objects.hash(getClass(), children());
    }

    /**
     * Two functions are equal when they are of the same type and have equal children.
     * <p>
     *     A {@link NonFiniteSupport} function additionally distinguishes its non-finite-preserving form from its strict
     *     one, because the two evaluate different math. Expression tree transformations decide whether anything changed
     *     by comparing the old node with the new one, so substituting one form for the other would be silently
     *     discarded if they compared equal.
     * </p>
     */
    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        Function other = (Function) obj;
        // The class check above guarantees that either both sides support non-finite results, or neither does.
        if (this instanceof NonFiniteSupport nonFinite && nonFinite.allowNonFinite() != ((NonFiniteSupport) other).allowNonFinite()) {
            return false;
        }
        return Objects.equals(children(), other.children());
    }

    @Override
    public void nodeString(StringBuilder sb, NodeStringFormat format, NodeStringMapper mapper) {
        sb.append(functionName()).append("(");
        List<Expression> args = arguments();
        for (int i = 0; i < args.size(); i++) {
            if (i > 0) {
                sb.append(",");
            }
            args.get(i).nodeString(sb, format, mapper);
        }
        sb.append(")");
    }
}
