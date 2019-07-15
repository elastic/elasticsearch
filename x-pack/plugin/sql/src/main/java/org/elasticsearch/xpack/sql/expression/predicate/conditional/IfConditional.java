/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.predicate.conditional;

import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.Nullability;
import org.elasticsearch.xpack.sql.tree.NodeInfo;
import org.elasticsearch.xpack.sql.tree.Source;
import org.elasticsearch.xpack.sql.type.DataType;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * Helper expression (cannot be created directly from a query) to model a
 * {@code WHEN <condition> ELSE <result>} clause of {@link Case} expression
 */
public class IfConditional extends Expression {

    private final Expression condition;
    private final Expression result;

    public IfConditional(Source source, Expression condition, Expression result) {
        super(source, Arrays.asList(condition, result));
        this.condition = condition;
        this.result = result;
    }

    public Expression condition() {
        return condition;
    }

    public Expression result() {
        return result;
    }

    @Override
    public Nullability nullable() {
        return Nullability.UNKNOWN;
    }

    @Override
    public DataType dataType() {
        return result.dataType();
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        if (newChildren.size() < 2) {
            throw new IllegalArgumentException("expected at least [2] children but received [" + newChildren.size() + "]");
        }
        return new IfConditional(source(), newChildren.get(0), newChildren.get(1));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, IfConditional::new, condition, result);
    }

    @Override
    protected TypeResolution resolveType() {
        // Verification takes place is Case function to be
        // able to generate more accurate error messages
        return TypeResolution.TYPE_RESOLVED;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }

        IfConditional that = (IfConditional) o;
        return Objects.equals(condition, that.condition) &&
            Objects.equals(result, that.result);
    }

    @Override
    public int hashCode() {
        return Objects.hash(condition, result);
    }
}
