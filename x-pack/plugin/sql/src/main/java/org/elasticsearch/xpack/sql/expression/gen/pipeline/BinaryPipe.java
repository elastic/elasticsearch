/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.gen.pipeline;

import org.elasticsearch.xpack.sql.execution.search.SqlSourceBuilder;
import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.tree.Source;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public abstract class BinaryPipe extends Pipe {

    private final Pipe left, right;

    public BinaryPipe(Source source, Expression expression, Pipe left, Pipe right) {
        super(source, expression, Arrays.asList(left, right));
        this.left = left;
        this.right = right;
    }

    @Override
    public final Pipe replaceChildren(List<Pipe> newChildren) {
        if (newChildren.size() != 2) {
            throw new IllegalArgumentException("expected [2] children but received [" + newChildren.size() + "]");
        }
        return replaceChildren(newChildren.get(0), newChildren.get(1));
    }

    public Pipe left() {
        return left;
    }

    public Pipe right() {
        return right;
    }

    @Override
    public boolean supportedByAggsOnlyQuery() {
        return left.supportedByAggsOnlyQuery() || right.supportedByAggsOnlyQuery();
    }

    @Override
    public final Pipe resolveAttributes(AttributeResolver resolver) {
        Pipe newLeft = left.resolveAttributes(resolver);
        Pipe newRight = right.resolveAttributes(resolver);
        if (newLeft == left && newRight == right) {
            return this;
        }
        return replaceChildren(newLeft, newRight);
    }

    /**
     * Build a copy of this object with new left and right children. Used by
     * {@link #resolveAttributes(AttributeResolver)}.
     */
    protected abstract BinaryPipe replaceChildren(Pipe left, Pipe right);

    @Override
    public boolean resolved() {
        return left().resolved() && right().resolved();
    }

    @Override
    public final void collectFields(SqlSourceBuilder sourceBuilder) {
        left.collectFields(sourceBuilder);
        right.collectFields(sourceBuilder);
    }

    @Override
    public int hashCode() {
        return Objects.hash(left(), right());
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        BinaryPipe other = (BinaryPipe) obj;
        return Objects.equals(left(), other.left())
                && Objects.equals(right(), other.right());
    }
}