/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.expression;

import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.NameId;
import org.elasticsearch.xpack.ql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.ql.tree.Source;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

public abstract class SubQueryExpression extends Expression {

    private final LogicalPlan query;
    private final NameId id;

    public SubQueryExpression(Source source, LogicalPlan query) {
        this(source, query, null);
    }

    public SubQueryExpression(Source source, LogicalPlan query, NameId id) {
        super(source, Collections.emptyList());
        this.query = query;
        this.id = id == null ? new NameId() : id;
    }

    @Override
    public final Expression replaceChildren(List<Expression> newChildren) {
        throw new UnsupportedOperationException("this type of node doesn't have any children to replace");
    }

    public LogicalPlan query() {
        return query;
    }

    public NameId id() {
        return id;
    }

    @Override
    public boolean resolved() {
        return false;
    }

    public SubQueryExpression withQuery(LogicalPlan newQuery) {
        return (Objects.equals(query, newQuery) ? this : clone(newQuery));
    }

    protected abstract SubQueryExpression clone(LogicalPlan newQuery);

    @Override
    public int hashCode() {
        return Objects.hash(query());
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        SubQueryExpression other = (SubQueryExpression) obj;
        return Objects.equals(query(), other.query());
    }
}
