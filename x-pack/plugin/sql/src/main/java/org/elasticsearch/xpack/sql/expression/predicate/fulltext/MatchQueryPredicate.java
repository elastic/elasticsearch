/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.predicate.fulltext;

import java.util.Objects;

import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.tree.Source;
import org.elasticsearch.xpack.sql.tree.NodeInfo;

import static java.util.Collections.singletonList;

import java.util.List;

public class MatchQueryPredicate extends FullTextPredicate {

    private final Expression field;

    public MatchQueryPredicate(Source source, Expression field, String query, String options) {
        super(source, query, options, singletonList(field));
        this.field = field;
    }

    @Override
    protected NodeInfo<MatchQueryPredicate> info() {
        return NodeInfo.create(this, MatchQueryPredicate::new, field, query(), options());
    }

    @Override
    public MatchQueryPredicate replaceChildren(List<Expression> newChildren) {
        if (newChildren.size() != 1) {
            throw new IllegalArgumentException("expected [1] child but received [" + newChildren.size() + "]");
        }
        return new MatchQueryPredicate(source(), newChildren.get(0), query(), options());
    }

    public Expression field() {
        return field;
    }

    @Override
    public int hashCode() {
        return Objects.hash(field, super.hashCode());
    }

    @Override
    public boolean equals(Object obj) {
        if (super.equals(obj)) {
            MatchQueryPredicate other = (MatchQueryPredicate) obj;
            return Objects.equals(field, other.field);
        }
        return false;
    }
}
