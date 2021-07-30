/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ql.expression.predicate.fulltext;

import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;

import java.util.List;
import java.util.Objects;

import static java.util.Collections.singletonList;

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
