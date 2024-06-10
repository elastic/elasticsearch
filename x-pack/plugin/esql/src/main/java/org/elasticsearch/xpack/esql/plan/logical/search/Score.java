/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.search;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.core.plan.logical.UnaryPlan;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;

import java.util.Objects;

public class Score extends UnaryPlan {

    private final Expression query;

    public Score(Source source, LogicalPlan child, Expression query) {
        super(source, child);
        this.query = query;
    }

    @Override
    public boolean expressionsResolved() {
        return query.resolved();
    }

    @Override
    public Score replaceChild(LogicalPlan newChild) {
        return new Score(source(), newChild, query);
    }

    @Override
    protected NodeInfo<? extends LogicalPlan> info() {
        return NodeInfo.create(this, Score::new, child(), query);
    }

    public Expression query() {
        return query;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), query);
    }

    @Override
    public boolean equals(Object obj) {
        if (false == super.equals(obj)) {
            return false;
        }
        return Objects.equals(query, ((Score) obj).query);
    }
}
