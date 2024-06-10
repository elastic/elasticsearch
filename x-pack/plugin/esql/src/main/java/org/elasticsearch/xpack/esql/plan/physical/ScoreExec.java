/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;

import java.util.Objects;

public class ScoreExec extends UnaryExec {

    private final Expression query;

    public ScoreExec(Source source, PhysicalPlan child, Expression query) {
        super(source, child);
        this.query = query;
    }

    @Override
    public ScoreExec replaceChild(PhysicalPlan newChild) {
        return new ScoreExec(source(), newChild, query);
    }

    @Override
    protected NodeInfo<? extends PhysicalPlan> info() {
        return NodeInfo.create(this, ScoreExec::new, child(), query);
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
        return Objects.equals(query, ((ScoreExec) obj).query);
    }
}
