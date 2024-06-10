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

public class RankExec extends UnaryExec {

    private final Expression query;

    public RankExec(Source source, PhysicalPlan child, Expression query) {
        super(source, child);
        this.query = query;
    }

    @Override
    public RankExec replaceChild(PhysicalPlan newChild) {
        return new RankExec(source(), newChild, query);
    }

    @Override
    protected NodeInfo<? extends PhysicalPlan> info() {
        return NodeInfo.create(this, RankExec::new, child(), query);
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
        if (getClass() != obj.getClass()) {
            return false;
        }
        RankExec that = (RankExec) obj;
        return Objects.equals(query, that.query);
    }
}
