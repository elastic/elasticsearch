/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql.plan.logical;

import org.elasticsearch.xpack.eql.EqlIllegalArgumentException;
import org.elasticsearch.xpack.ql.expression.Attribute;
import org.elasticsearch.xpack.ql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.util.CollectionUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static java.util.Collections.emptyList;

public class Join extends LogicalPlan {

    private final List<LogicalPlan> queries;
    private final LogicalPlan until;

    public Join(Source source, List<LogicalPlan> queries, LogicalPlan until) {
        super(source, CollectionUtils.combine(queries, until));
        this.queries = queries;
        this.until = until;
    }

    @Override
    protected NodeInfo<Join> info() {
        return NodeInfo.create(this, Join::new, queries, until);
    }

    @Override
    public Join replaceChildren(List<LogicalPlan> newChildren) {
        if (newChildren.size() < 2) {
            throw new EqlIllegalArgumentException("expected at least [2] children but received [{}]", newChildren.size());
        }
        int lastIndex = newChildren.size() - 1;
        return new Join(source(), newChildren.subList(0, lastIndex), newChildren.get(lastIndex));
    }

    @Override
    public List<Attribute> output() {
        List<Attribute> out = new ArrayList<>();
        for (LogicalPlan plan : queries) {
            out.addAll(plan.output());
        }
        if (until != null) {
            out.addAll(until.output());
        }

        return out;
    }

    @Override
    public boolean expressionsResolved() {
        return true;
    }

    public List<LogicalPlan> queries() {
        return queries;
    }

    public LogicalPlan until() {
        return until;
    }

    @Override
    public int hashCode() {
        return Objects.hash(queries, until);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        Join other = (Join) obj;

        return Objects.equals(queries, other.queries)
                && Objects.equals(until, other.until);
    }

    @Override
    public List<Object> nodeProperties() {
        return emptyList();
    }
}
