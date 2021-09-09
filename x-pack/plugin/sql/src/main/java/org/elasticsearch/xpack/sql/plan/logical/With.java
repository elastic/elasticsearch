/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.plan.logical;

import org.elasticsearch.xpack.ql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.ql.plan.logical.UnaryPlan;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;

import java.util.Map;
import java.util.Objects;

public class With extends UnaryPlan {
    private final Map<String, SubQueryAlias> subQueries;

    public With(Source source, LogicalPlan child, Map<String, SubQueryAlias> subQueries) {
        super(source, child);
        this.subQueries = subQueries;
    }

    @Override
    protected NodeInfo<With> info() {
        return NodeInfo.create(this, With::new, child(), subQueries);
    }

    @Override
    public With replaceChild(LogicalPlan newChild) {
        return new With(source(), newChild, subQueries);
    }

    public Map<String, SubQueryAlias> subQueries() {
        return subQueries;
    }

    @Override
    public boolean expressionsResolved() {
        return true;
    }

    @Override
    public int hashCode() {
        return Objects.hash(child(), subQueries);
    }

    @Override
    public boolean equals(Object obj) {
        if (super.equals(obj) == false) {
            return false;
        }

        With other = (With) obj;
        return Objects.equals(subQueries, other.subQueries);
    }
}
