/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.plan.physical;

import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;

import java.util.List;
import java.util.Objects;

public class QueryStringFilterExec extends UnaryExec {

    private final String queryString;

    public QueryStringFilterExec(Source source, PhysicalPlan child, String queryString) {
        super(source, child);
        this.queryString = queryString;
    }

    @Override
    protected NodeInfo<QueryStringFilterExec> info() {
        return NodeInfo.create(this, QueryStringFilterExec::new, child(), queryString);
    }

    @Override
    public QueryStringFilterExec replaceChild(PhysicalPlan newChild) {
        return new QueryStringFilterExec(source(), newChild, queryString);
    }

    public String queryString() {
        return queryString;
    }

    @Override
    public List<Attribute> output() {
        return child().output();
    }

    @Override
    public int hashCode() {
        return Objects.hash(queryString, child());
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        QueryStringFilterExec other = (QueryStringFilterExec) obj;
        return Objects.equals(queryString, other.queryString) && Objects.equals(child(), other.child());
    }
}
