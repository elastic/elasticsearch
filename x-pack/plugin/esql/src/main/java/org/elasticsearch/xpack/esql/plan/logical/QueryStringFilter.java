/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;

import java.util.Objects;

/**
 * A {@code Filter} is a type of Plan that performs filtering of results. In
 * {@code SELECT x FROM y WHERE z ..} the "WHERE" clause is a Filter. A
 * {@code Filter} has a "queryString" Expression that does the filtering.
 */
public class QueryStringFilter extends UnaryPlan {

    private final String queryString;

    public QueryStringFilter(Source source, LogicalPlan child, String queryString) {
        super(source, child);
        this.queryString = queryString;
    }

    @Override
    protected NodeInfo<QueryStringFilter> info() {
        return NodeInfo.create(this, QueryStringFilter::new, child(), queryString);
    }

    @Override
    public QueryStringFilter replaceChild(LogicalPlan newChild) {
        return new QueryStringFilter(source(), newChild, queryString);
    }

    public String queryString() {
        return queryString;
    }

    @Override
    public boolean expressionsResolved() {
        return true;
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

        QueryStringFilter other = (QueryStringFilter) obj;

        return Objects.equals(queryString, other.queryString) && Objects.equals(child(), other.child());
    }

    public QueryStringFilter with(String queryString) {
        return new QueryStringFilter(source(), child(), queryString);
    }

    public QueryStringFilter with(LogicalPlan child, String queryString) {
        return new QueryStringFilter(source(), child, queryString);
    }
}
