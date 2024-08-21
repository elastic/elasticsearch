/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical;

import org.elasticsearch.common.Strings;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.NodeUtils;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.util.Queries;
import org.elasticsearch.xpack.esql.index.EsIndex;

import java.util.List;
import java.util.Objects;

import static java.util.Arrays.asList;

/**
 * Specialized query class for retrieving statistics about the underlying data and not the actual documents.
 * For that see {@link EsQueryExec}
 */
public class EsStatsQueryExec extends LeafExec implements EstimatesRowSize {

    public enum StatsType {
        COUNT,
        MIN,
        MAX,
        EXISTS
    }

    public record Stat(String name, StatsType type, QueryBuilder query) {

        public QueryBuilder filter(QueryBuilder sourceQuery) {
            return query == null ? sourceQuery : Queries.combine(Queries.Clause.FILTER, asList(sourceQuery, query));
        }
    }

    private final EsIndex index;
    private final QueryBuilder query;
    private final Expression limit;
    private final List<Attribute> attrs;
    private final List<Stat> stats;

    public EsStatsQueryExec(
        Source source,
        EsIndex index,
        QueryBuilder query,
        Expression limit,
        List<Attribute> attributes,
        List<Stat> stats
    ) {
        super(source);
        this.index = index;
        this.query = query;
        this.limit = limit;
        this.attrs = attributes;
        this.stats = stats;
    }

    @Override
    protected NodeInfo<EsStatsQueryExec> info() {
        return NodeInfo.create(this, EsStatsQueryExec::new, index, query, limit, attrs, stats);
    }

    public EsIndex index() {
        return index;
    }

    public QueryBuilder query() {
        return query;
    }

    public List<Stat> stats() {
        return stats;
    }

    @Override
    public List<Attribute> output() {
        return attrs;
    }

    public Expression limit() {
        return limit;
    }

    @Override
    // TODO - get the estimation outside the plan so it doesn't touch the plan
    public PhysicalPlan estimateRowSize(State state) {
        int size;
        state.add(false, attrs);
        size = state.consumeAllFields(false);
        return this;
    }

    @Override
    public int hashCode() {
        return Objects.hash(index, query, limit, attrs, stats);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        EsStatsQueryExec other = (EsStatsQueryExec) obj;
        return Objects.equals(index, other.index)
            && Objects.equals(attrs, other.attrs)
            && Objects.equals(query, other.query)
            && Objects.equals(limit, other.limit)
            && Objects.equals(stats, other.stats);
    }

    @Override
    public String nodeString() {
        return nodeName()
            + "["
            + index
            + "], stats"
            + stats
            + "], query["
            + (query != null ? Strings.toString(query, false, true) : "")
            + "]"
            + NodeUtils.limitedToString(attrs)
            + ", limit["
            + (limit != null ? limit.toString() : "")
            + "], ";
    }
}
