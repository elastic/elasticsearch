/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.NodeUtils;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.util.Queries;

import java.io.IOException;
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
    }

    public sealed interface Stat {
        List<ElementType> tagTypes();
    }

    public record BasicStat(String name, StatsType type, QueryBuilder query) implements Stat {
        public QueryBuilder filter(QueryBuilder sourceQuery) {
            return query == null ? sourceQuery : Queries.combine(Queries.Clause.FILTER, asList(sourceQuery, query)).boost(0.0f);
        }

        @Override
        public List<ElementType> tagTypes() {
            return List.of();
        }
    }

    public record ByStat(List<EsQueryExec.QueryBuilderAndTags> queryBuilderAndTags) implements Stat {
        public ByStat {
            if (queryBuilderAndTags.isEmpty()) {
                throw new IllegalStateException("ByStat must have at least one queryBuilderAndTags");
            }
        }

        @Override
        public List<ElementType> tagTypes() {
            return List.of(switch (queryBuilderAndTags.getFirst().tags().getFirst()) {
                case Integer i -> ElementType.INT;
                case Long l -> ElementType.LONG;
                default -> throw new IllegalStateException(
                    "Unsupported tag type in ByStat: " + queryBuilderAndTags.getFirst().tags().getFirst()
                );
            });
        }

        @Override
        public String toString() {
            final StringBuffer sb = new StringBuffer("ByStat{");
            sb.append("queryBuilderAndTags=").append(queryBuilderAndTags);
            sb.append('}');
            return sb.toString();
        }
    }

    private final String indexPattern;
    private final QueryBuilder query;
    private final Expression limit;
    private final List<Attribute> attrs;
    private final Stat stat;

    public EsStatsQueryExec(
        Source source,
        String indexPattern,
        @Nullable QueryBuilder query,
        Expression limit,
        List<Attribute> attributes,
        Stat stat
    ) {
        super(source);
        this.indexPattern = indexPattern;
        this.query = query;
        this.limit = limit;
        this.attrs = attributes;
        this.stat = stat;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        throw new UnsupportedOperationException("not serialized");
    }

    @Override
    public String getWriteableName() {
        throw new UnsupportedOperationException("not serialized");
    }

    @Override
    protected NodeInfo<EsStatsQueryExec> info() {
        return NodeInfo.create(this, EsStatsQueryExec::new, indexPattern, query, limit, attrs, stat);
    }

    public @Nullable QueryBuilder query() {
        return query;
    }

    public Stat stat() {
        return stat;
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
        return Objects.hash(indexPattern, query, limit, attrs, stat);
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
        return Objects.equals(indexPattern, other.indexPattern)
            && Objects.equals(attrs, other.attrs)
            && Objects.equals(query, other.query)
            && Objects.equals(limit, other.limit)
            && Objects.equals(stat, other.stat);
    }

    @Override
    public String nodeString() {
        return nodeString(NodeUtils.limitedToString(attrs), limit != null ? limit.toString() : null);
    }

    @Override
    public String goldenTestToString() {
        return nodeString(NodeUtils.unlimitedToString(attrs), limit != null ? limit.goldenTestToString() : null);
    }

    private String nodeString(String attrsToString, @Nullable String limitToString) {
        return nodeName()
            + "["
            + indexPattern
            + "], stats"
            + stat
            + "], query["
            + (query != null ? Strings.toString(query, false, true) : "")
            + "]"
            + attrsToString
            + ", limit["
            + (limit != null ? limitToString : "")
            + "], ";
    }
}
