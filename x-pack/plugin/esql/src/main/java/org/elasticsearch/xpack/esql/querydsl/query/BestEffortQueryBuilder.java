/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.querydsl.query;

import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.Rewriteable;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.search.internal.MaxClauseCountQueryVisitor;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.stats.SearchContextStats;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * A filter pushed to every shard, which each shard runs as well as it can. A shard that can query every field the filter reads,
 * as ES|QL reads them, runs {@code query}; any other shard matches every document, and the filter the query is pushed from checks
 * the values it reads there.
 * <p>
 * It is built on the data node that runs it, so it is never serialized.
 */
public class BestEffortQueryBuilder extends AbstractQueryBuilder<BestEffortQueryBuilder> {
    private static final String NAME = "esql_best_effort";

    /**
     * A field the filter reads.
     *
     * @param comparesValuesAsRead whether the filter compares the values of the field as read, as it does a field some indices may
     *                             not map, rather than as the index normalizes its input
     */
    public record Field(String name, DataType type, boolean comparesValuesAsRead) {}

    private final QueryBuilder query;
    private final List<Field> fields;

    public BestEffortQueryBuilder(QueryBuilder query, List<Field> fields) {
        this.query = Objects.requireNonNull(query);
        this.fields = List.copyOf(fields);
    }

    public QueryBuilder query() {
        return query;
    }

    public List<Field> fields() {
        return fields;
    }

    @Override
    protected Query doToQuery(SearchExecutionContext context, MaxClauseCountQueryVisitor visitor) throws IOException {
        for (Field field : fields) {
            if (SearchContextStats.isQueryable(context, field.name(), field.type(), field.comparesValuesAsRead()) == false) {
                // Scores nothing, like the filter it stands for
                Query all = new BoostQuery(Queries.ALL_DOCS_INSTANCE, 0f);
                all.visit(visitor);
                return all;
            }
        }
        return Rewriteable.rewrite(query, context, true).toQuery(context, visitor);
    }

    @Override
    protected QueryBuilder doRewrite(QueryRewriteContext queryRewriteContext) {
        // Only the shard knows whether it runs the query, so the query is rewritten there
        return this;
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        builder.field("query", query, params);
        builder.startArray("fields");
        for (Field field : fields) {
            builder.value(field.name());
        }
        builder.endArray();
        printBoostAndQueryName(builder);
        builder.endObject();
    }

    @Override
    protected void doWriteTo(StreamOutput out) {
        throw new UnsupportedOperationException("Not serialized");
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        throw new UnsupportedOperationException("Not serialized");
    }

    @Override
    protected boolean doEquals(BestEffortQueryBuilder other) {
        return query.equals(other.query) && fields.equals(other.fields);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(query, fields);
    }
}
