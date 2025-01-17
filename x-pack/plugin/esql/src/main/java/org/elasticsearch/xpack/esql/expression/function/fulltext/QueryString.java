/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.fulltext;

import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.querydsl.query.Query;
import org.elasticsearch.xpack.esql.core.querydsl.query.QueryStringQuery;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.planner.TranslatorHandler;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Full text function that performs a {@link QueryStringQuery} .
 */
public class QueryString extends FullTextFunction {

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "QStr",
        QueryString::readFrom
    );

    @FunctionInfo(
        returnType = "boolean",
        preview = true,
        description = "Performs a <<query-dsl-query-string-query,query string query>>. "
            + "Returns true if the provided query string matches the row.",
        examples = { @Example(file = "qstr-function", tag = "qstr-with-field") }
    )
    public QueryString(
        Source source,
        @Param(
            name = "query",
            type = { "keyword", "text" },
            description = "Query string in Lucene query string format."
        ) Expression queryString
    ) {
        super(source, queryString, List.of(queryString), null);
    }

    public QueryString(Source source, Expression queryString, QueryBuilder queryBuilder) {
        super(source, queryString, List.of(queryString), queryBuilder);
    }

    private static QueryString readFrom(StreamInput in) throws IOException {
        Source source = Source.readFrom((PlanStreamInput) in);
        Expression query = in.readNamedWriteable(Expression.class);
        QueryBuilder queryBuilder = null;
        if (in.getTransportVersion().onOrAfter(TransportVersions.ESQL_QUERY_BUILDER_IN_SEARCH_FUNCTIONS)) {
            queryBuilder = in.readOptionalNamedWriteable(QueryBuilder.class);
        }
        return new QueryString(source, query, queryBuilder);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        source().writeTo(out);
        out.writeNamedWriteable(query());
        if (out.getTransportVersion().onOrAfter(TransportVersions.ESQL_QUERY_BUILDER_IN_SEARCH_FUNCTIONS)) {
            out.writeOptionalNamedWriteable(queryBuilder());
        }
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    public String functionName() {
        return "QSTR";
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new QueryString(source(), newChildren.get(0), queryBuilder());
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, QueryString::new, query(), queryBuilder());
    }

    @Override
    protected Query translate(TranslatorHandler handler) {
        return new QueryStringQuery(source(), Objects.toString(queryAsObject()), Map.of(), Map.of());
    }

    @Override
    public Expression replaceQueryBuilder(QueryBuilder queryBuilder) {
        return new QueryString(source(), query(), queryBuilder);
    }
}
