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
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;

import java.io.IOException;
import java.util.List;

/**
 * This class performs a {@link org.elasticsearch.xpack.esql.querydsl.query.MatchQuery} using an operator.
 */
public class MatchOperator extends AbstractMatchFullTextFunction {

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "MatchOperator",
        MatchOperator::readFrom
    );

    @FunctionInfo(
        returnType = "boolean",
        operator = ":",
        preview = true,
        description = """
            Use the match operator (`:`) to perform a <<query-dsl-match-query,match query>> on the specified field.
            Using `:` is equivalent to using the `match` query in the Elasticsearch Query DSL.

            The match operator is equivalent to the <<esql-match,match function>>.

            For using the function syntax, or adding <<match-field-params,match query parameters>>, you can use the
            <<esql-match,match function>>.

            `:` returns true if the provided query matches the row.""",
        examples = { @Example(file = "match-function", tag = "match-with-field") }
    )
    public MatchOperator(
        Source source,
        @Param(
            name = "field",
            type = { "keyword", "text", "boolean", "date", "date_nanos", "double", "integer", "ip", "long", "unsigned_long", "version" },
            description = "Field that the query will target."
        ) Expression field,
        @Param(
            name = "query",
            type = { "keyword", "boolean", "date", "date_nanos", "double", "integer", "ip", "long", "unsigned_long", "version" },
            description = "Value to find in the provided field."
        ) Expression matchQuery
    ) {
        this(source, field, matchQuery, null);
    }

    private MatchOperator(Source source, Expression field, Expression matchQuery, QueryBuilder queryBuilder) {
        super(source, matchQuery, List.of(field, matchQuery), queryBuilder, field);
    }

    private static MatchOperator readFrom(StreamInput in) throws IOException {
        Source source = Source.readFrom((PlanStreamInput) in);
        Expression field = in.readNamedWriteable(Expression.class);
        Expression query = in.readNamedWriteable(Expression.class);
        QueryBuilder queryBuilder = null;
        if (in.getTransportVersion().onOrAfter(TransportVersions.ESQL_QUERY_BUILDER_IN_SEARCH_FUNCTIONS)) {
            queryBuilder = in.readOptionalNamedWriteable(QueryBuilder.class);
        }
        return new MatchOperator(source, field, query, queryBuilder);
    }

    @Override
    public String functionType() {
        return "operator";
    }

    @Override
    public String functionName() {
        return ":";
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, MatchOperator::new, field(), query());
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new MatchOperator(source(), newChildren.get(0), newChildren.get(1));
    }

    @Override
    public Expression replaceQueryBuilder(QueryBuilder queryBuilder) {
        return new MatchOperator(source(), field(), query(), queryBuilder);
    }
}
