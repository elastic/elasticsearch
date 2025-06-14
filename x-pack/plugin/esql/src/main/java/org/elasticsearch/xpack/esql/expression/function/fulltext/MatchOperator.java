/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.fulltext;

import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;

import java.util.List;

/**
 * This class performs a {@link org.elasticsearch.xpack.esql.querydsl.query.MatchQuery} using an operator.
 * This is used as a convenience for generating documentation and for error message purposes - it’s  a way to represent
 * the match operator in the function syntax.
 * Serialization is provided as a way to pass the corresponding tests - serialization must be done to a Match class.
 */
public class MatchOperator extends Match {

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
        examples = { @Example(file = "match-operator", tag = "match-with-field") }
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
        super(source, List.of(field), matchQuery, null, null);
    }

    private MatchOperator(Source source, Expression field, Expression matchQuery, QueryBuilder queryBuilder) {
        super(source, List.of(field), matchQuery, null, queryBuilder);
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
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, MatchOperator::new, fields().getFirst(), query());
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        // Query first, then field.
        return new MatchOperator(source(), newChildren.get(1), newChildren.get(0), queryBuilder());
    }

    @Override
    public Expression replaceQueryBuilder(QueryBuilder queryBuilder) {
        return new MatchOperator(source(), fields().getFirst(), query(), queryBuilder);
    }
}
