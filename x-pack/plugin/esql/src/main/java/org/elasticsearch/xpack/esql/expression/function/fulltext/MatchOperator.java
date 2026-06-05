/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.fulltext;

import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.xpack.esql.capabilities.ConfigurationAware;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesTo;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesToLifecycle;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.session.Configuration;

import java.util.List;

/**
 * Use the match operator ({@code :}) to perform a match query on the specified field.
 * Using {@code :} is equivalent to using the {@code match} query in the Elasticsearch Query DSL.
 *
 * The match operator is equivalent to the match function.
 *
 * For using the function syntax, or adding match query parameters, you can use the match function.
 *
 * {@code :} returns true if the provided query matches the row.
 */
public class MatchOperator extends Match implements ConfigurationAware {

    @FunctionInfo(
        returnType = "boolean",
        operator = ":",
        appliesTo = {
            @FunctionAppliesTo(lifeCycle = FunctionAppliesToLifecycle.PREVIEW, version = "9.0.0"),
            @FunctionAppliesTo(lifeCycle = FunctionAppliesToLifecycle.GA, version = "9.1.0") },
        examples = {
            @Example(
                file = "match-operator",
                tag = "match-with-text-field",
                description = "The match operator can be used to perform full-text search on a `text` field. "
                    + "Notice how the match operator handles multi-valued columns, if a single value matches the query string, "
                    + "the expression evaluates to `TRUE`."
            ),
            @Example(
                file = "match-operator",
                tag = "match-with-keyword-field",
                description = " The match operator can also be used with `keyword` columns to filter multi-values."
            ),
            @Example(
                file = "match-operator",
                tag = "match-with-semantic-text-field",
                description = "This example illustrates how to do semantic search using the match operator on `semantic_text` fields. "
                    + "By including the metadata field `_score` and sorting on `_score`, "
                    + "we can retrieve the most relevant results in order."
            ), }
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
        ) Expression matchQuery,
        Configuration configuration
    ) {
        super(source, field, matchQuery, null, null, configuration);
    }

    private MatchOperator(Source source, Expression field, Expression matchQuery, QueryBuilder queryBuilder, Configuration configuration) {
        super(source, field, matchQuery, null, queryBuilder, configuration);
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
        return NodeInfo.create(this, MatchOperator::new, field(), query(), configuration());
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new MatchOperator(source(), newChildren.get(0), newChildren.get(1), queryBuilder(), configuration());
    }

    @Override
    public Expression replaceQueryBuilder(QueryBuilder queryBuilder) {
        return new MatchOperator(source(), field, query(), queryBuilder, configuration());
    }

    @Override
    public Configuration configuration() {
        return super.configuration();
    }

    @Override
    public Expression withConfiguration(Configuration configuration) {
        return new MatchOperator(source(), field, query(), queryBuilder(), configuration);
    }
}
