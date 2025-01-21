/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.fulltext;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;

import java.io.IOException;

/**
 * This class exists to generate documentation for the match operator.
 * Unlike other ESQL functions, Match is both a function and an operator. This class provides the documentation for the operator.
 */
public class MatchOperator extends Match {

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
        super(source, field, matchQuery);
    }

    protected static Match readFrom(StreamInput in) throws IOException {
        Source source = Source.readFrom((PlanStreamInput) in);
        Expression field = in.readNamedWriteable(Expression.class);
        Expression query = in.readNamedWriteable(Expression.class);

        return new MatchOperator(source, field, query);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }
}
