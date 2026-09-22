/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.date;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesTo;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesToLifecycle;
import org.elasticsearch.xpack.esql.expression.function.FunctionDefinition;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.Signature;
import org.elasticsearch.xpack.esql.session.Configuration;

import java.util.List;

/**
 * Extracts the calendar year from a date. Lowers to
 * {@code DATE_EXTRACT("year", date)} after analysis.
 */
public class Year extends DatePartFunction {

    public static final FunctionDefinition DEFINITION = FunctionDefinition.def(Year.class).unaryConfig(Year::new).name("year");

    @FunctionInfo(
        returnType = "long",
        signatures = { @Signature(params = { "date|date_nanos" }, returnType = "long") },
        briefSummary = "Extracts the year from a date.",
        description = """
            Extracts the calendar year from a date, using the query time zone.
            Equivalent to `DATE_EXTRACT("year", date)`.
            The argument is a date or date_nanos value, not a unix-seconds long; convert seconds
            first (for example `TO_DATETIME(start * 1000)`).""",
        examples = @Example(file = "date", tag = "docsYear"),
        appliesTo = { @FunctionAppliesTo(lifeCycle = FunctionAppliesToLifecycle.GA, version = "9.6.0") }
    )
    public Year(
        Source source,
        @Param(
            name = "date",
            type = { "date", "date_nanos" },
            description = "Date expression. If `null`, the function returns `null`."
        ) Expression date,
        Configuration configuration
    ) {
        super(source, date, configuration, "year");
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new Year(source(), newChildren.get(0), configuration());
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, Year::new, field(), configuration());
    }
}
