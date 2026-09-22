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
 * Extracts the month of year from a date. Lowers to
 * {@code DATE_EXTRACT("month_of_year", date)} after analysis.
 */
public class Month extends DatePartFunction {

    public static final FunctionDefinition DEFINITION = FunctionDefinition.def(Month.class).unaryConfig(Month::new).name("month");

    @FunctionInfo(
        returnType = "long",
        signatures = { @Signature(params = { "date|date_nanos" }, returnType = "long") },
        briefSummary = "Extracts the month of year from a date.",
        description = """
            Extracts the month of year (1-12) from a date, using the query time zone.
            Equivalent to `DATE_EXTRACT("month_of_year", date)`.
            The argument is a date or date_nanos value, not a unix-seconds long; convert seconds
            first (for example `TO_DATETIME(start * 1000)`).
            This extract is cyclic: `MONTH(ts) > 3` is not rewritten to a timestamp range.""",
        examples = @Example(file = "date", tag = "docsMonth"),
        appliesTo = { @FunctionAppliesTo(lifeCycle = FunctionAppliesToLifecycle.GA, version = "9.6.0") }
    )
    public Month(
        Source source,
        @Param(
            name = "date",
            type = { "date", "date_nanos" },
            description = "Date expression. If `null`, the function returns `null`."
        ) Expression date,
        Configuration configuration
    ) {
        super(source, date, configuration, "month_of_year");
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new Month(source(), newChildren.get(0), configuration());
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, Month::new, field(), configuration());
    }
}
