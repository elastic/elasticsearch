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
 * Extracts the hour of day from a date. Lowers to
 * {@code DATE_EXTRACT("hour_of_day", date)} after analysis.
 */
public class Hour extends DatePartFunction {

    public static final FunctionDefinition DEFINITION = FunctionDefinition.def(Hour.class).unaryConfig(Hour::new).name("hour");

    @FunctionInfo(
        returnType = "long",
        signatures = { @Signature(params = { "date|date_nanos" }, returnType = "long") },
        briefSummary = "Extracts the hour of day from a date.",
        description = """
            Extracts the hour of day (0-23) from a date, using the query time zone.
            Equivalent to `DATE_EXTRACT("hour_of_day", date)`.
            The argument is a date or date_nanos value, not a unix-seconds long; convert seconds
            first (for example `TO_DATETIME(start * 1000)`).
            This extract is cyclic: `HOUR(ts) > 9` is not rewritten to a timestamp range.""",
        examples = @Example(file = "date", tag = "docsHour"),
        appliesTo = { @FunctionAppliesTo(lifeCycle = FunctionAppliesToLifecycle.GA, version = "9.6.0") }
    )
    public Hour(
        Source source,
        @Param(
            name = "date",
            type = { "date", "date_nanos" },
            description = "Date expression. If `null`, the function returns `null`."
        ) Expression date,
        Configuration configuration
    ) {
        super(source, date, configuration, "hour_of_day");
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new Hour(source(), newChildren.get(0), configuration());
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, Hour::new, field(), configuration());
    }
}
