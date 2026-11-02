/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.timeseries;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesTo;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesToLifecycle;
import org.elasticsearch.xpack.esql.expression.function.FunctionDefinition;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;

import java.io.IOException;
import java.util.List;

/** Replaces a label using each row's value; empty or null values remove the label. */
public final class TimeSeriesSet extends TimeSeriesFunction {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "TimeSeriesSet",
        TimeSeriesSet::new
    );
    public static final FunctionDefinition DEFINITION = FunctionDefinition.def(TimeSeriesSet.class)
        .ternary(TimeSeriesSet::new)
        .name("ts_set");

    @FunctionInfo(
        returnType = "_timeseries",
        appliesTo = @FunctionAppliesTo(lifeCycle = FunctionAppliesToLifecycle.EXPERIMENTAL),
        briefSummary = "Sets a label in time-series metadata.",
        description = "Sets a label in time-series metadata."
    )
    public TimeSeriesSet(
        Source source,
        @Param(
            name = "metadata",
            type = "_timeseries",
            description = "Time-series metadata; null denotes the empty label set."
        ) Expression metadata,
        @Param(name = "label", type = "keyword", description = "Constant label name.") Expression label,
        @Param(
            name = "value",
            type = { "keyword", "text", "boolean", "integer", "long", "unsigned_long", "double", "ip" },
            description = "New label value."
        ) Expression value
    ) {
        super(source, List.of(metadata, label, value));
    }

    private TimeSeriesSet(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    public DataType dataType() {
        return DataType.TIMESERIES_DATA_TYPE;
    }

    @Override
    public ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        return TimeSeriesEvaluator.factory(TimeSeriesEvaluator.Operation.SET, children(), toEvaluator);
    }

    @Override
    protected TypeResolution resolveArguments() {
        return labelName(1).and(labelValue(2));
    }

    @Override
    public TimeSeriesSet replaceChildren(List<Expression> children) {
        return new TimeSeriesSet(source(), children.get(0), children.get(1), children.get(2));
    }

    @Override
    protected NodeInfo<TimeSeriesSet> info() {
        return NodeInfo.create(this, TimeSeriesSet::new, children().get(0), children().get(1), children().get(2));
    }
}
