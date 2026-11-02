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
import java.util.stream.Stream;

/** Removes the requested labels; removing an absent label leaves the metadata unchanged. */
public final class TimeSeriesUnset extends TimeSeriesFunction {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "TimeSeriesUnset",
        TimeSeriesUnset::new
    );
    public static final FunctionDefinition DEFINITION = FunctionDefinition.def(TimeSeriesUnset.class)
        .unaryVariadic(TimeSeriesUnset::new)
        .name("ts_unset");

    @FunctionInfo(
        returnType = "_timeseries",
        appliesTo = @FunctionAppliesTo(lifeCycle = FunctionAppliesToLifecycle.EXPERIMENTAL),
        briefSummary = "Removes labels from time-series metadata.",
        description = "Removes labels from time-series metadata."
    )
    public TimeSeriesUnset(
        Source source,
        @Param(
            name = "metadata",
            type = "_timeseries",
            description = "Time-series metadata; null denotes the empty label set."
        ) Expression metadata,
        @Param(name = "labels", type = "keyword", description = "Constant label names to remove.") List<Expression> labels
    ) {
        super(source, Stream.concat(Stream.of(metadata), labels.stream()).toList());
    }

    private TimeSeriesUnset(StreamInput in) throws IOException {
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
        return TimeSeriesEvaluator.factory(TimeSeriesEvaluator.Operation.UNSET, children(), toEvaluator);
    }

    @Override
    protected TypeResolution resolveArguments() {
        for (int i = 1; i < children().size(); i++) {
            TypeResolution resolution = labelName(i);
            if (resolution.unresolved()) {
                return resolution;
            }
        }
        return TypeResolution.TYPE_RESOLVED;
    }

    @Override
    public TimeSeriesUnset replaceChildren(List<Expression> children) {
        return new TimeSeriesUnset(source(), children.getFirst(), children.subList(1, children.size()));
    }

    @Override
    protected NodeInfo<TimeSeriesUnset> info() {
        return NodeInfo.create(this, TimeSeriesUnset::new, metadata(), children().subList(1, children().size()));
    }
}
