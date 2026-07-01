/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.grouping;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesTo;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesToLifecycle;
import org.elasticsearch.xpack.esql.expression.function.FunctionDefinition;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.FunctionType;
import org.elasticsearch.xpack.esql.expression.function.OptionalArgument;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;

import java.io.IOException;
import java.util.List;

/**
 * Groups by all dimensions except the specified ones.
 * <p>
 * This function is implemented in block loader and translated to {@code _timeseries} field attribute by
 * {@link org.elasticsearch.xpack.esql.optimizer.rules.logical.TranslateTimeSeriesWithout}.
 */
public class TimeSeriesWithout extends GroupingFunction.NonEvaluatableGroupingFunction implements OptionalArgument {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "TimeSeriesWithout",
        TimeSeriesWithout::new
    );

    public static final FunctionDefinition DEFINITION = FunctionDefinition.def(TimeSeriesWithout.class)
        .nAry(TimeSeriesWithout::new)
        .name("without");

    @FunctionInfo(
        returnType = "keyword",
        briefSummary = "Groups by all time-series dimensions except the specified ones.",
        description = "Groups by all time-series dimensions except the specified ones. "
            + "When called with no arguments, groups by all dimensions.",
        detailedDescription = """
            `WITHOUT` is a grouping function used with the `BY` clause of
            [`STATS`](/reference/query-languages/esql/commands/stats-by.md) inside a
            [`TS`](/reference/query-languages/esql/commands/ts.md) source command. It groups by all
            time series dimensions **except** the dimensions listed.

            The output of a `STATS ... BY WITHOUT(...)` aggregation includes a `_timeseries`
            `keyword` column containing a JSON-encoded object with the dimension key/value pairs that
            identify each surviving group. When fields are excluded via `WITHOUT(dim1, dim2, ...)`,
            those dimensions are omitted from the `_timeseries` object.

            `WITHOUT()` (with no arguments) is equivalent to grouping by all dimensions. This is
            the explicit form of the implicit "group by all" behavior that `TS` uses when a bare
            [time series aggregation function](/reference/query-languages/esql/functions-operators/time-series-aggregation-functions.md)
            is used without a `BY` clause. Refer to
            [Grouping time series](/reference/query-languages/esql/commands/ts.md#grouping-time-series) for details.

            ### Limitations

            - `WITHOUT` is only supported inside time series queries that start with a
              [`TS`](/reference/query-languages/esql/commands/ts.md) source command. Using it in a
              regular `FROM | STATS ... BY WITHOUT(...)` pipeline fails with:
              `WITHOUT is only supported in time-series queries (i.e. TS | ...) at the moment`.
            - All arguments must be dimension fields. Non-dimension fields or non-field expressions
              produce an error.
            - `WITHOUT` can only appear in the first `STATS` command of a `TS` pipeline. A subsequent
              `STATS` is a regular aggregation and does not accept `WITHOUT`.""",
        type = FunctionType.GROUPING,
        appliesTo = { @FunctionAppliesTo(lifeCycle = FunctionAppliesToLifecycle.GA, version = "9.4.0") },
        examples = {
            @Example(
                description = "Aggregate by every dimension except `pod`:",
                file = "k8s-timeseries-without",
                tag = "docsWithoutSingleDimension"
            ),
            @Example(description = "Exclude multiple dimensions:", file = "k8s-timeseries-without", tag = "docsWithoutMultipleDimensions"),
            @Example(
                description = "`WITHOUT()` with no arguments groups by every dimension, equivalent to the "
                    + "implicit \"group by all\" behavior of a bare time series aggregation function:",
                file = "k8s-timeseries-without",
                tag = "docsWithoutEmpty"
            ) }
    )
    public TimeSeriesWithout(
        Source source,
        @Param(
            name = "dimension",
            type = { "keyword" },
            description = "(Optional) One or more [time series dimension]"
                + "(docs-content://manage-data/data-store/data-streams/time-series-data-stream-tsds.md#time-series-dimension) "
                + "fields to exclude from the time series grouping. Must be dimension fields of the index "
                + "(not metrics, not regular fields). When called with no arguments, groups by all dimensions.",
            optional = true
        ) List<Expression> fields
    ) {
        super(source, fields);
    }

    public TimeSeriesWithout(Source source) {
        this(source, List.of());
    }

    private TimeSeriesWithout(StreamInput in) throws IOException {
        this(Source.readFrom((PlanStreamInput) in), in.readNamedWriteableCollectionAsList(Expression.class));
    }

    public NamedExpression createNamedExpression() {
        return new Alias(source(), MetadataAttribute.TIMESERIES, this);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        source().writeTo(out);
        out.writeNamedWriteableCollection(children());
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, TimeSeriesWithout::new, children());
    }

    @Override
    public TimeSeriesWithout replaceChildren(List<Expression> newChildren) {
        return new TimeSeriesWithout(source(), newChildren);
    }

    @Override
    public DataType dataType() {
        return DataType.KEYWORD;
    }

    @Override
    public Nullability nullable() {
        return Nullability.FALSE;
    }

    @Override
    protected TypeResolution resolveType() {
        // Excluded labels are matched by name, so any label reference is acceptable - including labels that are not
        // (or not yet known to be) dimensions. Excluding a label that is absent from a series is simply a no-op.
        for (Expression field : children()) {
            if (field instanceof Attribute == false) {
                return new TypeResolution(
                    ENTRY.name + " requires label names, got [" + field.sourceText() + "] of type [" + field.dataType().typeName() + "]"
                );
            }
        }
        return TypeResolution.TYPE_RESOLVED;
    }

    @Override
    public String toString() {
        return ENTRY.name + "{fields=" + children() + "}";
    }
}
