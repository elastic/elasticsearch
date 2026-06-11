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
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.NameId;
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
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Grouping function that keeps time-series grouping generic while excluding the specified dimensions.
 * An empty field list means "group by all dimensions".
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

    private TimeSeriesWithout(StreamInput in) throws IOException {
        this(Source.readFrom((PlanStreamInput) in), in.readNamedWriteableCollectionAsList(Expression.class));
    }

    /**
     * Returns the canonical grouping expression for the {@code _timeseries} metadata column.
     */
    public Alias toAttribute() {
        return toAttribute(null);
    }

    /**
     * Returns the canonical grouping expression for the {@code _timeseries} metadata column.
     */
    public Alias toAttribute(@Nullable NameId id) {
        return new Alias(source(), MetadataAttribute.TIMESERIES, this, id);
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
        for (Expression field : children()) {
            if (field instanceof FieldAttribute fa) {
                if (fa.isDimension() == false) {
                    return new TypeResolution("WITHOUT requires dimension fields, but [" + fa.sourceText() + "] is not a dimension");
                }
            } else {
                return new TypeResolution(
                    "WITHOUT requires dimension field names, got [" + field.sourceText() + "] of type [" + field.dataType().typeName() + "]"
                );
            }
        }
        return TypeResolution.TYPE_RESOLVED;
    }

    /**
     * Returns the set of field names to exclude from the time-series grouping.
     */
    public Set<String> excludedFieldNames() {
        Set<String> excluded = new LinkedHashSet<>();
        for (Expression field : children()) {
            if (field instanceof FieldAttribute fa) {
                excluded.add(fa.fieldName().string());
            }
        }
        return excluded;
    }

    @Override
    public String toString() {
        return "TimeSeriesWithout{fields=" + children() + "}";
    }
}
