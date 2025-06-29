/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.aggregate;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.compute.aggregation.QuantileStates;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.SurrogateExpression;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.FunctionType;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToDouble;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvMedian;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;

import java.io.IOException;
import java.util.List;

import static java.util.Collections.emptyList;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.DEFAULT;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isType;

public class Median extends AggregateFunction implements SurrogateExpression {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "Median", Median::new);

    // TODO: Add the compression parameter
    @FunctionInfo(
        returnType = "double",
        description = "The value that is greater than half of all values and less than half of all values, "
            + "also known as the 50% <<esql-percentile>>.",
        note = "Like <<esql-percentile>>, `MEDIAN` is <<esql-percentile-approximate,usually approximate>>.",
        appendix = """
            ::::{warning}
            `MEDIAN` is also {wikipedia}/Nondeterministic_algorithm[non-deterministic].
            This means you can get slightly different results using the same data.
            ::::""",
        type = FunctionType.AGGREGATE,
        examples = {
            @Example(file = "stats_percentile", tag = "median"),
            @Example(
                description = "The expression can use inline functions. For example, to calculate the median of "
                    + "the maximum values of a multivalued column, first use `MV_MAX` to get the "
                    + "maximum value per row, and use the result with the `MEDIAN` function",
                file = "stats_percentile",
                tag = "docsStatsMedianNestedExpression"
            ), }
    )
    public Median(
        Source source,
        @Param(
            name = "number",
            type = { "double", "integer", "long" },
            description = "Expression that outputs values to calculate the median of."
        ) Expression field,
        QueryPragmas pragmas
    ) {
        this(source, field, Literal.TRUE, pragmas);
    }

    public Median(Source source, Expression field, Expression filter, QueryPragmas pragmas) {
        super(source, field, filter, emptyList());
        this.pragmas = pragmas;
    }

    private final QueryPragmas pragmas;

    private Median(StreamInput in) throws IOException {
        super(in);
        this.pragmas = in.readNamedWriteable(QueryPragmas.class);
    }

    @Override
    protected Expression.TypeResolution resolveType() {
        return isType(
            field(),
            dt -> dt.isNumeric() && dt != DataType.UNSIGNED_LONG,
            sourceText(),
            DEFAULT,
            "numeric except unsigned_long or counter types"
        );
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    public DataType dataType() {
        return DataType.DOUBLE;
    }

    @Override
    protected NodeInfo<Median> info() {
        return NodeInfo.create(this, Median::new, field(), filter(), pragmas);
    }

    @Override
    public Median replaceChildren(List<Expression> newChildren) {
        return new Median(source(), newChildren.get(0), newChildren.get(1), pragmas);
    }

    @Override
    public AggregateFunction withFilter(Expression filter) {
        return new Median(source(), field(), filter, pragmas);
    }

    @Override
    public Expression surrogate() {
        var s = source();
        var field = field();

        return field.foldable()
            ? new MvMedian(s, new ToDouble(s, field, pragmas))
            : new Percentile(source(), field(), new Literal(source(), (int) QuantileStates.MEDIAN, DataType.INTEGER), pragmas);
    }
}
