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
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToDouble;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvMedian;

import java.io.IOException;
import java.util.List;

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
            [WARNING]
            ====
            `MEDIAN` is also {wikipedia}/Nondeterministic_algorithm[non-deterministic].
            This means you can get slightly different results using the same data.
            ====""",
        isAggregation = true,
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
    public Median(Source source, @Param(name = "number", type = { "double", "integer", "long" }) Expression field) {
        super(source, field);
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

    private Median(StreamInput in) throws IOException {
        super(in);
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
        return NodeInfo.create(this, Median::new, field());
    }

    @Override
    public Median replaceChildren(List<Expression> newChildren) {
        return new Median(source(), newChildren.get(0));
    }

    @Override
    public Expression surrogate() {
        var s = source();
        var field = field();

        return field.foldable()
            ? new MvMedian(s, new ToDouble(s, field))
            : new Percentile(source(), field(), new Literal(source(), (int) QuantileStates.MEDIAN, DataType.INTEGER));
    }
}
