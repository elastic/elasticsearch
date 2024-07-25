/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.aggregate;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.SurrogateExpression;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvAvg;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Div;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.DEFAULT;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isType;

public class Avg extends AggregateFunction implements SurrogateExpression {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "Avg", Avg::new);

    @FunctionInfo(
        returnType = "double",
        description = "The average of a numeric field.",
        isAggregation = true,
        examples = {
            @Example(file = "stats", tag = "avg"),
            @Example(
                description = "The expression can use inline functions. For example, to calculate the average "
                    + "over a multivalued column, first use `MV_AVG` to average the multiple values per row, "
                    + "and use the result with the `AVG` function",
                file = "stats",
                tag = "docsStatsAvgNestedExpression"
            ) }
    )
    public Avg(Source source, @Param(name = "number", type = { "double", "integer", "long" }) Expression field) {
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

    private Avg(StreamInput in) throws IOException {
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
    protected NodeInfo<Avg> info() {
        return NodeInfo.create(this, Avg::new, field());
    }

    @Override
    public Avg replaceChildren(List<Expression> newChildren) {
        return new Avg(source(), newChildren.get(0));
    }

    @Override
    public Expression surrogate() {
        var s = source();
        var field = field();

        return field().foldable() ? new MvAvg(s, field) : new Div(s, new Sum(s, field), new Count(s, field), dataType());
    }
}
