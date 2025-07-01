/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.aggregate;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.compute.aggregation.StdDevStates;
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

import java.io.IOException;
import java.util.List;

import static java.util.Collections.emptyList;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.DEFAULT;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isType;

public class VarianceSample extends AggregateFunction implements SurrogateExpression {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "VarianceSample",
        VarianceSample::new
    );

    @FunctionInfo(
        returnType = "double",
        description = "The sample variance of a numeric field.",
        type = FunctionType.AGGREGATE,
        examples = {
            @Example(file = "stats", tag = "VarianceSample"),
            @Example(
                description = "The expression can use inline functions. For example, to calculate the "
                    + "sample variance of each employee’s maximum salary changes, "
                    + "first use `MV_MAX` on each row, and then use `VARIANCE_SAMPLE` on the result",
                file = "stats",
                tag = "docsStatsVarianceSampleNestedExpression"
            ) }
    )
    public VarianceSample(Source source, @Param(name = "number", type = { "double", "integer", "long" }) Expression field) {
        this(source, field, Literal.TRUE);
    }

    public VarianceSample(Source source, Expression field, Expression filter) {
        super(source, field, filter, emptyList());
    }

    private VarianceSample(StreamInput in) throws IOException {
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
    protected NodeInfo<VarianceSample> info() {
        return NodeInfo.create(this, VarianceSample::new, field(), filter());
    }

    @Override
    public VarianceSample replaceChildren(List<Expression> newChildren) {
        return new VarianceSample(source(), newChildren.get(0), newChildren.get(1));
    }

    public VarianceSample withFilter(Expression filter) {
        return new VarianceSample(source(), field(), filter);
    }

    @Override
    public Expression surrogate() {
        return new StdDev(
            source(),
            field(),
            filter(),
            new Literal(source(), StdDevStates.Variation.SAMPLE_VARIANCE.getIndex(), DataType.INTEGER)
        );
    }
}
