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

public class StdDevSample extends AggregateFunction implements SurrogateExpression {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "StdDevSample",
        StdDevSample::new
    );

    @FunctionInfo(
        returnType = "double",
        description = "The sample standard deviation of a numeric field.",
        type = FunctionType.AGGREGATE,
        examples = {
            @Example(file = "stats", tag = "StdDevSample"),
            @Example(
                description = "The expression can use inline functions. For example, to calculate the "
                    + "sample standard deviation of each employee’s maximum salary changes, "
                    + "first use `MV_MAX` on each row, and then use `STD_DEV_SAMPLE` on the result",
                file = "stats",
                tag = "docsStatsStdDevSampleNestedExpression"
            ) }
    )
    public StdDevSample(Source source, @Param(name = "number", type = { "double", "integer", "long" }) Expression field) {
        this(source, field, Literal.TRUE);
    }

    public StdDevSample(Source source, Expression field, Expression filter) {
        super(source, field, filter, emptyList());
    }

    private StdDevSample(StreamInput in) throws IOException {
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
    protected NodeInfo<StdDevSample> info() {
        return NodeInfo.create(this, StdDevSample::new, field(), filter());
    }

    @Override
    public StdDevSample replaceChildren(List<Expression> newChildren) {
        return new StdDevSample(source(), newChildren.get(0), newChildren.get(1));
    }

    public StdDevSample withFilter(Expression filter) {
        return new StdDevSample(source(), field(), filter);
    }

    @Override
    public Expression surrogate() {
        return new StdDev(source(), field(), filter(), new Literal(source(), StdDevStates.Variation.SAMPLE.getIndex(), DataType.INTEGER));

    }
}
