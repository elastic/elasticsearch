/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.date;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.ann.Evaluator;
import org.elasticsearch.compute.ann.Fixed;
import org.elasticsearch.compute.operator.EvalOperator.ExpressionEvaluator;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.ExpressionContext;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.ConfigurationFunction;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.scalar.EsqlScalarFunction;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;

import java.io.IOException;
import java.time.ZonedDateTime;
import java.util.List;

public class Now extends EsqlScalarFunction implements ConfigurationFunction {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "Now", Now::new);

    private final long now;

    @FunctionInfo(
        returnType = "date",
        description = "Returns current date and time.",
        examples = {
            @Example(file = "date", tag = "docsNow"),
            @Example(file = "date", tag = "docsNowWhere", description = "To retrieve logs from the last hour:") }
    )
    public Now(Source source) {
        super(source, List.of());
    }

    private Now(StreamInput in) throws IOException {
        this(Source.readFrom((PlanStreamInput) in));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        Source.EMPTY.writeTo(out);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    public Object fold(ExpressionContext ctx) {
        ZonedDateTime now = ctx.configuration().now();
        assert now != null;
        return ctx.configuration().now().toInstant().toEpochMilli();
    }

    @Override
    public boolean foldable() {
        return true;
    }

    @Override
    public DataType dataType() {
        return DataType.DATETIME;
    }

    @Evaluator
    static long process(@Fixed long now) {
        return now;
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return this;
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this);
    }

    @Override
    public ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        assert toEvaluator.configuration().now() != null;
        return dvrCtx -> new NowEvaluator(source(), toEvaluator.configuration().now().toInstant().toEpochMilli(), dvrCtx);
    }
}
