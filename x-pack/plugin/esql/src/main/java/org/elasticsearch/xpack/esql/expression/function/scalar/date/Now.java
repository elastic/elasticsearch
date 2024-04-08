/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.date;

import org.elasticsearch.compute.ann.Evaluator;
import org.elasticsearch.compute.ann.Fixed;
import org.elasticsearch.compute.operator.EvalOperator.ExpressionEvaluator;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.scalar.EsqlConfigurationFunction;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.session.Configuration;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;

import java.util.List;
import java.util.function.Function;

public class Now extends EsqlConfigurationFunction {

    private final long now;

    @FunctionInfo(returnType = "date", description = "Returns current date and time.")
    public Now(Source source, Configuration configuration) {
        super(source, List.of(), configuration);
        this.now = configuration.now() == null ? System.currentTimeMillis() : configuration.now().toInstant().toEpochMilli();
    }

    private Now(Source source, long now) {
        super(source, List.of(), null);
        this.now = now;
    }

    public static Now newInstance(Source source, long now) {
        return new Now(source, now);
    }

    @Override
    public Object fold() {
        return now;
    }

    @Override
    public boolean foldable() {
        return true;
    }

    @Override
    public DataType dataType() {
        return DataTypes.DATETIME;
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
        return NodeInfo.create(this, Now::new, configuration());
    }

    @Override
    public ExpressionEvaluator.Factory toEvaluator(Function<Expression, ExpressionEvaluator.Factory> toEvaluator) {
        return dvrCtx -> new NowEvaluator(source(), now, dvrCtx);
    }
}
