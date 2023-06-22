/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.aggregate;

import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.aggregation.AggregatorFunctionSupplier;
import org.elasticsearch.compute.ann.Experimental;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.expression.SurrogateExpression;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.predicate.operator.arithmetic.Div;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;

import java.util.List;

@Experimental
public class Avg extends NumericAggregate implements SurrogateExpression {

    public Avg(Source source, Expression field) {
        super(source, field);
    }

    @Override
    protected NodeInfo<Avg> info() {
        return NodeInfo.create(this, Avg::new, field());
    }

    @Override
    public Avg replaceChildren(List<Expression> newChildren) {
        return new Avg(source(), newChildren.get(0));
    }

    public Expression surrogate() {
        var s = source();
        var field = field();
        return new Div(s, new Sum(s, field), new Count(s, field), dataType());
    }

    @Override
    protected AggregatorFunctionSupplier longSupplier(BigArrays bigArrays, List<Integer> inputChannels) {
        throw new EsqlIllegalArgumentException("unsupported operation");
    }

    @Override
    protected AggregatorFunctionSupplier intSupplier(BigArrays bigArrays, List<Integer> inputChannels) {
        throw new EsqlIllegalArgumentException("unsupported operation");
    }

    @Override
    protected AggregatorFunctionSupplier doubleSupplier(BigArrays bigArrays, List<Integer> inputChannels) {
        throw new EsqlIllegalArgumentException("unsupported operation");
    }
}
