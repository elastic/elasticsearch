/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.aggregate;

import org.elasticsearch.compute.aggregation.QuantileStates;
import org.elasticsearch.compute.ann.Experimental;
import org.elasticsearch.xpack.esql.expression.SurrogateExpression;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Literal;
import org.elasticsearch.xpack.ql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;

import java.util.List;

import static org.elasticsearch.xpack.ql.expression.TypeResolutions.ParamOrdinal.DEFAULT;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.isNumeric;

@Experimental
public class Median extends AggregateFunction implements SurrogateExpression {
    // TODO: Add the compression parameter
    public Median(Source source, Expression field) {
        super(source, field);
    }

    @Override
    protected Expression.TypeResolution resolveType() {
        return isNumeric(field(), sourceText(), DEFAULT);
    }

    @Override
    public DataType dataType() {
        return DataTypes.DOUBLE;
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
        return new Percentile(source(), field(), new Literal(source(), (int) QuantileStates.MEDIAN, DataTypes.INTEGER));
    }
}
