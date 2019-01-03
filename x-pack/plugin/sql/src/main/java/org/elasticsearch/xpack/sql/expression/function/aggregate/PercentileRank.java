/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.aggregate;

import org.elasticsearch.xpack.sql.SqlIllegalArgumentException;
import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.Expressions;
import org.elasticsearch.xpack.sql.expression.Expressions.ParamOrdinal;
import org.elasticsearch.xpack.sql.expression.Foldables;
import org.elasticsearch.xpack.sql.tree.Source;
import org.elasticsearch.xpack.sql.tree.NodeInfo;
import org.elasticsearch.xpack.sql.type.DataType;

import java.util.List;

import static java.util.Collections.singletonList;

public class PercentileRank extends AggregateFunction implements EnclosedAgg {

    private final Expression value;

    public PercentileRank(Source source, Expression field, Expression value) {
        super(source, field, singletonList(value));
        this.value = value;
    }

    @Override
    protected NodeInfo<PercentileRank> info() {
        return NodeInfo.create(this, PercentileRank::new, field(), value);
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        if (newChildren.size() != 2) {
            throw new IllegalArgumentException("expected [2] children but received [" + newChildren.size() + "]");
        }
        return new PercentileRank(source(), newChildren.get(0), newChildren.get(1));
    }

    @Override
    protected TypeResolution resolveType() {
        if (!value.foldable()) {
            throw new SqlIllegalArgumentException("2nd argument of PERCENTILE_RANK must be constant, received [{}]",
                Expressions.name(value));
        }

        TypeResolution resolution = super.resolveType();
        if (resolution.unresolved()) {
            return resolution;
        }

        return Expressions.typeMustBeNumeric(value, functionName(), ParamOrdinal.DEFAULT);
    }

    public Expression value() {
        return value;
    }

    @Override
    public DataType dataType() {
        return DataType.DOUBLE;
    }

    @Override
    public String innerName() {
        return Double.toString(Foldables.doubleValueOf(value));
    }
}
