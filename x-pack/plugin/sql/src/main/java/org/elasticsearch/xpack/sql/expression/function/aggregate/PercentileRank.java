/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.aggregate;

import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Expressions.ParamOrdinal;
import org.elasticsearch.xpack.ql.expression.Foldables;
import org.elasticsearch.xpack.ql.expression.function.TwoOptionalArguments;
import org.elasticsearch.xpack.ql.expression.function.aggregate.EnclosedAgg;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataTypes;
import org.elasticsearch.xpack.sql.type.SqlDataTypeConverter;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xpack.ql.expression.TypeResolutions.isFoldable;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.isNumeric;
import static org.elasticsearch.xpack.sql.expression.function.aggregate.Percentile.resolvePercentileConfiguration;

public class PercentileRank extends NumericAggregate implements EnclosedAgg, TwoOptionalArguments, HasPercentileConfig {

    private final Expression value;
    private final Expression method;
    private final Expression methodParameter;

    public PercentileRank(Source source, Expression field, Expression value, Expression method, Expression methodParameter) {
        super(source, field, Collections.singletonList(value));
        this.value = value;
        this.method = method;
        this.methodParameter = methodParameter;
    }

    @Override
    protected NodeInfo<PercentileRank> info() {
        return NodeInfo.create(this, PercentileRank::new, field(), value, method, methodParameter);
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        if (children().size() < 2) {
            throw new IllegalArgumentException("expected at least [2] children but received [" + newChildren.size() + "]");
        }
        return new PercentileRank(source(), newChildren.get(0), newChildren.get(1), method, methodParameter);
    }

    @Override
    protected TypeResolution resolveType() {
        TypeResolution resolution = isFoldable(value, sourceText(), ParamOrdinal.SECOND);
        if (resolution.unresolved()) {
            return resolution;
        }

        resolution = super.resolveType();
        if (resolution.unresolved()) {
            return resolution;
        }

        resolution = resolvePercentileConfiguration(sourceText(), method, ParamOrdinal.THIRD, methodParameter, ParamOrdinal.FOURTH);
        if (resolution.unresolved()) {
            return resolution;
        }

        return isNumeric(value, sourceText(), ParamOrdinal.DEFAULT);
    }

    public Expression value() {
        return value;
    }

    @Override
    public Expression method() {
        return method;
    }

    @Override
    public Expression methodParameter() {
        return methodParameter;
    }

    @Override
    public String innerName() {
        Double doubleValue = (Double) SqlDataTypeConverter.convert(Foldables.valueOf(value), DataTypes.DOUBLE);
        return Double.toString(doubleValue);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        PercentileRank that = (PercentileRank) o;

        return Objects.equals(method, that.method)
            && Objects.equals(methodParameter, that.methodParameter);
    }

    @Override
    public int hashCode() {
        return Objects.hash(getClass(), children(), method, methodParameter);
    }
}
