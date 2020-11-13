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
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;
import org.elasticsearch.xpack.sql.type.SqlDataTypeConverter;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.xpack.ql.expression.TypeResolutions.isFoldable;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.isNumeric;
import static org.elasticsearch.xpack.ql.expression.function.Functions.countOfNonNullOptionalArgs;
import static org.elasticsearch.xpack.sql.expression.function.aggregate.PercentileMethodConfiguration.defaultMethod;
import static org.elasticsearch.xpack.sql.expression.function.aggregate.PercentileMethodConfiguration.defaultMethodParameter;
import static org.elasticsearch.xpack.sql.expression.function.aggregate.PercentileMethodConfiguration.resolvePercentileConfiguration;

public class Percentile extends NumericAggregate implements EnclosedAgg, TwoOptionalArguments {

    private final Expression percent;
    private final Expression method;
    private final Expression methodParameter;

    public Percentile(Source source, Expression field, Expression percent, Expression method, Expression methodParameter) {
        super(source, field, Stream.of(percent, (method = defaultMethod(source, method)),
            (methodParameter = defaultMethodParameter(methodParameter))).filter(Objects::nonNull).collect(Collectors.toList()));
        this.percent = percent;
        this.method = method;
        this.methodParameter = methodParameter;
    }

    @Override
    protected NodeInfo<Percentile> info() {
        return NodeInfo.create(this, Percentile::new, field(), percent, method, methodParameter);
    }

    @Override
    public Percentile replaceChildren(List<Expression> newChildren) {
        if (children().size() != newChildren.size()) {
            throw new IllegalArgumentException("expected [" + children().size() + "] children but received [" + newChildren.size() + "]");
        }
        return new Percentile(source(), newChildren.get(0), newChildren.get(1),
            method == null ? null : newChildren.get(2),
            methodParameter == null ? null : newChildren.get(2 + countOfNonNullOptionalArgs(method)));
    }

    @Override
    protected TypeResolution resolveType() {
        TypeResolution resolution = isFoldable(percent, sourceText(), ParamOrdinal.SECOND);
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

        return isNumeric(percent, sourceText(), ParamOrdinal.DEFAULT);
    }

    public Expression percent() {
        return percent;
    }

    public Expression method() {
        return method;
    }

    public Expression methodParameter() {
        return methodParameter;
    }

    @Override
    public DataType dataType() {
        return DataTypes.DOUBLE;
    }

    @Override
    public String innerName() {
        Double value = (Double) SqlDataTypeConverter.convert(Foldables.valueOf(percent), DataTypes.DOUBLE);
        return Double.toString(value);
    }
}
