/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.aggregate;

import org.elasticsearch.search.aggregations.metrics.PercentilesMethod;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Expressions;
import org.elasticsearch.xpack.ql.expression.Expressions.ParamOrdinal;
import org.elasticsearch.xpack.ql.expression.Foldables;
import org.elasticsearch.xpack.ql.expression.TypeResolutions;
import org.elasticsearch.xpack.ql.expression.function.TwoOptionalArguments;
import org.elasticsearch.xpack.ql.expression.function.aggregate.EnclosedAgg;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;
import org.elasticsearch.xpack.sql.type.SqlDataTypeConverter;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.common.logging.LoggerMessageFormat.format;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.isFoldable;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.isInteger;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.isNumeric;

public class Percentile extends NumericAggregate implements EnclosedAgg, TwoOptionalArguments, HasPercentileConfig {

    private final Expression percent;
    private final Expression method;
    private final Expression methodParameter;

    public Percentile(Source source, Expression field, Expression percent, Expression method, Expression methodParameter) {
        super(source, field, Collections.singletonList(percent));
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
        if (children().size() != 2) {
            throw new IllegalArgumentException("expected [2] children but received [" + newChildren.size() + "]");
        }
        return new Percentile(source(), newChildren.get(0), newChildren.get(1), method, methodParameter);
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

    @Override
    public Expression method() {
        return method;
    }

    @Override
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

    private static final Set<String> ACCEPTED_METHODS =
        Arrays.stream(PercentilesMethod.values()).map(pm -> pm.getParseField().getPreferredName()).collect(Collectors.toSet());

    static Expression.TypeResolution resolvePercentileConfiguration(
        String sourceText, Expression method, Expressions.ParamOrdinal methodOrdinal,
        Expression methodParameter, Expressions.ParamOrdinal methodParameterOrdinal) {

        if (method != null) {
            Expression.TypeResolution resolution = isFoldable(method, sourceText, methodOrdinal);
            if (resolution.unresolved()) {
                return resolution;
            }
            resolution = TypeResolutions.isString(method, sourceText, methodOrdinal);
            if (resolution.unresolved()) {
                return resolution;
            }

            String methodName = (String) method.fold();

            if (ACCEPTED_METHODS.contains(methodName) == false) {
                return new Expression.TypeResolution(format(
                    null,
                    "{}argument of [{}] must be one of {}, received [{}]",
                    methodOrdinal.name().toLowerCase(Locale.ROOT) + " ",
                    sourceText,
                    Stream.of(PercentilesMethod.values()).map(p -> p.getParseField().getPreferredName()).collect(Collectors.toList()),
                    methodName));
            }

            // if method is null, the method parameter is not checked
            if (methodParameter != null) {
                resolution = isFoldable(methodParameter, sourceText, methodParameterOrdinal);
                if (resolution.unresolved()) {
                    return resolution;
                }

                if (PercentilesMethod.TDIGEST.getParseField().getPreferredName().equals(methodName)) {
                    resolution = isNumeric(methodParameter, sourceText, methodParameterOrdinal);
                } else if (PercentilesMethod.HDR.getParseField().getPreferredName().equals(methodName)) {
                    resolution = isInteger(methodParameter, sourceText, methodParameterOrdinal);
                } else {
                    throw new IllegalStateException("Not handled PercentilesMethod [" + methodName + "], type resolution needs fix");
                }
                if (resolution.unresolved()) {
                    return resolution;
                }
                return resolution;
            }
        }

        return Expression.TypeResolution.TYPE_RESOLVED;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        Percentile that = (Percentile) o;

        return Objects.equals(method, that.method)
            && Objects.equals(methodParameter, that.methodParameter);
    }

    @Override
    public int hashCode() {
        return Objects.hash(getClass(), children(), method, methodParameter);
    }
}
