/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.sql.expression.function.aggregate;

import org.elasticsearch.search.aggregations.metrics.PercentilesConfig;
import org.elasticsearch.search.aggregations.metrics.PercentilesMethod;
import org.elasticsearch.xpack.ql.QlIllegalArgumentException;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Expressions;
import org.elasticsearch.xpack.ql.expression.Foldables;
import org.elasticsearch.xpack.ql.expression.Literal;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;
import org.elasticsearch.xpack.sql.type.SqlDataTypeConverter;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.ql.expression.TypeResolutions.isEnum;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.isFoldable;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.isInteger;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.isNumeric;

public abstract class PercentileMethodConfiguration {

    private static final Set<String> ACCEPTED_METHODS =
        Arrays.stream(PercentilesMethod.values()).map(p -> p.getParseField().getPreferredName()).collect(Collectors.toSet());

    public static Expression defaultMethod(Source source, Expression method) {
        return method == null
            ? new Literal(source, PercentilesMethod.TDIGEST.getParseField().getPreferredName(), DataTypes.KEYWORD)
            : method;
    }

    public static Expression defaultMethodParameter(Expression methodParameter) {
        return nullIfUnspecified(methodParameter);
    }

    public static Expression.TypeResolution resolvePercentileConfiguration(
        String sourceText, Expression method, Expressions.ParamOrdinal methodOrdinal,
        Expression methodParameter, Expressions.ParamOrdinal methodParameterOrdinal) {

        Expression.TypeResolution resolution = isEnum(method, sourceText, methodOrdinal, ACCEPTED_METHODS);
        if (resolution.unresolved()) {
            return resolution;
        }

        if (methodParameter != null) {
            resolution = isFoldable(methodParameter, sourceText, methodParameterOrdinal);
            if (resolution.unresolved()) {
                return resolution;
            }

            PercentilesMethod percentilesMethod = asPercentilesMethod(method);
            switch (percentilesMethod) {
                case TDIGEST:
                    resolution = isNumeric(methodParameter, sourceText, methodParameterOrdinal);
                    break;
                case HDR:
                    resolution = isInteger(methodParameter, sourceText, methodParameterOrdinal);
                    break;
                default:
                    throw new QlIllegalArgumentException("Not handled PercentilesMethod [" + percentilesMethod + "]");
            }
            if (resolution.unresolved()) {
                return resolution;
            }
        }
        return resolution;
    }

    private static PercentilesMethod asPercentilesMethod(Expression method) {
        PercentilesMethod percentilesMethod = null;
        String methodName = foldOptionalNullable(method, DataTypes.KEYWORD);

        for (PercentilesMethod m : PercentilesMethod.values()) {
            if (m.getParseField().getPreferredName().equals(methodName)) {
                percentilesMethod = m;
            }
        }
        if (percentilesMethod == null) {
            throw new QlIllegalArgumentException("specified method [ " + methodName + " ] is not one of the allowed methods "
                + Arrays.toString(PercentilesMethod.values()));
        }
        return percentilesMethod;
    }

    public static PercentilesConfig asPercentileConfig(Expression method, Expression methodParameter) {
        PercentilesMethod percentilesMethod = asPercentilesMethod(method);
        switch (percentilesMethod) {
            case TDIGEST:
                Double compression = foldOptionalNullable(methodParameter, DataTypes.DOUBLE);
                return compression == null ? new PercentilesConfig.TDigest() : new PercentilesConfig.TDigest(compression);
            case HDR:
                Integer numOfDigits = foldOptionalNullable(methodParameter, DataTypes.INTEGER);
                return numOfDigits == null ? new PercentilesConfig.Hdr() : new PercentilesConfig.Hdr(numOfDigits);
            default:
                throw new QlIllegalArgumentException("Not handled PercentilesMethod [" + percentilesMethod + "]");
        }
    }

    @SuppressWarnings("unchecked")
    private static <T> T foldOptionalNullable(Expression e, DataType dataType) {
        if (e == null) {
            return (T) null;
        }
        return (T) SqlDataTypeConverter.convert(Foldables.valueOf(e), dataType);
    }

    private static Expression nullIfUnspecified(Expression e) {
        return e == null || (e.foldable() && e.fold() == null) ? null : e;
    }
}
