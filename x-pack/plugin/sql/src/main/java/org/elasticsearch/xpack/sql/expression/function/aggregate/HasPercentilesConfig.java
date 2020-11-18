/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.sql.expression.function.aggregate;

import org.elasticsearch.search.aggregations.metrics.PercentilesConfig;
import org.elasticsearch.search.aggregations.metrics.PercentilesMethod;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Expressions;
import org.elasticsearch.xpack.ql.expression.Foldables;
import org.elasticsearch.xpack.ql.expression.TypeResolutions;
import org.elasticsearch.xpack.ql.expression.function.TwoOptionalArguments;
import org.elasticsearch.xpack.ql.expression.function.aggregate.EnclosedAgg;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;
import org.elasticsearch.xpack.sql.type.SqlDataTypeConverter;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

import static java.util.Arrays.stream;
import static java.util.stream.Collectors.toMap;
import static org.elasticsearch.common.logging.LoggerMessageFormat.format;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.isFoldable;

public abstract class HasPercentilesConfig extends NumericAggregate implements EnclosedAgg, TwoOptionalArguments {
    
    private final Expression method;
    private final Expression methodParameter;
    
    public HasPercentilesConfig(Source source, Expression field, List<Expression> parameters, 
        Expression method, Expression methodParameter) 
    {
        super(source, field, parameters);
        this.method = method;
        this.methodParameter = methodParameter;
    }
    
    @FunctionalInterface
    interface MethodParameterResolver {
        Expression.TypeResolution resolve(Expression methodParameter, String sourceText, Expressions.ParamOrdinal methodParameterOrdinal);
    } 

    // extensive list of all the possible PercentilesMethods (all can be used with default method parameters)
    private static final Map<String, PercentilesMethod> NAME_TO_METHOD = stream(PercentilesMethod.values()).collect(toMap(
            pm -> pm.getParseField().getPreferredName(),
            pm -> pm
        ));
    
    // list of all the possible PercentileMethods that can we are capable of parameterizing as of now
    private static final Map<PercentilesMethod, MethodParameterResolver> METHOD_TO_RESOLVER = Map.of(
        PercentilesMethod.TDIGEST, TypeResolutions::isNumeric,
        PercentilesMethod.HDR, TypeResolutions::isInteger
    );

    private static Expression.TypeResolution resolvePercentileConfiguration(
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

            PercentilesMethod percentilesMethod = NAME_TO_METHOD.get(methodName);
            if (percentilesMethod == null) {
                return new Expression.TypeResolution(format(null, "{}argument of [{}] must be one of {}, received [{}]",
                    methodOrdinal.name().toLowerCase(Locale.ROOT) + " ", sourceText, NAME_TO_METHOD.keySet(), methodName));
            }

            // if method is null, the method parameter is not checked
            if (methodParameter != null && Expressions.isNull(methodParameter) == false) {
                resolution = isFoldable(methodParameter, sourceText, methodParameterOrdinal);
                if (resolution.unresolved()) {
                    return resolution;
                }
                
                MethodParameterResolver resolver = METHOD_TO_RESOLVER.get(percentilesMethod);
                if (resolver == null) {
                    // so in the future if a new method is added, at least the users will be able to use it with 
                    // the default parameters, but won't be able to configure it until the resolver is added
                    return new Expression.TypeResolution(format(null, 
                        "the [{}] method can only be used with the default method parameters, please omit the {} argument of [{}]",
                        methodParameterOrdinal.name().toLowerCase(Locale.ROOT), sourceText, methodName));
                }
                resolution = resolver.resolve(methodParameter, sourceText, methodParameterOrdinal);
                return resolution;
            }
        }

        return Expression.TypeResolution.TYPE_RESOLVED;
    }

    @Override protected TypeResolution resolveType() {
        TypeResolution resolution = super.resolveType();
        if (resolution.unresolved()) {
            return resolution;
        }

        return resolvePercentileConfiguration(sourceText(), method, Expressions.ParamOrdinal.fromIndex(parameters().size()+1), 
            methodParameter, Expressions.ParamOrdinal.fromIndex(parameters().size()+2));
    }

    public PercentilesConfig percentileConfig() {
        return asPercentileConfig(method, methodParameter);
    }

    private static PercentilesConfig asPercentileConfig(Expression method, Expression methodParameter) {
        if (method == null) {
            // sadly we had to set the default here, the PercentilesConfig does not provide a default
            return new PercentilesConfig.TDigest();
        }
        String methodName = foldOptionalNullable(method, DataTypes.KEYWORD);
        PercentilesMethod percentilesMethod = null;
        for (PercentilesMethod m : PercentilesMethod.values()) {
            if (m.getParseField().getPreferredName().equals(methodName)) {
                percentilesMethod = m;
                break;
            }
        }
        if (percentilesMethod == null) {
            throw new IllegalStateException("Not handled PercentilesMethod [" + methodName + "], type resolution needs fix");
        }
        switch (percentilesMethod) {
            case TDIGEST:
                Double compression = foldOptionalNullable(methodParameter, DataTypes.DOUBLE);
                return compression == null ? new PercentilesConfig.TDigest() : new PercentilesConfig.TDigest(compression);
            case HDR:
                Integer numOfDigits = foldOptionalNullable(methodParameter, DataTypes.INTEGER);
                return numOfDigits == null ? new PercentilesConfig.Hdr() : new PercentilesConfig.Hdr(numOfDigits);
            default:
                throw new IllegalStateException("Not handled PercentilesMethod [" + percentilesMethod + "], type resolution needs fix");
        }
    }

    @SuppressWarnings("unchecked")
    private static <T> T foldOptionalNullable(Expression e, DataType dataType) {
        if (e == null) {
            return null;
        }
        return (T) SqlDataTypeConverter.convert(Foldables.valueOf(e), dataType);
    }

    public Expression method() {
        return method;
    }
    
    public Expression methodParameter() {
        return methodParameter;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        HasPercentilesConfig that = (HasPercentilesConfig) o;

        return Objects.equals(method, that.method)
            && Objects.equals(methodParameter, that.methodParameter);
    }

    @Override
    public int hashCode() {
        return Objects.hash(getClass(), children(), method, methodParameter);
    }

}
