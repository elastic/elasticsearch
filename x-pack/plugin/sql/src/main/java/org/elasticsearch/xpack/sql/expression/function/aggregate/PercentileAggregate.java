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
import org.elasticsearch.xpack.ql.expression.Expressions.ParamOrdinal;
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
import java.util.function.Function;

import static java.util.stream.Collectors.toMap;
import static org.elasticsearch.common.logging.LoggerMessageFormat.format;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.isFoldable;

abstract class PercentileAggregate extends NumericAggregate implements EnclosedAgg, TwoOptionalArguments {

    private static final PercentilesConfig.TDigest DEFAULT_PERCENTILES_CONFIG = new PercentilesConfig.TDigest();
    
    // all the possible PercentileMethods that we are capable of parameterizing at the moment
    private static final Map<String, MethodConfigurator> METHODNAME_TO_CONFIGURATOR = List.of(
        new MethodConfigurator(PercentilesMethod.TDIGEST, TypeResolutions::isNumeric, methodParameter -> {
            Double compression = foldNullSafe(methodParameter, DataTypes.DOUBLE);
            return compression == null ? new PercentilesConfig.TDigest() : new PercentilesConfig.TDigest(compression);
        }),
        new MethodConfigurator(PercentilesMethod.HDR, TypeResolutions::isInteger, methodParameter -> {
            Integer numOfDigits = foldNullSafe(methodParameter, DataTypes.INTEGER);
            return numOfDigits == null ? new PercentilesConfig.Hdr() : new PercentilesConfig.Hdr(numOfDigits);
        })
    ).stream().collect(toMap(
        c -> c.method.getParseField().getPreferredName(),
        Function.identity()
    ));
    
    private static class MethodConfigurator {

        @FunctionalInterface
        private interface MethodParameterResolver {
            TypeResolution resolve(Expression methodParameter, String sourceText, ParamOrdinal methodParameterOrdinal);
        }
        
        private final PercentilesMethod method;
        private final MethodParameterResolver resolver;
        private final Function<Expression, PercentilesConfig> parameterToConfig;

        MethodConfigurator(PercentilesMethod method, MethodParameterResolver resolver, 
            Function<Expression, PercentilesConfig> parameterToConfig) {
            this.method = method;
            this.resolver = resolver;
            this.parameterToConfig = parameterToConfig;
        }
    }
    
    private final Expression method;
    private final Expression methodParameter;
    
    PercentileAggregate(Source source, Expression field, List<Expression> parameters, Expression method, Expression methodParameter) 
    {
        super(source, field, parameters);
        this.method = method;
        this.methodParameter = methodParameter;
    }

    @Override 
    protected TypeResolution resolveType() {
        TypeResolution resolution = super.resolveType();
        if (resolution.unresolved()) {
            return resolution;
        }

        return resolvePercentileConfiguration(sourceText(), method, ParamOrdinal.fromIndex(parameters().size() + 1),
            methodParameter, ParamOrdinal.fromIndex(parameters().size() + 2));
    }

    public Expression method() {
        return method;
    }

    public Expression methodParameter() {
        return methodParameter;
    }

    public PercentilesConfig percentilesConfig() {
        return asPercentilesConfig(method, methodParameter);
    }

    private static TypeResolution resolvePercentileConfiguration(
        String sourceText, Expression method, ParamOrdinal methodOrdinal,
        Expression methodParameter, ParamOrdinal methodParameterOrdinal) {

        if (method != null) {
            TypeResolution resolution = isFoldable(method, sourceText, methodOrdinal);
            if (resolution.unresolved()) {
                return resolution;
            }
            resolution = TypeResolutions.isString(method, sourceText, methodOrdinal);
            if (resolution.unresolved()) {
                return resolution;
            }

            String methodName = (String) method.fold();

            MethodConfigurator methodConfigurator = METHODNAME_TO_CONFIGURATOR.get(methodName);
            if (methodConfigurator == null) {
                return new TypeResolution(format(null, "{}argument of [{}] must be one of {}, received [{}]",
                    methodOrdinal.name().toLowerCase(Locale.ROOT) + " ", sourceText, 
                    METHODNAME_TO_CONFIGURATOR.keySet(), methodName));
            }

            // if method is null, the method parameter is not checked
            if (methodParameter != null && Expressions.isNull(methodParameter) == false) {
                resolution = isFoldable(methodParameter, sourceText, methodParameterOrdinal);
                if (resolution.unresolved()) {
                    return resolution;
                }
                
                resolution = methodConfigurator.resolver.resolve(methodParameter, sourceText, methodParameterOrdinal);
                return resolution;
            }
        }

        return TypeResolution.TYPE_RESOLVED;
    }

    private static PercentilesConfig asPercentilesConfig(Expression method, Expression methodParameter) {
        if (method == null) {
            // sadly we had to set the default here, the PercentilesConfig does not provide a default
            return DEFAULT_PERCENTILES_CONFIG;
        }
        String methodName = foldNullSafe(method, DataTypes.KEYWORD);
        MethodConfigurator methodConfigurator = METHODNAME_TO_CONFIGURATOR.get(methodName);
        if (methodConfigurator == null) {
            throw new IllegalStateException("Not handled PercentilesMethod [" + methodName + "], type resolution needs fix");
        }
        return methodConfigurator.parameterToConfig.apply(methodParameter);
    }

    @SuppressWarnings("unchecked")
    private static <T> T foldNullSafe(Expression e, DataType dataType) {
        return e == null ? null : (T) SqlDataTypeConverter.convert(Foldables.valueOf(e), dataType);
    }

    @Override
    public DataType dataType() {
        return DataTypes.DOUBLE;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        
        if (!super.equals(o)) {
            return false;
        }

        PercentileAggregate that = (PercentileAggregate) o;

        return Objects.equals(method, that.method)
            && Objects.equals(methodParameter, that.methodParameter);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), method, methodParameter);
    }
}
