/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function;

import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.xpack.esql.core.InvalidArgumentException;
import org.elasticsearch.xpack.esql.core.expression.EntryExpression;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.MapExpression;
import org.elasticsearch.xpack.esql.core.expression.TypeResolutions;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.DataTypeConverter;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static org.elasticsearch.common.logging.LoggerMessageFormat.format;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isFoldable;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isMapExpression;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isNotNull;

public class Options {

    public static Expression.TypeResolution resolve(
        Expression options,
        Source source,
        TypeResolutions.ParamOrdinal paramOrdinal,
        Map<String, DataType> allowedOptions
    ) {
        return resolve(
            options,
            source,
            paramOrdinal,
            null,
            (opts, optsMap) -> populateMap(opts, optsMap, source, paramOrdinal, allowedOptions)
        );
    }

    public static Expression.TypeResolution resolve(
        Expression options,
        Source source,
        TypeResolutions.ParamOrdinal paramOrdinal,
        Map<String, DataType> allowedOptions,
        Consumer<Map<String, Object>> verifyOptions
    ) {
        return resolve(
            options,
            source,
            paramOrdinal,
            verifyOptions,
            (opts, optsMap) -> populateMap(opts, optsMap, source, paramOrdinal, allowedOptions)
        );
    }

    public static Expression.TypeResolution resolveWithMultipleDataTypesAllowed(
        Expression options,
        Source source,
        TypeResolutions.ParamOrdinal paramOrdinal,
        Map<String, Collection<DataType>> allowedOptions
    ) {
        return resolve(
            options,
            source,
            paramOrdinal,
            null,
            (opts, optsMap) -> populateMapWithExpressionsMultipleDataTypesAllowed(opts, optsMap, source, paramOrdinal, allowedOptions)
        );
    }

    private static Expression.TypeResolution resolve(
        Expression options,
        Source source,
        TypeResolutions.ParamOrdinal paramOrdinal,
        Consumer<Map<String, Object>> verifyOptions,
        BiConsumer<MapExpression, Map<String, Object>> populateMap
    ) {
        if (options != null) {
            Expression.TypeResolution resolution = isNotNull(options, source.text(), paramOrdinal);
            if (resolution.unresolved()) {
                return resolution;
            }
            // MapExpression does not have a DataType associated with it
            resolution = isMapExpression(options, source.text(), paramOrdinal);
            if (resolution.unresolved()) {
                return resolution;
            }
            try {
                Map<String, Object> optionsMap = new HashMap<>();
                populateMap.accept((MapExpression) options, optionsMap);
                if (verifyOptions != null) {
                    verifyOptions.accept(optionsMap);
                }
            } catch (InvalidArgumentException e) {
                return new Expression.TypeResolution(e.getMessage());
            }
        }
        return Expression.TypeResolution.TYPE_RESOLVED;
    }

    public static void populateMap(
        final MapExpression options,
        final Map<String, Object> optionsMap,
        final Source source,
        final TypeResolutions.ParamOrdinal paramOrdinal,
        final Map<String, DataType> allowedOptions
    ) throws InvalidArgumentException {
        for (EntryExpression entry : options.entryExpressions()) {
            Expression optionExpr = entry.key();
            Expression valueExpr = entry.value();

            Expression.TypeResolution optionNameResolution = isFoldable(optionExpr, source.text(), paramOrdinal);
            if (optionNameResolution.unresolved()) {
                throw new InvalidArgumentException(optionNameResolution.message());
            }

            Object optionExprLiteral = ((Literal) optionExpr).value();
            String optionName = BytesRefs.toString(optionExprLiteral);
            DataType dataType = allowedOptions.get(optionName);

            // valueExpr could be a MapExpression, but for now functions only accept literal values in options
            if ((valueExpr instanceof Literal) == false) {
                throw new InvalidArgumentException(
                    format(null, "Invalid option [{}] in [{}], expected a [{}] value", optionName, source.text(), dataType)
                );
            }

            Object valueExprLiteral = ((Literal) valueExpr).value();
            String optionValue = BytesRefs.toString(valueExprLiteral);
            // validate the optionExpr is supported
            if (dataType == null) {
                throw new InvalidArgumentException(
                    format(null, "Invalid option [{}] in [{}], expected one of {}", optionName, source.text(), allowedOptions.keySet())
                );
            }
            try {
                optionsMap.put(optionName, DataTypeConverter.convert(optionValue, dataType));
            } catch (InvalidArgumentException e) {
                throw new InvalidArgumentException(
                    format(null, "Invalid option [{}] in [{}], {}", optionName, source.text(), e.getMessage())
                );
            }
        }
    }

    public static void populateMapWithExpressionsMultipleDataTypesAllowed(
        final MapExpression options,
        final Map<String, Object> optionsMap,
        final Source source,
        final TypeResolutions.ParamOrdinal paramOrdinal,
        final Map<String, Collection<DataType>> allowedOptions
    ) throws InvalidArgumentException {
        if (options == null) {
            return;
        }

        for (EntryExpression entry : options.entryExpressions()) {
            Expression optionExpr = entry.key();
            Expression valueExpr = entry.value();

            Expression.TypeResolution optionNameResolution = isFoldable(optionExpr, source.text(), paramOrdinal);
            if (optionNameResolution.unresolved()) {
                throw new InvalidArgumentException(optionNameResolution.message());
            }

            Object optionExprLiteral = ((Literal) optionExpr).value();
            String optionName = BytesRefs.toString(optionExprLiteral);
            Collection<DataType> allowedDataTypes = allowedOptions.get(optionName);

            // valueExpr could be a MapExpression, but for now functions only accept literal values in options
            if ((valueExpr instanceof Literal) == false) {
                throw new InvalidArgumentException(
                    format(null, "Invalid option [{}] in [{}], expected a [{}] value", optionName, source.text(), allowedDataTypes)
                );
            }

            // validate the optionExpr is supported
            if (allowedDataTypes == null || allowedDataTypes.isEmpty()) {
                throw new InvalidArgumentException(
                    format(null, "Invalid option [{}] in [{}], expected one of {}", optionName, source.text(), allowedOptions.keySet())
                );
            }

            Literal valueExprLiteral = ((Literal) valueExpr);
            // validate that the literal has one of the allowed data types
            if (allowedDataTypes.contains(valueExprLiteral.dataType()) == false) {
                throw new InvalidArgumentException(
                    format(null, "Invalid option [{}] in [{}], allowed types [{}]", optionName, source.text(), allowedDataTypes)
                );
            }

            optionsMap.put(optionName, valueExprLiteral);
        }
    }
}
