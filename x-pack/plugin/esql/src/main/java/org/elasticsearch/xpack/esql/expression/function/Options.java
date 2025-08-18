/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.xpack.esql.core.InvalidArgumentException;
import org.elasticsearch.xpack.esql.core.expression.EntryExpression;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.MapExpression;
import org.elasticsearch.xpack.esql.core.expression.TypeResolutions;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.DataTypeConverter;

import java.util.HashMap;
import java.util.Map;
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
        return resolve(options, source, paramOrdinal, allowedOptions, null);
    }

    public static Expression.TypeResolution resolve(
        Expression options,
        Source source,
        TypeResolutions.ParamOrdinal paramOrdinal,
        Map<String, DataType> allowedOptions,
        Consumer<Map<String, Object>> verifyOptions
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
                populateMap((MapExpression) options, optionsMap, source, paramOrdinal, allowedOptions);
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
            Expression.TypeResolution resolution = isFoldable(optionExpr, source.text(), paramOrdinal).and(
                isFoldable(valueExpr, source.text(), paramOrdinal)
            );
            if (resolution.unresolved()) {
                throw new InvalidArgumentException(resolution.message());
            }
            Object optionExprLiteral = ((Literal) optionExpr).value();
            Object valueExprLiteral = ((Literal) valueExpr).value();
            String optionName = optionExprLiteral instanceof BytesRef br ? br.utf8ToString() : optionExprLiteral.toString();
            String optionValue = valueExprLiteral instanceof BytesRef br ? br.utf8ToString() : valueExprLiteral.toString();
            // validate the optionExpr is supported
            DataType dataType = allowedOptions.get(optionName);
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
}
