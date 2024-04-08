/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ql.expression;

import org.elasticsearch.xpack.ql.expression.Expression.TypeResolution;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;
import org.elasticsearch.xpack.ql.type.EsField;

import java.util.Locale;
import java.util.StringJoiner;
import java.util.function.Predicate;

import static org.elasticsearch.common.logging.LoggerMessageFormat.format;
import static org.elasticsearch.xpack.ql.expression.Expressions.name;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.ParamOrdinal.DEFAULT;
import static org.elasticsearch.xpack.ql.type.DataTypes.BOOLEAN;
import static org.elasticsearch.xpack.ql.type.DataTypes.DATETIME;
import static org.elasticsearch.xpack.ql.type.DataTypes.IP;
import static org.elasticsearch.xpack.ql.type.DataTypes.NULL;

public final class TypeResolutions {

    public enum ParamOrdinal {
        DEFAULT,
        FIRST,
        SECOND,
        THIRD,
        FOURTH,
        FIFTH;

        public static ParamOrdinal fromIndex(int index) {
            return switch (index) {
                case 0 -> FIRST;
                case 1 -> SECOND;
                case 2 -> THIRD;
                case 3 -> FOURTH;
                case 4 -> FIFTH;
                default -> DEFAULT;
            };
        }
    }

    private TypeResolutions() {}

    public static TypeResolution isBoolean(Expression e, String operationName, ParamOrdinal paramOrd) {
        return isType(e, dt -> dt == BOOLEAN, operationName, paramOrd, "boolean");
    }

    public static TypeResolution isInteger(Expression e, String operationName, ParamOrdinal paramOrd) {
        return isType(e, DataType::isInteger, operationName, paramOrd, "integer");
    }

    public static TypeResolution isNumeric(Expression e, String operationName, ParamOrdinal paramOrd) {
        return isType(e, DataType::isNumeric, operationName, paramOrd, "numeric");
    }

    public static TypeResolution isString(Expression e, String operationName, ParamOrdinal paramOrd) {
        return isType(e, DataTypes::isString, operationName, paramOrd, "string");
    }

    public static TypeResolution isIP(Expression e, String operationName, ParamOrdinal paramOrd) {
        return isType(e, dt -> dt == IP, operationName, paramOrd, "ip");
    }

    public static TypeResolution isDate(Expression e, String operationName, ParamOrdinal paramOrd) {
        return isType(e, dt -> dt == DATETIME, operationName, paramOrd, "datetime");
    }

    public static TypeResolution isExact(Expression e, String message) {
        if (e instanceof FieldAttribute fa) {
            EsField.Exact exact = fa.getExactInfo();
            if (exact.hasExact() == false) {
                return new TypeResolution(format(null, message, e.dataType().typeName(), exact.errorMsg()));
            }
        }
        return TypeResolution.TYPE_RESOLVED;
    }

    public static TypeResolution isExact(Expression e, String operationName, ParamOrdinal paramOrd) {
        if (e instanceof FieldAttribute fa) {
            EsField.Exact exact = fa.getExactInfo();
            if (exact.hasExact() == false) {
                return new TypeResolution(
                    format(
                        null,
                        "[{}] cannot operate on {}field of data type [{}]: {}",
                        operationName,
                        paramOrd == null || paramOrd == DEFAULT ? "" : paramOrd.name().toLowerCase(Locale.ROOT) + " argument ",
                        e.dataType().typeName(),
                        exact.errorMsg()
                    )
                );
            }
        }
        return TypeResolution.TYPE_RESOLVED;
    }

    public static TypeResolution isStringAndExact(Expression e, String operationName, ParamOrdinal paramOrd) {
        TypeResolution resolution = isString(e, operationName, paramOrd);
        if (resolution.unresolved()) {
            return resolution;
        }

        return isExact(e, operationName, paramOrd);
    }

    public static TypeResolution isIPAndExact(Expression e, String operationName, ParamOrdinal paramOrd) {
        TypeResolution resolution = isIP(e, operationName, paramOrd);
        if (resolution.unresolved()) {
            return resolution;
        }

        return isExact(e, operationName, paramOrd);
    }

    public static TypeResolution isFoldable(Expression e, String operationName, ParamOrdinal paramOrd) {
        if (e.foldable() == false) {
            return new TypeResolution(
                format(
                    null,
                    "{}argument of [{}] must be a constant, received [{}]",
                    paramOrd == null || paramOrd == DEFAULT ? "" : paramOrd.name().toLowerCase(Locale.ROOT) + " ",
                    operationName,
                    Expressions.name(e)
                )
            );
        }
        return TypeResolution.TYPE_RESOLVED;
    }

    public static TypeResolution isNotFoldable(Expression e, String operationName, ParamOrdinal paramOrd) {
        if (e.foldable()) {
            return new TypeResolution(
                format(
                    null,
                    "{}argument of [{}] must be a table column, found constant [{}]",
                    paramOrd == null || paramOrd == DEFAULT ? "" : paramOrd.name().toLowerCase(Locale.ROOT) + " ",
                    operationName,
                    Expressions.name(e)
                )
            );
        }
        return TypeResolution.TYPE_RESOLVED;
    }

    public static TypeResolution isType(
        Expression e,
        Predicate<DataType> predicate,
        String operationName,
        ParamOrdinal paramOrd,
        String... acceptedTypes
    ) {
        return predicate.test(e.dataType()) || e.dataType() == NULL
            ? TypeResolution.TYPE_RESOLVED
            : new TypeResolution(
                format(
                    null,
                    "{}argument of [{}] must be [{}], found value [{}] type [{}]",
                    paramOrd == null || paramOrd == DEFAULT ? "" : paramOrd.name().toLowerCase(Locale.ROOT) + " ",
                    operationName,
                    acceptedTypesForErrorMsg(acceptedTypes),
                    name(e),
                    e.dataType().typeName()
                )
            );
    }

    private static String acceptedTypesForErrorMsg(String... acceptedTypes) {
        StringJoiner sj = new StringJoiner(", ");
        for (int i = 0; i < acceptedTypes.length - 1; i++) {
            sj.add(acceptedTypes[i]);
        }
        if (acceptedTypes.length > 1) {
            return sj.toString() + " or " + acceptedTypes[acceptedTypes.length - 1];
        } else {
            return acceptedTypes[0];
        }
    }
}
