/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression;

import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.xpack.sql.type.DataType;
import org.elasticsearch.xpack.sql.type.DataTypes;

import java.util.Locale;
import java.util.StringJoiner;
import java.util.function.Predicate;

import static org.elasticsearch.common.logging.LoggerMessageFormat.format;
import static org.elasticsearch.xpack.sql.expression.Expression.TypeResolution;
import static org.elasticsearch.xpack.sql.expression.Expressions.ParamOrdinal;
import static org.elasticsearch.xpack.sql.expression.Expressions.name;
import static org.elasticsearch.xpack.sql.type.DataType.BOOLEAN;

public final class TypeResolutionUtils {

    private TypeResolutionUtils() {}

    public static TypeResolution typeMustBeBoolean(Expression e, String operationName, ParamOrdinal paramOrd) {
        return typeMustBe(e, dt -> dt == BOOLEAN, operationName, paramOrd, "boolean");
    }

    public static TypeResolution typeMustBeInteger(Expression e, String operationName, ParamOrdinal paramOrd) {
        return typeMustBe(e, DataType::isInteger, operationName, paramOrd, "integer");
    }

    public static TypeResolution typeMustBeNumeric(Expression e, String operationName, ParamOrdinal paramOrd) {
        return typeMustBe(e, DataType::isNumeric, operationName, paramOrd, "numeric");
    }

    public static TypeResolution typeMustBeString(Expression e, String operationName, ParamOrdinal paramOrd) {
        return typeMustBe(e, DataType::isString, operationName, paramOrd, "string");
    }

    public static TypeResolution typeMustBeDate(Expression e, String operationName, ParamOrdinal paramOrd) {
        return typeMustBe(e, DataType::isDateBased, operationName, paramOrd, "date", "datetime");
    }

    public static TypeResolution typeMustBeNumericOrDate(Expression e, String operationName, ParamOrdinal paramOrd) {
        return typeMustBe(e, dt -> dt.isNumeric() || dt.isDateBased(), operationName, paramOrd, "date", "datetime", "numeric");
    }

    public static TypeResolution typeMustBeExact(Expression e, String message) {
        if (e instanceof FieldAttribute) {
            Tuple<Boolean, String> hasExact = ((FieldAttribute) e).hasExact();
            if (hasExact.v1() == Boolean.FALSE) {
                return new TypeResolution(format(null, message, e.dataType().typeName, hasExact.v2()));
            }
        }
        return TypeResolution.TYPE_RESOLVED;
    }

    public static TypeResolution typeMustBeExact(Expression e, String operationName, ParamOrdinal paramOrd) {
        if (e instanceof FieldAttribute) {
            Tuple<Boolean, String> hasExact = ((FieldAttribute) e).hasExact();
            if (hasExact.v1() == Boolean.FALSE) {
                return new TypeResolution(format(null, "[{}] cannot operate on {}field of data type [{}]: {}",
                    operationName,
                    paramOrd == null || paramOrd == ParamOrdinal.DEFAULT ?
                        "" : paramOrd.name().toLowerCase(Locale.ROOT) + " argument ",
                    e.dataType().typeName, hasExact.v2()));
            }
        }
        return TypeResolution.TYPE_RESOLVED;
    }

    public static TypeResolution typeMustBeStringAndExact(Expression e, String operationName, ParamOrdinal paramOrd) {
        TypeResolution resolution = typeMustBe(e, DataType::isString, operationName, paramOrd, "string");
        if (resolution.unresolved()) {
            return resolution;
        }

        return typeMustBeExact(e, operationName, paramOrd);
    }

    public static TypeResolution expressionMustBeConstant(Expression e, String operationName, ParamOrdinal paramOrd) {
        if (!e.foldable()) {
            return new TypeResolution(format(null, "{}argument of [{}] must be a constant, received [{}]",
                paramOrd == null || paramOrd == ParamOrdinal.DEFAULT ? "" : paramOrd.name().toLowerCase(Locale.ROOT) + " ",
                operationName,
                Expressions.name(e)));
        }
        return TypeResolution.TYPE_RESOLVED;
    }

    public static TypeResolution expressionMustBeTableColumn(Expression e, String operationName, ParamOrdinal paramOrd) {
        if (e.foldable()) {
            return new TypeResolution(format(null, "{}argument of [{}] must be a table column, found constant [{}]",
                paramOrd == null || paramOrd == ParamOrdinal.DEFAULT ? "" : paramOrd.name().toLowerCase(Locale.ROOT) + " ",
                operationName,
                Expressions.name(e)));
        }
        return TypeResolution.TYPE_RESOLVED;
    }

    public static TypeResolution typeMustBe(Expression e,
                                                       Predicate<DataType> predicate,
                                                       String operationName,
                                                       ParamOrdinal paramOrd,
                                                       String... acceptedTypes) {
        return predicate.test(e.dataType()) || DataTypes.isNull(e.dataType())?
            TypeResolution.TYPE_RESOLVED :
            new TypeResolution(format(null, "{}argument of [{}] must be [{}], found value [{}] type [{}]",
                paramOrd == null || paramOrd == ParamOrdinal.DEFAULT ? "" : paramOrd.name().toLowerCase(Locale.ROOT) + " ",
                operationName,
                acceptedTypesForErrorMsg(acceptedTypes),
                name(e),
                e.dataType().typeName));
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
