/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression;

import org.elasticsearch.xpack.sql.type.DataType;
import org.elasticsearch.xpack.sql.type.DataTypes;
import org.elasticsearch.xpack.sql.type.EsField;

import java.util.Locale;
import java.util.StringJoiner;
import java.util.function.Predicate;

import static org.elasticsearch.common.logging.LoggerMessageFormat.format;
import static org.elasticsearch.xpack.sql.expression.Expression.TypeResolution;
import static org.elasticsearch.xpack.sql.expression.Expressions.ParamOrdinal;
import static org.elasticsearch.xpack.sql.expression.Expressions.name;
import static org.elasticsearch.xpack.sql.type.DataType.BOOLEAN;

public final class TypeResolutions {

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
        return isType(e, DataType::isString, operationName, paramOrd, "string");
    }

    public static TypeResolution isDate(Expression e, String operationName, ParamOrdinal paramOrd) {
        return isType(e, DataType::isDateBased, operationName, paramOrd, "date", "datetime");
    }

    public static TypeResolution isDateOrTime(Expression e, String operationName, ParamOrdinal paramOrd) {
        return isType(e, DataType::isDateOrTimeBased, operationName, paramOrd, "date", "time", "datetime");
    }

    public static TypeResolution isNumericOrDate(Expression e, String operationName, ParamOrdinal paramOrd) {
        return isType(e, dt -> dt.isNumeric() || dt.isDateBased(), operationName, paramOrd,
            "date", "datetime", "numeric");
    }

    public static TypeResolution isNumericOrDateOrTime(Expression e, String operationName, ParamOrdinal paramOrd) {
        return isType(e, dt -> dt.isNumeric() || dt.isDateOrTimeBased(), operationName, paramOrd,
            "date", "time", "datetime", "numeric");
    }


    public static TypeResolution isGeo(Expression e, String operationName, ParamOrdinal paramOrd) {
        return isType(e, DataType::isGeo, operationName, paramOrd, "geo_point", "geo_shape");
    }

    public static TypeResolution isExact(Expression e, String message) {
        if (e instanceof FieldAttribute) {
            EsField.Exact exact = ((FieldAttribute) e).getExactInfo();
            if (exact.hasExact() == false) {
                return new TypeResolution(format(null, message, e.dataType().typeName, exact.errorMsg()));
            }
        }
        return TypeResolution.TYPE_RESOLVED;
    }

    public static TypeResolution isExact(Expression e, String operationName, ParamOrdinal paramOrd) {
        if (e instanceof FieldAttribute) {
            EsField.Exact exact = ((FieldAttribute) e).getExactInfo();
            if (exact.hasExact() == false) {
                return new TypeResolution(format(null, "[{}] cannot operate on {}field of data type [{}]: {}",
                    operationName,
                    paramOrd == null || paramOrd == ParamOrdinal.DEFAULT ?
                        "" : paramOrd.name().toLowerCase(Locale.ROOT) + " argument ",
                    e.dataType().typeName, exact.errorMsg()));
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

    public static TypeResolution isFoldable(Expression e, String operationName, ParamOrdinal paramOrd) {
        if (!e.foldable()) {
            return new TypeResolution(format(null, "{}argument of [{}] must be a constant, received [{}]",
                paramOrd == null || paramOrd == ParamOrdinal.DEFAULT ? "" : paramOrd.name().toLowerCase(Locale.ROOT) + " ",
                operationName,
                Expressions.name(e)));
        }
        return TypeResolution.TYPE_RESOLVED;
    }

    public static TypeResolution isNotFoldable(Expression e, String operationName, ParamOrdinal paramOrd) {
        if (e.foldable()) {
            return new TypeResolution(format(null, "{}argument of [{}] must be a table column, found constant [{}]",
                paramOrd == null || paramOrd == ParamOrdinal.DEFAULT ? "" : paramOrd.name().toLowerCase(Locale.ROOT) + " ",
                operationName,
                Expressions.name(e)));
        }
        return TypeResolution.TYPE_RESOLVED;
    }

    public static TypeResolution isType(Expression e,
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
