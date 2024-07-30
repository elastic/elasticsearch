/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.TypeResolutions;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;

import java.util.Locale;

import static org.elasticsearch.common.logging.LoggerMessageFormat.format;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.DEFAULT;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isType;
import static org.elasticsearch.xpack.esql.core.type.DataType.CARTESIAN_POINT;
import static org.elasticsearch.xpack.esql.core.type.DataType.CARTESIAN_SHAPE;
import static org.elasticsearch.xpack.esql.core.type.DataType.GEO_POINT;
import static org.elasticsearch.xpack.esql.core.type.DataType.GEO_SHAPE;

public class EsqlTypeResolutions {

    public static Expression.TypeResolution isStringAndExact(Expression e, String operationName, TypeResolutions.ParamOrdinal paramOrd) {
        Expression.TypeResolution resolution = TypeResolutions.isString(e, operationName, paramOrd);
        if (resolution.unresolved()) {
            return resolution;
        }

        return isExact(e, operationName, paramOrd);
    }

    public static Expression.TypeResolution isExact(Expression e, String operationName, TypeResolutions.ParamOrdinal paramOrd) {
        if (e instanceof FieldAttribute fa) {
            if (DataType.isString(fa.dataType())) {
                // ESQL can extract exact values for TEXT fields
                return Expression.TypeResolution.TYPE_RESOLVED;
            }
            EsField.Exact exact = fa.getExactInfo();
            if (exact.hasExact() == false) {
                return new Expression.TypeResolution(
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
        return Expression.TypeResolution.TYPE_RESOLVED;
    }

    private static final String[] SPATIAL_TYPE_NAMES = new String[] {
        GEO_POINT.typeName(),
        CARTESIAN_POINT.typeName(),
        GEO_SHAPE.typeName(),
        CARTESIAN_SHAPE.typeName() };
    private static final String[] POINT_TYPE_NAMES = new String[] { GEO_POINT.typeName(), CARTESIAN_POINT.typeName() };
    private static final String[] NON_SPATIAL_TYPE_NAMES = DataType.types()
        .stream()
        .filter(DataType::isRepresentable)
        .filter(t -> DataType.isSpatial(t) == false)
        .map(DataType::esType)
        .toArray(String[]::new);

    public static Expression.TypeResolution isSpatialPoint(Expression e, String operationName, TypeResolutions.ParamOrdinal paramOrd) {
        return isType(e, DataType::isSpatialPoint, operationName, paramOrd, POINT_TYPE_NAMES);
    }

    public static Expression.TypeResolution isSpatial(Expression e, String operationName, TypeResolutions.ParamOrdinal paramOrd) {
        return isType(e, DataType::isSpatial, operationName, paramOrd, SPATIAL_TYPE_NAMES);
    }

    public static Expression.TypeResolution isNotSpatial(Expression e, String operationName, TypeResolutions.ParamOrdinal paramOrd) {
        return isType(e, t -> DataType.isSpatial(t) == false, operationName, paramOrd, NON_SPATIAL_TYPE_NAMES);
    }

}
