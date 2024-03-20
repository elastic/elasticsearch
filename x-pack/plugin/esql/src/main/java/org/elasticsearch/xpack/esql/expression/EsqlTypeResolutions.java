/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression;

import org.elasticsearch.xpack.esql.type.EsqlDataTypes;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.FieldAttribute;
import org.elasticsearch.xpack.ql.expression.TypeResolutions;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;
import org.elasticsearch.xpack.ql.type.EsField;

import java.util.Locale;

import static org.elasticsearch.common.logging.LoggerMessageFormat.format;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypes.CARTESIAN_POINT;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypes.CARTESIAN_SHAPE;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypes.GEO_POINT;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypes.GEO_SHAPE;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.ParamOrdinal.DEFAULT;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.isType;

public class EsqlTypeResolutions {

    public static Expression.TypeResolution isExact(Expression e, String operationName, TypeResolutions.ParamOrdinal paramOrd) {
        if (e instanceof FieldAttribute fa) {
            if (DataTypes.isString(fa.dataType())) {
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
    private static final String[] NON_SPATIAL_TYPE_NAMES = EsqlDataTypes.types()
        .stream()
        .filter(EsqlDataTypes::isRepresentable)
        .filter(t -> EsqlDataTypes.isSpatial(t) == false)
        .map(DataType::esType)
        .toArray(String[]::new);

    public static Expression.TypeResolution isSpatialPoint(Expression e, String operationName, TypeResolutions.ParamOrdinal paramOrd) {
        return isType(e, EsqlDataTypes::isSpatialPoint, operationName, paramOrd, POINT_TYPE_NAMES);
    }

    public static Expression.TypeResolution isSpatial(Expression e, String operationName, TypeResolutions.ParamOrdinal paramOrd) {
        return isType(e, EsqlDataTypes::isSpatial, operationName, paramOrd, SPATIAL_TYPE_NAMES);
    }

    public static Expression.TypeResolution isNotSpatial(Expression e, String operationName, TypeResolutions.ParamOrdinal paramOrd) {
        return isType(e, t -> EsqlDataTypes.isSpatial(t) == false, operationName, paramOrd, NON_SPATIAL_TYPE_NAMES);
    }

}
