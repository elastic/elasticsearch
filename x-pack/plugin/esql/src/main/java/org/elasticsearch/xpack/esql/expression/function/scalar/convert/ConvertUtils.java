/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.convert;

import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;

import java.util.Map;
import java.util.function.BiFunction;

import static java.util.Map.entry;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypes.CARTESIAN_POINT;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypes.CARTESIAN_SHAPE;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypes.GEO_POINT;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypes.GEO_SHAPE;
import static org.elasticsearch.xpack.ql.type.DataTypes.BOOLEAN;
import static org.elasticsearch.xpack.ql.type.DataTypes.DATETIME;
import static org.elasticsearch.xpack.ql.type.DataTypes.DOUBLE;
import static org.elasticsearch.xpack.ql.type.DataTypes.INTEGER;
import static org.elasticsearch.xpack.ql.type.DataTypes.IP;
import static org.elasticsearch.xpack.ql.type.DataTypes.KEYWORD;
import static org.elasticsearch.xpack.ql.type.DataTypes.LONG;
import static org.elasticsearch.xpack.ql.type.DataTypes.TEXT;
import static org.elasticsearch.xpack.ql.type.DataTypes.UNSIGNED_LONG;
import static org.elasticsearch.xpack.ql.type.DataTypes.VERSION;

public final class ConvertUtils {

    private static final Map<DataType, BiFunction<Source, Expression, AbstractConvertFunction>> TYPE_TO_CONVERTER_MAP = Map.ofEntries(
        entry(BOOLEAN, ToBoolean::new),
        entry(CARTESIAN_POINT, ToCartesianPoint::new),
        entry(CARTESIAN_SHAPE, ToCartesianShape::new),
        entry(DATETIME, ToDatetime::new),
        // ToDegrees, typeless
        entry(DOUBLE, ToDouble::new),
        entry(GEO_POINT, ToGeoPoint::new),
        entry(GEO_SHAPE, ToGeoShape::new),
        entry(INTEGER, ToInteger::new),
        entry(IP, ToIP::new),
        entry(LONG, ToLong::new),
        // ToRadians, typeless
        entry(KEYWORD, ToString::new),
        entry(TEXT, ToString::new),
        entry(UNSIGNED_LONG, ToUnsignedLong::new),
        entry(VERSION, ToVersion::new)
    );

    public static BiFunction<Source, Expression, AbstractConvertFunction> converterFactory(DataType toType) {
        return TYPE_TO_CONVERTER_MAP.get(toType);
    }
}
