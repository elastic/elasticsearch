/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.type;

import org.elasticsearch.xpack.esql.core.type.DataType;

import java.util.Collections;
import java.util.Locale;
import java.util.Map;

import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toUnmodifiableMap;
import static org.elasticsearch.xpack.esql.core.type.DataType.BYTE;
import static org.elasticsearch.xpack.esql.core.type.DataType.DATE_PERIOD;
import static org.elasticsearch.xpack.esql.core.type.DataType.FLOAT;
import static org.elasticsearch.xpack.esql.core.type.DataType.HALF_FLOAT;
import static org.elasticsearch.xpack.esql.core.type.DataType.KEYWORD;
import static org.elasticsearch.xpack.esql.core.type.DataType.NESTED;
import static org.elasticsearch.xpack.esql.core.type.DataType.NULL;
import static org.elasticsearch.xpack.esql.core.type.DataType.OBJECT;
import static org.elasticsearch.xpack.esql.core.type.DataType.SCALED_FLOAT;
import static org.elasticsearch.xpack.esql.core.type.DataType.SHORT;
import static org.elasticsearch.xpack.esql.core.type.DataType.SOURCE;
import static org.elasticsearch.xpack.esql.core.type.DataType.TEXT;
import static org.elasticsearch.xpack.esql.core.type.DataType.TIME_DURATION;
import static org.elasticsearch.xpack.esql.core.type.DataType.UNSUPPORTED;
import static org.elasticsearch.xpack.esql.core.type.DataType.isNull;

public final class EsqlDataTypes {

    private static final Map<String, DataType> NAME_TO_TYPE = DataType.types()
        .stream()
        .collect(toUnmodifiableMap(DataType::typeName, t -> t));

    private EsqlDataTypes() {}

    public static DataType fromTypeName(String name) {
        return NAME_TO_TYPE.get(name.toLowerCase(Locale.ROOT));
    }

    public static DataType fromName(String name) {
        return DataType.fromEs(name);
    }

    public static boolean isUnsupported(DataType type) {
        return DataType.isUnsupported(type);
    }

    public static String outputType(DataType type) {
        if (type != null && type.esType() != null) {
            return type.esType();
        }
        return "unsupported";
    }

    public static boolean isString(DataType t) {
        return t == KEYWORD || t == TEXT;
    }

    public static boolean isPrimitive(DataType t) {
        return t != OBJECT && t != NESTED;
    }

    public static boolean isDateTimeOrTemporal(DataType t) {
        return DataType.isDateTime(t) || isTemporalAmount(t);
    }

    public static boolean isTemporalAmount(DataType t) {
        return t == DataType.DATE_PERIOD || t == DataType.TIME_DURATION;
    }

    public static boolean isNullOrTemporalAmount(DataType t) {
        return isTemporalAmount(t) || isNull(t);
    }

    public static boolean isNullOrDatePeriod(DataType t) {
        return t == DataType.DATE_PERIOD || isNull(t);
    }

    public static boolean isNullOrTimeDuration(DataType t) {
        return t == DataType.TIME_DURATION || isNull(t);
    }

    public static boolean isSpatial(DataType t) {
        return t == DataType.GEO_POINT || t == DataType.CARTESIAN_POINT || t == DataType.GEO_SHAPE || t == DataType.CARTESIAN_SHAPE;
    }

    public static boolean isSpatialGeo(DataType t) {
        return t == DataType.GEO_POINT || t == DataType.GEO_SHAPE;
    }

    public static boolean isSpatialPoint(DataType t) {
        return t == DataType.GEO_POINT || t == DataType.CARTESIAN_POINT;
    }

    /**
     * Supported types that can be contained in a block.
     */
    public static boolean isRepresentable(DataType t) {
        return t != OBJECT
            && t != NESTED
            && t != UNSUPPORTED
            && t != DATE_PERIOD
            && t != TIME_DURATION
            && t != BYTE
            && t != SHORT
            && t != FLOAT
            && t != SCALED_FLOAT
            && t != SOURCE
            && t != HALF_FLOAT
            && t.isCounter() == false;
    }

    public static boolean areCompatible(DataType left, DataType right) {
        if (left == right) {
            return true;
        } else {
            return (left == NULL || right == NULL) || (isString(left) && isString(right)) || (left.isNumeric() && right.isNumeric());
        }
    }
}
