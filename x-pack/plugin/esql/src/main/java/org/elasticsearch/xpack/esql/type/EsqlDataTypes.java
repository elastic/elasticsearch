/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.type;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;

import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toUnmodifiableMap;
import static org.elasticsearch.xpack.ql.type.DataTypes.BOOLEAN;
import static org.elasticsearch.xpack.ql.type.DataTypes.BYTE;
import static org.elasticsearch.xpack.ql.type.DataTypes.DATETIME;
import static org.elasticsearch.xpack.ql.type.DataTypes.DOUBLE;
import static org.elasticsearch.xpack.ql.type.DataTypes.FLOAT;
import static org.elasticsearch.xpack.ql.type.DataTypes.HALF_FLOAT;
import static org.elasticsearch.xpack.ql.type.DataTypes.INTEGER;
import static org.elasticsearch.xpack.ql.type.DataTypes.IP;
import static org.elasticsearch.xpack.ql.type.DataTypes.KEYWORD;
import static org.elasticsearch.xpack.ql.type.DataTypes.LONG;
import static org.elasticsearch.xpack.ql.type.DataTypes.NESTED;
import static org.elasticsearch.xpack.ql.type.DataTypes.NULL;
import static org.elasticsearch.xpack.ql.type.DataTypes.OBJECT;
import static org.elasticsearch.xpack.ql.type.DataTypes.SCALED_FLOAT;
import static org.elasticsearch.xpack.ql.type.DataTypes.SHORT;
import static org.elasticsearch.xpack.ql.type.DataTypes.SOURCE;
import static org.elasticsearch.xpack.ql.type.DataTypes.TEXT;
import static org.elasticsearch.xpack.ql.type.DataTypes.UNSIGNED_LONG;
import static org.elasticsearch.xpack.ql.type.DataTypes.UNSUPPORTED;
import static org.elasticsearch.xpack.ql.type.DataTypes.VERSION;
import static org.elasticsearch.xpack.ql.type.DataTypes.isNull;

public final class EsqlDataTypes {

    public static final DataType DATE_PERIOD = new DataType("DATE_PERIOD", null, 3 * Integer.BYTES, false, false, false);
    public static final DataType TIME_DURATION = new DataType("TIME_DURATION", null, Integer.BYTES + Long.BYTES, false, false, false);
    public static final DataType GEO_POINT = new DataType("geo_point", Double.BYTES * 2, false, false, true);
    public static final DataType CARTESIAN_POINT = new DataType("cartesian_point", Double.BYTES * 2, false, false, true);
    public static final DataType GEO_SHAPE = new DataType("geo_shape", Integer.MAX_VALUE, false, false, true);
    public static final DataType CARTESIAN_SHAPE = new DataType("cartesian_shape", Integer.MAX_VALUE, false, false, true);

    private static final Collection<DataType> TYPES = Stream.of(
        BOOLEAN,
        UNSUPPORTED,
        NULL,
        BYTE,
        SHORT,
        INTEGER,
        LONG,
        DOUBLE,
        FLOAT,
        HALF_FLOAT,
        KEYWORD,
        TEXT,
        DATETIME,
        DATE_PERIOD,
        TIME_DURATION,
        IP,
        OBJECT,
        NESTED,
        SCALED_FLOAT,
        SOURCE,
        VERSION,
        UNSIGNED_LONG,
        GEO_POINT,
        CARTESIAN_POINT,
        CARTESIAN_SHAPE,
        GEO_SHAPE
    ).sorted(Comparator.comparing(DataType::typeName)).toList();

    private static final Map<String, DataType> NAME_TO_TYPE = TYPES.stream().collect(toUnmodifiableMap(DataType::typeName, t -> t));

    private static final Map<String, DataType> ES_TO_TYPE;

    static {
        Map<String, DataType> map = TYPES.stream().filter(e -> e.esType() != null).collect(toMap(DataType::esType, t -> t));
        // ES calls this 'point', but ESQL calls it 'cartesian_point'
        map.put("point", CARTESIAN_POINT);
        map.put("shape", CARTESIAN_SHAPE);
        ES_TO_TYPE = Collections.unmodifiableMap(map);
    }

    private EsqlDataTypes() {}

    public static Collection<DataType> types() {
        return TYPES;
    }

    public static DataType fromTypeName(String name) {
        return NAME_TO_TYPE.get(name.toLowerCase(Locale.ROOT));
    }

    public static DataType fromName(String name) {
        DataType type = ES_TO_TYPE.get(name);
        return type != null ? type : UNSUPPORTED;
    }

    public static DataType fromJava(Object value) {
        if (value == null) {
            return NULL;
        }
        if (value instanceof Boolean) {
            return BOOLEAN;
        }
        if (value instanceof Integer) {
            return INTEGER;
        }
        if (value instanceof Long) {
            return LONG;
        }
        if (value instanceof Double) {
            return DOUBLE;
        }
        if (value instanceof Float) {
            return FLOAT;
        }
        if (value instanceof String || value instanceof Character || value instanceof BytesRef) {
            return KEYWORD;
        }

        return null;
    }

    public static boolean isUnsupported(DataType type) {
        return DataTypes.isUnsupported(type);
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
        return DataTypes.isDateTime(t) || isTemporalAmount(t);
    }

    public static boolean isTemporalAmount(DataType t) {
        return t == DATE_PERIOD || t == TIME_DURATION;
    }

    public static boolean isNullOrTemporalAmount(DataType t) {
        return isTemporalAmount(t) || isNull(t);
    }

    public static boolean isNullOrDatePeriod(DataType t) {
        return t == DATE_PERIOD || isNull(t);
    }

    public static boolean isNullOrTimeDuration(DataType t) {
        return t == TIME_DURATION || isNull(t);
    }

    public static boolean isSpatial(DataType t) {
        return t == GEO_POINT || t == CARTESIAN_POINT || t == GEO_SHAPE || t == CARTESIAN_SHAPE;
    }

    public static boolean isSpatialGeo(DataType t) {
        return t == GEO_POINT || t == GEO_SHAPE;
    }

    public static boolean isSpatialPoint(DataType t) {
        return t == GEO_POINT || t == CARTESIAN_POINT;
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
            && t != HALF_FLOAT;
    }

    public static boolean areCompatible(DataType left, DataType right) {
        if (left == right) {
            return true;
        } else {
            return (left == NULL || right == NULL) || (isString(left) && isString(right)) || (left.isNumeric() && right.isNumeric());
        }
    }

    public static DataType widenSmallNumericTypes(DataType type) {
        if (type == BYTE || type == SHORT) {
            return INTEGER;
        }
        if (type == HALF_FLOAT || type == FLOAT || type == SCALED_FLOAT) {
            return DOUBLE;
        }
        return type;
    }
}
