/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.type;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.xpack.esql.core.type.DataTypes;

import java.util.Collections;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;

import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toUnmodifiableMap;
import static org.elasticsearch.xpack.esql.core.type.DataTypes.BOOLEAN;
import static org.elasticsearch.xpack.esql.core.type.DataTypes.BYTE;
import static org.elasticsearch.xpack.esql.core.type.DataTypes.DATE_PERIOD;
import static org.elasticsearch.xpack.esql.core.type.DataTypes.DOUBLE;
import static org.elasticsearch.xpack.esql.core.type.DataTypes.FLOAT;
import static org.elasticsearch.xpack.esql.core.type.DataTypes.HALF_FLOAT;
import static org.elasticsearch.xpack.esql.core.type.DataTypes.INTEGER;
import static org.elasticsearch.xpack.esql.core.type.DataTypes.KEYWORD;
import static org.elasticsearch.xpack.esql.core.type.DataTypes.LONG;
import static org.elasticsearch.xpack.esql.core.type.DataTypes.NESTED;
import static org.elasticsearch.xpack.esql.core.type.DataTypes.NULL;
import static org.elasticsearch.xpack.esql.core.type.DataTypes.OBJECT;
import static org.elasticsearch.xpack.esql.core.type.DataTypes.SCALED_FLOAT;
import static org.elasticsearch.xpack.esql.core.type.DataTypes.SHORT;
import static org.elasticsearch.xpack.esql.core.type.DataTypes.SOURCE;
import static org.elasticsearch.xpack.esql.core.type.DataTypes.TEXT;
import static org.elasticsearch.xpack.esql.core.type.DataTypes.TIME_DURATION;
import static org.elasticsearch.xpack.esql.core.type.DataTypes.UNSUPPORTED;
import static org.elasticsearch.xpack.esql.core.type.DataTypes.isNull;

public final class EsqlDataTypes {

    private static final Map<String, DataTypes> NAME_TO_TYPE = DataTypes.types()
        .stream()
        .collect(toUnmodifiableMap(DataTypes::typeName, t -> t));

    private static final Map<String, DataTypes> ES_TO_TYPE;

    static {
        Map<String, DataTypes> map = DataTypes.types().stream().filter(e -> e.esType() != null).collect(toMap(DataTypes::esType, t -> t));
        // ES calls this 'point', but ESQL calls it 'cartesian_point'
        map.put("point", DataTypes.CARTESIAN_POINT);
        map.put("shape", DataTypes.CARTESIAN_SHAPE);
        ES_TO_TYPE = Collections.unmodifiableMap(map);
    }

    private static final Map<String, DataTypes> NAME_OR_ALIAS_TO_TYPE;
    static {
        Map<String, DataTypes> map = DataTypes.types().stream().collect(toMap(DataTypes::typeName, Function.identity()));
        map.put("bool", BOOLEAN);
        map.put("int", INTEGER);
        map.put("string", KEYWORD);
        NAME_OR_ALIAS_TO_TYPE = Collections.unmodifiableMap(map);
    }

    private EsqlDataTypes() {}

    public static DataTypes fromTypeName(String name) {
        return NAME_TO_TYPE.get(name.toLowerCase(Locale.ROOT));
    }

    public static DataTypes fromName(String name) {
        DataTypes type = ES_TO_TYPE.get(name);
        return type != null ? type : UNSUPPORTED;
    }

    public static DataTypes fromNameOrAlias(String typeName) {
        DataTypes type = NAME_OR_ALIAS_TO_TYPE.get(typeName.toLowerCase(Locale.ROOT));
        return type != null ? type : UNSUPPORTED;
    }

    public static DataTypes fromJava(Object value) {
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

    public static boolean isUnsupported(DataTypes type) {
        return DataTypes.isUnsupported(type);
    }

    public static String outputType(DataTypes type) {
        if (type != null && type.esType() != null) {
            return type.esType();
        }
        return "unsupported";
    }

    public static boolean isString(DataTypes t) {
        return t == KEYWORD || t == TEXT;
    }

    public static boolean isPrimitive(DataTypes t) {
        return t != OBJECT && t != NESTED;
    }

    public static boolean isDateTimeOrTemporal(DataTypes t) {
        return DataTypes.isDateTime(t) || isTemporalAmount(t);
    }

    public static boolean isTemporalAmount(DataTypes t) {
        return t == DataTypes.DATE_PERIOD || t == DataTypes.TIME_DURATION;
    }

    public static boolean isNullOrTemporalAmount(DataTypes t) {
        return isTemporalAmount(t) || isNull(t);
    }

    public static boolean isNullOrDatePeriod(DataTypes t) {
        return t == DataTypes.DATE_PERIOD || isNull(t);
    }

    public static boolean isNullOrTimeDuration(DataTypes t) {
        return t == DataTypes.TIME_DURATION || isNull(t);
    }

    public static boolean isSpatial(DataTypes t) {
        return t == DataTypes.GEO_POINT || t == DataTypes.CARTESIAN_POINT || t == DataTypes.GEO_SHAPE || t == DataTypes.CARTESIAN_SHAPE;
    }

    public static boolean isSpatialGeo(DataTypes t) {
        return t == DataTypes.GEO_POINT || t == DataTypes.GEO_SHAPE;
    }

    public static boolean isSpatialPoint(DataTypes t) {
        return t == DataTypes.GEO_POINT || t == DataTypes.CARTESIAN_POINT;
    }

    /**
     * Supported types that can be contained in a block.
     */
    public static boolean isRepresentable(DataTypes t) {
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
            && isCounterType(t) == false;
    }

    public static boolean areCompatible(DataTypes left, DataTypes right) {
        if (left == right) {
            return true;
        } else {
            return (left == NULL || right == NULL) || (isString(left) && isString(right)) || (left.isNumeric() && right.isNumeric());
        }
    }

    public static DataTypes widenSmallNumericTypes(DataTypes type) {
        if (type == BYTE || type == SHORT) {
            return INTEGER;
        }
        if (type == HALF_FLOAT || type == FLOAT || type == SCALED_FLOAT) {
            return DOUBLE;
        }
        return type;
    }

    public static DataTypes getCounterType(String typeName) {
        final DataTypes rootType = widenSmallNumericTypes(fromName(typeName));
        if (rootType == UNSUPPORTED) {
            return rootType;
        }
        assert rootType == LONG || rootType == INTEGER || rootType == DOUBLE : rootType;
        return fromTypeName("counter_" + rootType.typeName());
    }

    public static boolean isCounterType(DataTypes dt) {
        return dt == DataTypes.COUNTER_LONG || dt == DataTypes.COUNTER_INTEGER || dt == DataTypes.COUNTER_DOUBLE;
    }
}
