/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.type;

import org.elasticsearch.xpack.esql.core.type.DataType;

import static org.elasticsearch.xpack.esql.core.type.DataType.NESTED;
import static org.elasticsearch.xpack.esql.core.type.DataType.OBJECT;
import static org.elasticsearch.xpack.esql.core.type.DataType.isNull;

public final class EsqlDataTypes {

    private EsqlDataTypes() {}

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

}
