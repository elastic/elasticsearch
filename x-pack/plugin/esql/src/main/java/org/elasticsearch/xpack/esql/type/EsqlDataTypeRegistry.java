/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.type;

import org.elasticsearch.index.mapper.TimeSeriesParams;
import org.elasticsearch.xpack.esql.core.type.DataType;

public class EsqlDataTypeRegistry {

    public static final EsqlDataTypeRegistry INSTANCE = new EsqlDataTypeRegistry();

    private EsqlDataTypeRegistry() {}

    public DataType fromEs(String typeName, TimeSeriesParams.MetricType metricType) {
        DataType type = DataType.fromEs(typeName);
        if (metricType != TimeSeriesParams.MetricType.COUNTER) {
            return type;
        }
        /*
         * If we're handling a time series COUNTER type field then convert it
         * into it's counter. But *first* we have to widen it because we only
         * have time series counters for `double`, `long` and `int`, not `float`
         * and `half_float`, etc. If the widened type still has no counter
         * variant (e.g. `unsigned_long`, which mappers happily accept as a
         * counter), treat it as unsupported rather than returning `null`.
         */
        DataType counter = type.widenSmallNumeric().counter();
        return counter != null ? counter : DataType.UNSUPPORTED;
    }
}
