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
        /*
         * If we're handling a time series COUNTER type field then convert it
         * into it's counter. But *first* we have to widen it because we only
         * have time series counters for `double`, `long` and `int`, not `float`
         * and `half_float`, etc.
         */
        return metricType == TimeSeriesParams.MetricType.COUNTER ? type.widenSmallNumeric().counter() : type;
    }
}
