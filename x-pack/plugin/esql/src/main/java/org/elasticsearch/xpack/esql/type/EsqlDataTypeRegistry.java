/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.type;

import org.elasticsearch.index.mapper.TimeSeriesParams;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.DataTypeRegistry;

import java.util.Collection;

import static org.elasticsearch.xpack.esql.core.type.DataType.DATETIME;
import static org.elasticsearch.xpack.esql.core.type.DataType.DATE_PERIOD;
import static org.elasticsearch.xpack.esql.core.type.DataType.TIME_DURATION;
import static org.elasticsearch.xpack.esql.core.type.DataType.isDateTime;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypes.isDateTimeOrTemporal;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypes.isNullOrDatePeriod;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypes.isNullOrTemporalAmount;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypes.isNullOrTimeDuration;

public class EsqlDataTypeRegistry implements DataTypeRegistry {

    public static final DataTypeRegistry INSTANCE = new EsqlDataTypeRegistry();

    private EsqlDataTypeRegistry() {}

    @Override
    public Collection<DataType> dataTypes() {
        return DataType.types();
    }

    @Override
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

    @Override
    public DataType fromJava(Object value) {
        return DataType.fromJava(value);
    }

    @Override
    public boolean isUnsupported(DataType type) {
        return type == DataType.UNSUPPORTED;
    }

    @Override
    public boolean canConvert(DataType from, DataType to) {
        return EsqlDataTypeConverter.canConvert(from, to);
    }

    @Override
    public Object convert(Object value, DataType type) {
        return EsqlDataTypeConverter.convert(value, type);
    }

    @Override
    public DataType commonType(DataType left, DataType right) {
        if (isDateTimeOrTemporal(left) || isDateTimeOrTemporal(right)) {
            if ((isDateTime(left) && isNullOrTemporalAmount(right)) || (isNullOrTemporalAmount(left) && isDateTime(right))) {
                return DATETIME;
            }
            if (isNullOrTimeDuration(left) && isNullOrTimeDuration(right)) {
                return TIME_DURATION;
            }
            if (isNullOrDatePeriod(left) && isNullOrDatePeriod(right)) {
                return DATE_PERIOD;
            }
        }
        return EsqlDataTypeConverter.commonType(left, right);
    }
}
