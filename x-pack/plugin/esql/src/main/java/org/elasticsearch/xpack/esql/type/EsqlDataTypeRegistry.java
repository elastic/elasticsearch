/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.type;

import org.elasticsearch.index.mapper.TimeSeriesParams;
import org.elasticsearch.xpack.esql.core.type.DataTypes;
import org.elasticsearch.xpack.esql.core.type.DataTypeRegistry;

import java.util.Collection;

import static org.elasticsearch.xpack.esql.core.type.DataTypes.DATETIME;
import static org.elasticsearch.xpack.esql.core.type.DataTypes.DATE_PERIOD;
import static org.elasticsearch.xpack.esql.core.type.DataTypes.TIME_DURATION;
import static org.elasticsearch.xpack.esql.core.type.DataTypes.isDateTime;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypes.isDateTimeOrTemporal;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypes.isNullOrDatePeriod;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypes.isNullOrTemporalAmount;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypes.isNullOrTimeDuration;

public class EsqlDataTypeRegistry implements DataTypeRegistry {

    public static final DataTypeRegistry INSTANCE = new EsqlDataTypeRegistry();

    private EsqlDataTypeRegistry() {}

    @Override
    public Collection<DataTypes> dataTypes() {
        return DataTypes.types();
    }

    @Override
    public DataTypes fromEs(String typeName, TimeSeriesParams.MetricType metricType) {
        if (metricType == TimeSeriesParams.MetricType.COUNTER) {
            return EsqlDataTypes.getCounterType(typeName);
        } else {
            return EsqlDataTypes.fromName(typeName);
        }
    }

    @Override
    public DataTypes fromJava(Object value) {
        return EsqlDataTypes.fromJava(value);
    }

    @Override
    public boolean isUnsupported(DataTypes type) {
        return EsqlDataTypes.isUnsupported(type);
    }

    @Override
    public boolean canConvert(DataTypes from, DataTypes to) {
        return EsqlDataTypeConverter.canConvert(from, to);
    }

    @Override
    public Object convert(Object value, DataTypes type) {
        return EsqlDataTypeConverter.convert(value, type);
    }

    @Override
    public DataTypes commonType(DataTypes left, DataTypes right) {
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
