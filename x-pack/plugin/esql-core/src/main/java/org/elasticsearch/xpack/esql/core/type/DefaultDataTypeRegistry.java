/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.core.type;

import org.elasticsearch.index.mapper.TimeSeriesParams;

import java.util.Collection;

public class DefaultDataTypeRegistry implements DataTypeRegistry {

    public static final DataTypeRegistry INSTANCE = new DefaultDataTypeRegistry();

    private DefaultDataTypeRegistry() {}

    @Override
    public Collection<DataTypes> dataTypes() {
        return DataTypes.types();
    }

    @Override
    public DataTypes fromEs(String typeName, TimeSeriesParams.MetricType metricType) {
        return DataTypes.fromEs(typeName);
    }

    @Override
    public DataTypes fromJava(Object value) {
        return DataTypes.fromJava(value);
    }

    @Override
    public boolean isUnsupported(DataTypes type) {
        return DataTypes.isUnsupported(type);
    }

    @Override
    public boolean canConvert(DataTypes from, DataTypes to) {
        return DataTypeConverter.canConvert(from, to);
    }

    @Override
    public Object convert(Object value, DataTypes type) {
        return DataTypeConverter.convert(value, type);
    }

    @Override
    public DataTypes commonType(DataTypes left, DataTypes right) {
        return DataTypeConverter.commonType(left, right);
    }
}
