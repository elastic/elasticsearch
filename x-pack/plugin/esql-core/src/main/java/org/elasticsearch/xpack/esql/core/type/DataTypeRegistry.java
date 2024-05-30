/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.core.type;

import org.elasticsearch.index.mapper.TimeSeriesParams;

import java.util.Collection;

/**
 * Central class for {@link DataTypes} creation and conversion.
 */
public interface DataTypeRegistry {

    //
    // Discovery
    //
    Collection<DataTypes> dataTypes();

    DataTypes fromEs(String typeName, TimeSeriesParams.MetricType metricType);

    DataTypes fromJava(Object value);

    boolean isUnsupported(DataTypes type);

    //
    // Conversion methods
    //
    boolean canConvert(DataTypes from, DataTypes to);

    Object convert(Object value, DataTypes type);

    DataTypes commonType(DataTypes left, DataTypes right);
}
