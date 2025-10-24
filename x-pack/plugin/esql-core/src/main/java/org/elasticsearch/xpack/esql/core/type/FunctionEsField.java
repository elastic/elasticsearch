/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.core.type;

import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.xpack.esql.core.expression.function.Function;

import java.util.Map;

/**
 * EsField that represents a function being applied to a field on extraction. It receives a
 * {@link org.elasticsearch.index.mapper.MappedFieldType.BlockLoaderFunctionConfig} that will be passed down to the block loading process
 * to apply the function at data load time.
 */
public class FunctionEsField extends EsField {

    // Not serialized as it will be created on the data node
    private final transient MappedFieldType.BlockLoaderFunctionConfig functionConfig;

    public FunctionEsField(EsField esField, Function function, MappedFieldType.BlockLoaderFunctionConfig functionConfig) {
        this(
            esField.getName(),
            function.dataType(),
            esField.getProperties(),
            esField.isAggregatable(),
            esField.isAlias(),
            esField.getTimeSeriesFieldType(),
            functionConfig
        );
    }

    private FunctionEsField(String name, DataType esDataType, Map<String, EsField> properties, boolean aggregatable,
                           boolean isAlias, TimeSeriesFieldType timeSeriesFieldType,
                            MappedFieldType.BlockLoaderFunctionConfig functionConfig) {
        super(name, esDataType, properties, aggregatable, isAlias, timeSeriesFieldType);
        this.functionConfig = functionConfig;
    }

    public MappedFieldType.BlockLoaderFunctionConfig functionConfig() {
        return functionConfig;
    }
}
