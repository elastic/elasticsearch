/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.core.type;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.mapper.blockloader.BlockLoaderFunctionConfig;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

/**
 * EsField that represents a function being applied to a field on extraction. It receives a
 * {@link BlockLoaderFunctionConfig} that will be passed down to the block loading process
 * to apply the function at data load time.
 */
public class FunctionEsField extends EsField {

    // Not serialized as it will be created on the data node
    private final transient BlockLoaderFunctionConfig functionConfig;

    public FunctionEsField(EsField esField, DataType dataType, BlockLoaderFunctionConfig functionConfig) {
        this(
            esField.getName(),
            dataType,
            esField.getProperties(),
            esField.isAggregatable(),
            esField.isAlias(),
            esField.getTimeSeriesFieldType(),
            functionConfig
        );
    }

    private FunctionEsField(
        String name,
        DataType esDataType,
        Map<String, EsField> properties,
        boolean aggregatable,
        boolean isAlias,
        TimeSeriesFieldType timeSeriesFieldType,
        BlockLoaderFunctionConfig functionConfig
    ) {
        super(name, esDataType, properties, aggregatable, isAlias, timeSeriesFieldType);
        this.functionConfig = functionConfig;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        throw new UnsupportedOperationException("FunctionEsField is not serializable, should be created on data nodes");
    }

    public BlockLoaderFunctionConfig functionConfig() {
        return functionConfig;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        if (super.equals(o) == false) return false;
        FunctionEsField that = (FunctionEsField) o;
        return Objects.equals(functionConfig, that.functionConfig);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), functionConfig);
    }

    @Override
    public Exact getExactInfo() {
        /*
         * We force and "inexact" field info to prevent pushing
         * expressions like `WHERE LENGTH(kwd) > 2`. `LENGTH(kwd)`
         * is a `FunctionEsField` which *looks* pushable without
         * the inexact match.
         */
        return new Exact(false, "merged with " + functionConfig.function());
    }
}
