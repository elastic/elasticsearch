/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.core.expression;

import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.xpack.esql.core.type.DataType;

/**
 * A field attribute that has a function applied to it for value extraction.
 * This is used to replace a field attribute with a function that can extract
 * the value in a way that is more efficient than loading the raw field value
 * and applying the function to it.
 */
public class FieldFunctionAttribute extends FieldAttribute {

    private final MappedFieldType.BlockLoaderValueFunction<?, ?> blockLoaderValueFunction;
    private final DataType dataType;
    private final FieldName fieldName;

    public FieldFunctionAttribute(
        FieldAttribute fieldAttribute,
        MappedFieldType.BlockLoaderValueFunction<?, ?> blockLoaderValueFunction,
        DataType dataType
    ) {
        super(fieldAttribute.source(), fieldAttribute.parentName(), fieldAttribute.qualifier(),
            Attribute.rawTemporaryName(fieldAttribute.name(), "replaced", new NameId().toString()),
            fieldAttribute.field(), fieldAttribute.synthetic());
        this.fieldName = fieldAttribute.fieldName();
        this.blockLoaderValueFunction = blockLoaderValueFunction;
        this.dataType = dataType;
    }

    /**
     * Returns the function that will be used to load the value of the field and transform it
     */
    public MappedFieldType.BlockLoaderValueFunction<?, ?> getBlockLoaderValueFunction() {
        return blockLoaderValueFunction;
    }

    @Override
    public FieldName fieldName() {
        return fieldName;
    }

    @Override
    public DataType dataType() {
        return dataType;
    }
}
