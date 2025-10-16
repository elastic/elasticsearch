/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.core.expression;

import org.elasticsearch.index.mapper.BlockLoader;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.util.concurrent.atomic.AtomicLong;


public class FieldTransformationAttribute<T, B extends BlockLoader.Builder>  extends FieldAttribute {

    private static final AtomicLong COUNTER = new AtomicLong();

    private final MappedFieldType.BlockValueLoader<?, ?> blockValueLoader;
    private final DataType dataType;
    private final FieldName fieldName;

    public FieldTransformationAttribute(
        FieldAttribute fieldAttribute,
        MappedFieldType.BlockValueLoader<?, ?> blockValueLoader,
        DataType dataType
    ) {
        super(fieldAttribute.source(), fieldAttribute.parentName(), fieldAttribute.qualifier(),
            fieldAttribute.name() + "_replaced_" + COUNTER.incrementAndGet(), fieldAttribute.field(), fieldAttribute.synthetic());
        this.fieldName = fieldAttribute.fieldName();
        this.blockValueLoader = blockValueLoader;
        this.dataType = dataType;
    }

    public MappedFieldType.BlockValueLoader<?, ?> getBlockValueLoader() {
        return blockValueLoader;
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
