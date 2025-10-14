/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.core.expression;

import org.elasticsearch.xpack.esql.core.type.DataType;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;


public class FieldTransformationAttribute<T, U>  extends FieldAttribute {

    private static final AtomicLong COUNTER = new AtomicLong();

    private final Function<T, U> fieldValueTransformation;
    private final DataType dataType;
    private final FieldName fieldName;

    public FieldTransformationAttribute(
        FieldAttribute fieldAttribute,
        Function<T, U> fieldValueTransformation,
        DataType dataType
    ) {
        super(fieldAttribute.source(), fieldAttribute.parentName(), fieldAttribute.qualifier(),
            fieldAttribute.name() + "_replaced_" + COUNTER.incrementAndGet(), fieldAttribute.field(), fieldAttribute.synthetic());
        this.fieldName = fieldAttribute.fieldName();
        this.fieldValueTransformation = fieldValueTransformation;
        this.dataType = dataType;
    }

    public Function<T, U> getFieldValueTransformation() {
        return fieldValueTransformation;
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
