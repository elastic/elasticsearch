/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.index.fieldvisitor;

import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.StoredFieldVisitor;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.mapper.IdFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.Uid;

import java.util.List;

/**
 * {@linkplain StoredFieldVisitor} that loads a single field value.
 */
public final class SingleFieldsVisitor extends StoredFieldVisitor {
    private final MappedFieldType field;
    private final List<Object> destination;

    /**
     * Build the field visitor;
     * @param field the name of the field to load
     * @param destination where to put the field's values
     */
    public SingleFieldsVisitor(MappedFieldType field, List<Object> destination) {
        this.field = field;
        this.destination = destination;
    }

    @Override
    public Status needsField(FieldInfo fieldInfo) {
        if (fieldInfo.name.equals(field.name())) {
            return Status.YES;
        }
        /*
         * We can't return Status.STOP here because we could be loading
         * multi-valued fields.
         */
        return Status.NO;
    }

    private void addValue(Object value) {
        destination.add(field.valueForDisplay(value));
    }

    @Override
    public void binaryField(FieldInfo fieldInfo, byte[] value) {
        if (IdFieldMapper.NAME.equals(fieldInfo.name)) {
            addValue(Uid.decodeId(value));
        } else {
            addValue(new BytesRef(value));
        }
    }

    @Override
    public void stringField(FieldInfo fieldInfo, String value) {
        addValue(value);
    }

    @Override
    public void intField(FieldInfo fieldInfo, int value) {
        addValue(value);
    }

    @Override
    public void longField(FieldInfo fieldInfo, long value) {
        addValue(value);
    }

    @Override
    public void floatField(FieldInfo fieldInfo, float value) {
        addValue(value);
    }

    @Override
    public void doubleField(FieldInfo fieldInfo, double value) {
        addValue(value);
    }
}
