/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.mapper;

import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.elasticsearch.index.mapper.FieldTypeTestCase;
import org.elasticsearch.index.mapper.MappedFieldType;

import java.util.Collections;

public class OffsetSourceFieldTypeTests extends FieldTypeTestCase {
    public void testIsNotAggregatable() {
        MappedFieldType fieldType = getMappedFieldType();
        assertFalse(fieldType.isAggregatable());
    }

    @Override
    public void testFieldHasValue() {
        MappedFieldType fieldType = getMappedFieldType();
        FieldInfos fieldInfos = new FieldInfos(new FieldInfo[] { getFieldInfoWithName(fieldType.name()) });
        assertTrue(fieldType.fieldHasValue(fieldInfos));
    }

    @Override
    public void testFieldHasValueWithEmptyFieldInfos() {
        MappedFieldType fieldType = getMappedFieldType();
        assertFalse(fieldType.fieldHasValue(FieldInfos.EMPTY));
    }

    @Override
    public MappedFieldType getMappedFieldType() {
        return new OffsetSourceFieldMapper.OffsetSourceFieldType(
            "field",
            OffsetSourceFieldMapper.CharsetFormat.UTF_16,
            Collections.emptyMap()
        );
    }
}
