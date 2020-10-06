/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */


package org.elasticsearch.xpack.vectors.mapper;

import org.elasticsearch.index.mapper.FieldTypeTestCase;
import org.elasticsearch.index.mapper.MappedFieldType;

import java.util.Collections;

public class SparseVectorFieldTypeTests extends FieldTypeTestCase {

    public void testHasDocValues() {
        MappedFieldType fieldType = new SparseVectorFieldMapper.SparseVectorFieldType("field", Collections.emptyMap());
        assertTrue(fieldType.hasDocValues());
    }

    public void testFielddataBuilder() {
        MappedFieldType fieldType = new SparseVectorFieldMapper.SparseVectorFieldType("field", Collections.emptyMap());
        assertNotNull(fieldType.fielddataBuilder("index", () -> {
            throw new UnsupportedOperationException();
        }));
    }

    public void testIsNotAggregatable() {
        MappedFieldType fieldType = new SparseVectorFieldMapper.SparseVectorFieldType("field", Collections.emptyMap());
        assertFalse(fieldType.isAggregatable());
    }

    public void testDocValueFormatIsNotSupported() {
        MappedFieldType fieldType = new SparseVectorFieldMapper.SparseVectorFieldType("field", Collections.emptyMap());
        UnsupportedOperationException exc = expectThrows(UnsupportedOperationException.class, () -> fieldType.docValueFormat(null, null));
        assertEquals("Field [field] of type [sparse_vector] doesn't support docvalue_fields or aggregations", exc.getMessage());
    }

    public void testTermQueryIsNotSupported() {
        MappedFieldType fieldType = new SparseVectorFieldMapper.SparseVectorFieldType("field", Collections.emptyMap());
        UnsupportedOperationException exc = expectThrows(UnsupportedOperationException.class, () -> fieldType.termQuery(null, null));
        assertEquals("Field [field] of type [sparse_vector] doesn't support queries", exc.getMessage());
    }
}
