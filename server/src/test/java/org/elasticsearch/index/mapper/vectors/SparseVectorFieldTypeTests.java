/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper.vectors;

import org.elasticsearch.index.fielddata.FieldDataContext;
import org.elasticsearch.index.mapper.FieldTypeTestCase;
import org.elasticsearch.index.mapper.MappedFieldType;

import java.util.Collections;

public class SparseVectorFieldTypeTests extends FieldTypeTestCase {

    public void testDocValuesDisabled() {
        MappedFieldType fieldType = new SparseVectorFieldMapper.SparseVectorFieldType("field", Collections.emptyMap());
        assertFalse(fieldType.hasDocValues());
        expectThrows(IllegalArgumentException.class, () -> fieldType.fielddataBuilder(FieldDataContext.noRuntimeFields("test")));
    }

    public void testIsNotAggregatable() {
        MappedFieldType fieldType = new SparseVectorFieldMapper.SparseVectorFieldType("field", Collections.emptyMap());
        assertFalse(fieldType.isAggregatable());
    }

    public void testDocValueFormatIsNotSupported() {
        MappedFieldType fieldType = new SparseVectorFieldMapper.SparseVectorFieldType("field", Collections.emptyMap());
        UnsupportedOperationException exc = expectThrows(UnsupportedOperationException.class, () -> fieldType.docValueFormat(null, null));
        assertEquals(SparseVectorFieldMapper.ERROR_MESSAGE_7X, exc.getMessage());
    }

    public void testExistsQueryIsNotSupported() {
        MappedFieldType fieldType = new SparseVectorFieldMapper.SparseVectorFieldType("field", Collections.emptyMap());
        UnsupportedOperationException exc = expectThrows(UnsupportedOperationException.class, () -> fieldType.existsQuery(null));
        assertEquals(SparseVectorFieldMapper.ERROR_MESSAGE_7X, exc.getMessage());
    }

    public void testTermQueryIsNotSupported() {
        MappedFieldType fieldType = new SparseVectorFieldMapper.SparseVectorFieldType("field", Collections.emptyMap());
        UnsupportedOperationException exc = expectThrows(UnsupportedOperationException.class, () -> fieldType.termQuery(null, null));
        assertEquals(SparseVectorFieldMapper.ERROR_MESSAGE_7X, exc.getMessage());
    }
}
