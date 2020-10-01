/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.vectors.mapper;

import org.elasticsearch.index.mapper.FieldTypeTestCase;

import java.util.Collections;

public class DenseVectorFieldTypeTests extends FieldTypeTestCase {

    public void testHasDocValues() {
        DenseVectorFieldMapper.DenseVectorFieldType ft = new DenseVectorFieldMapper.DenseVectorFieldType("f", 1, Collections.emptyMap());
        assertTrue(ft.hasDocValues());
    }

    public void testIsAggregatable() {
        DenseVectorFieldMapper.DenseVectorFieldType ft = new DenseVectorFieldMapper.DenseVectorFieldType("f", 1, Collections.emptyMap());
        assertFalse(ft.isAggregatable());
    }

    public void testFielddataBuilder() {
        DenseVectorFieldMapper.DenseVectorFieldType ft = new DenseVectorFieldMapper.DenseVectorFieldType("f", 1, Collections.emptyMap());
        assertNotNull(ft.fielddataBuilder("index", () -> {
            throw new UnsupportedOperationException();
        }));
    }

    public void testDocValueFormat() {
        DenseVectorFieldMapper.DenseVectorFieldType ft = new DenseVectorFieldMapper.DenseVectorFieldType("f", 1, Collections.emptyMap());
        expectThrows(UnsupportedOperationException.class, () -> ft.docValueFormat(null, null));
    }
}
