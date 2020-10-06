/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.vectors.mapper;

import org.elasticsearch.index.mapper.FieldTypeTestCase;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

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

    public void testFetchSourceValue() throws IOException {
        DenseVectorFieldMapper.DenseVectorFieldType ft = new DenseVectorFieldMapper.DenseVectorFieldType("f", 5, Collections.emptyMap());
        List<Double> vector = List.of(0.0, 1.0, 2.0, 3.0, 4.0);
        assertEquals(vector, fetchSourceValue(ft, vector));
    }
}
