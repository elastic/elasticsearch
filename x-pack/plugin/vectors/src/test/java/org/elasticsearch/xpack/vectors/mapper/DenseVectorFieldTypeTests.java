/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.vectors.mapper;

import org.elasticsearch.Version;
import org.elasticsearch.index.mapper.FieldTypeTestCase;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

public class DenseVectorFieldTypeTests extends FieldTypeTestCase {
    private final boolean indexed;

    public DenseVectorFieldTypeTests() {
        this.indexed = randomBoolean();
    }

    private DenseVectorFieldMapper.DenseVectorFieldType createFieldType() {
        return new DenseVectorFieldMapper.DenseVectorFieldType("f", Version.CURRENT, 5, indexed, Collections.emptyMap());
    }

    public void testHasDocValues() {
        DenseVectorFieldMapper.DenseVectorFieldType ft = createFieldType();
        assertNotEquals(indexed, ft.hasDocValues());
    }

    public void testIsSearchable() {
        DenseVectorFieldMapper.DenseVectorFieldType ft = createFieldType();
        assertEquals(indexed, ft.isSearchable());
    }

    public void testIsAggregatable() {
        DenseVectorFieldMapper.DenseVectorFieldType ft = createFieldType();
        assertFalse(ft.isAggregatable());
    }

    public void testFielddataBuilder() {
        DenseVectorFieldMapper.DenseVectorFieldType ft = createFieldType();
        assertNotNull(ft.fielddataBuilder("index", () -> {
            throw new UnsupportedOperationException();
        }));
    }

    public void testDocValueFormat() {
        DenseVectorFieldMapper.DenseVectorFieldType ft = createFieldType();
        expectThrows(IllegalArgumentException.class, () -> ft.docValueFormat(null, null));
    }

    public void testFetchSourceValue() throws IOException {
        DenseVectorFieldMapper.DenseVectorFieldType ft = createFieldType();
        List<Double> vector = List.of(0.0, 1.0, 2.0, 3.0, 4.0);
        assertEquals(vector, fetchSourceValue(ft, vector));
    }
}
