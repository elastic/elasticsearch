/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.vectors;

import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.fielddata.FieldDataContext;
import org.elasticsearch.index.mapper.FieldTypeTestCase;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.vectors.MultiDenseVectorFieldMapper.MultiDenseVectorFieldType;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper.BBQ_MIN_DIMS;

public class MultiDenseVectorFieldTypeTests extends FieldTypeTestCase {

    @BeforeClass
    public static void setup() {
        assumeTrue("Requires multi-dense vector support", MultiDenseVectorFieldMapper.FEATURE_FLAG.isEnabled());
    }

    private MultiDenseVectorFieldType createFloatFieldType() {
        return new MultiDenseVectorFieldType(
            "f",
            DenseVectorFieldMapper.ElementType.FLOAT,
            BBQ_MIN_DIMS,
            IndexVersion.current(),
            Collections.emptyMap()
        );
    }

    private MultiDenseVectorFieldType createByteFieldType() {
        return new MultiDenseVectorFieldType(
            "f",
            DenseVectorFieldMapper.ElementType.BYTE,
            5,
            IndexVersion.current(),
            Collections.emptyMap()
        );
    }

    public void testHasDocValues() {
        MultiDenseVectorFieldType fft = createFloatFieldType();
        assertTrue(fft.hasDocValues());
        MultiDenseVectorFieldType bft = createByteFieldType();
        assertTrue(bft.hasDocValues());
    }

    public void testIsIndexed() {
        MultiDenseVectorFieldType fft = createFloatFieldType();
        assertFalse(fft.isIndexed());
        MultiDenseVectorFieldType bft = createByteFieldType();
        assertFalse(bft.isIndexed());
    }

    public void testIsSearchable() {
        MultiDenseVectorFieldType fft = createFloatFieldType();
        assertFalse(fft.isSearchable());
        MultiDenseVectorFieldType bft = createByteFieldType();
        assertFalse(bft.isSearchable());
    }

    public void testIsAggregatable() {
        MultiDenseVectorFieldType fft = createFloatFieldType();
        assertFalse(fft.isAggregatable());
        MultiDenseVectorFieldType bft = createByteFieldType();
        assertFalse(bft.isAggregatable());
    }

    public void testFielddataBuilder() {
        MultiDenseVectorFieldType fft = createFloatFieldType();
        FieldDataContext fdc = new FieldDataContext("test", null, () -> null, Set::of, MappedFieldType.FielddataOperation.SCRIPT);
        assertNotNull(fft.fielddataBuilder(fdc));

        MultiDenseVectorFieldType bft = createByteFieldType();
        FieldDataContext bdc = new FieldDataContext("test", null, () -> null, Set::of, MappedFieldType.FielddataOperation.SCRIPT);
        assertNotNull(bft.fielddataBuilder(bdc));
    }

    public void testDocValueFormat() {
        MultiDenseVectorFieldType fft = createFloatFieldType();
        expectThrows(IllegalArgumentException.class, () -> fft.docValueFormat(null, null));
        MultiDenseVectorFieldType bft = createByteFieldType();
        expectThrows(IllegalArgumentException.class, () -> bft.docValueFormat(null, null));
    }

    public void testFetchSourceValue() throws IOException {
        MultiDenseVectorFieldType fft = createFloatFieldType();
        List<List<Double>> vector = List.of(List.of(0.0, 1.0, 2.0, 3.0, 4.0, 6.0));
        assertEquals(vector, fetchSourceValue(fft, vector));
        MultiDenseVectorFieldType bft = createByteFieldType();
        assertEquals(vector, fetchSourceValue(bft, vector));
    }
}
