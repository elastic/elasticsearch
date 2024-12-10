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
import org.elasticsearch.index.mapper.vectors.RankVectorsFieldMapper.RankVectorsFieldType;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper.BBQ_MIN_DIMS;

public class RankVectorsFieldTypeTests extends FieldTypeTestCase {

    @BeforeClass
    public static void setup() {
        assumeTrue("Requires rank-vectors support", RankVectorsFieldMapper.FEATURE_FLAG.isEnabled());
    }

    private RankVectorsFieldType createFloatFieldType() {
        return new RankVectorsFieldType(
            "f",
            DenseVectorFieldMapper.ElementType.FLOAT,
            BBQ_MIN_DIMS,
            IndexVersion.current(),
            Collections.emptyMap()
        );
    }

    private RankVectorsFieldType createByteFieldType() {
        return new RankVectorsFieldType("f", DenseVectorFieldMapper.ElementType.BYTE, 5, IndexVersion.current(), Collections.emptyMap());
    }

    public void testHasDocValues() {
        RankVectorsFieldType fft = createFloatFieldType();
        assertTrue(fft.hasDocValues());
        RankVectorsFieldType bft = createByteFieldType();
        assertTrue(bft.hasDocValues());
    }

    public void testIsIndexed() {
        RankVectorsFieldType fft = createFloatFieldType();
        assertFalse(fft.isIndexed());
        RankVectorsFieldType bft = createByteFieldType();
        assertFalse(bft.isIndexed());
    }

    public void testIsSearchable() {
        RankVectorsFieldType fft = createFloatFieldType();
        assertFalse(fft.isSearchable());
        RankVectorsFieldType bft = createByteFieldType();
        assertFalse(bft.isSearchable());
    }

    public void testIsAggregatable() {
        RankVectorsFieldType fft = createFloatFieldType();
        assertFalse(fft.isAggregatable());
        RankVectorsFieldType bft = createByteFieldType();
        assertFalse(bft.isAggregatable());
    }

    public void testFielddataBuilder() {
        RankVectorsFieldType fft = createFloatFieldType();
        FieldDataContext fdc = new FieldDataContext("test", null, () -> null, Set::of, MappedFieldType.FielddataOperation.SCRIPT);
        assertNotNull(fft.fielddataBuilder(fdc));

        RankVectorsFieldType bft = createByteFieldType();
        FieldDataContext bdc = new FieldDataContext("test", null, () -> null, Set::of, MappedFieldType.FielddataOperation.SCRIPT);
        assertNotNull(bft.fielddataBuilder(bdc));
    }

    public void testDocValueFormat() {
        RankVectorsFieldType fft = createFloatFieldType();
        expectThrows(IllegalArgumentException.class, () -> fft.docValueFormat(null, null));
        RankVectorsFieldType bft = createByteFieldType();
        expectThrows(IllegalArgumentException.class, () -> bft.docValueFormat(null, null));
    }

    public void testFetchSourceValue() throws IOException {
        RankVectorsFieldType fft = createFloatFieldType();
        List<List<Double>> vector = List.of(List.of(0.0, 1.0, 2.0, 3.0, 4.0, 6.0));
        assertEquals(vector, fetchSourceValue(fft, vector));
        RankVectorsFieldType bft = createByteFieldType();
        assertEquals(vector, fetchSourceValue(bft, vector));
    }
}
