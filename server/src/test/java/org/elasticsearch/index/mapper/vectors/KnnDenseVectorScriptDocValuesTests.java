/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper.vectors;

import org.apache.lucene.index.VectorValues;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.VectorUtil;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper.ElementType;
import org.elasticsearch.script.field.vectors.ByteKnnDenseVectorDocValuesField;
import org.elasticsearch.script.field.vectors.DenseVector;
import org.elasticsearch.script.field.vectors.DenseVectorDocValuesField;
import org.elasticsearch.script.field.vectors.KnnDenseVectorDocValuesField;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.nio.ByteBuffer;

import static org.hamcrest.Matchers.containsString;

public class KnnDenseVectorScriptDocValuesTests extends ESTestCase {

    public void testFloatGetVectorValueAndGetMagnitude() throws IOException {
        int dims = 3;
        float[][] vectors = { { 1, 1, 1 }, { 1, 1, 2 }, { 1, 1, 3 } };
        float[] expectedMagnitudes = { 1.7320f, 2.4495f, 3.3166f };

        DenseVectorDocValuesField field = new KnnDenseVectorDocValuesField(
            wrap(vectors, ElementType.FLOAT),
            "test",
            ElementType.FLOAT,
            dims
        );
        DenseVectorScriptDocValues scriptDocValues = field.toScriptDocValues();
        for (int i = 0; i < vectors.length; i++) {
            field.setNextDocId(i);
            assertEquals(1, field.size());
            assertEquals(dims, scriptDocValues.dims());
            assertArrayEquals(vectors[i], scriptDocValues.getVectorValue(), 0.0001f);
            assertEquals(expectedMagnitudes[i], scriptDocValues.getMagnitude(), 0.0001f);
        }
    }

    public void testByteGetVectorValueAndGetMagnitude() throws IOException {
        int dims = 3;
        float[][] vectors = { { 1, 1, 1 }, { 1, 1, 2 }, { 1, 1, 3 } };
        float[] expectedMagnitudes = { 1.7320f, 2.4495f, 3.3166f };

        DenseVectorDocValuesField field = new ByteKnnDenseVectorDocValuesField(
            wrap(vectors, ElementType.BYTE),
            "test",
            ElementType.BYTE,
            dims
        );
        DenseVectorScriptDocValues scriptDocValues = field.toScriptDocValues();
        for (int i = 0; i < vectors.length; i++) {
            field.setNextDocId(i);
            assertEquals(1, field.size());
            assertEquals(dims, scriptDocValues.dims());
            assertArrayEquals(vectors[i], scriptDocValues.getVectorValue(), 0.0001f);
            assertEquals(expectedMagnitudes[i], scriptDocValues.getMagnitude(), 0.0001f);
        }
    }

    public void testFloatMetadataAndIterator() throws IOException {
        int dims = 3;
        float[][] vectors = fill(new float[randomIntBetween(1, 5)][dims], ElementType.FLOAT);
        DenseVectorDocValuesField field = new KnnDenseVectorDocValuesField(
            wrap(vectors, ElementType.FLOAT),
            "test",
            ElementType.FLOAT,
            dims
        );
        for (int i = 0; i < vectors.length; i++) {
            field.setNextDocId(i);
            DenseVector dv = field.get();
            assertEquals(1, dv.size());
            assertFalse(dv.isEmpty());
            assertEquals(dims, dv.getDims());
            UnsupportedOperationException e = expectThrows(UnsupportedOperationException.class, field::iterator);
            assertEquals("Cannot iterate over single valued dense_vector field, use get() instead", e.getMessage());
        }
        assertEquals(1, field.size());
        field.setNextDocId(vectors.length);
        DenseVector dv = field.get();
        assertEquals(dv, DenseVector.EMPTY);
    }

    public void testByteMetadataAndIterator() throws IOException {
        int dims = 3;
        float[][] vectors = fill(new float[randomIntBetween(1, 5)][dims], ElementType.BYTE);
        DenseVectorDocValuesField field = new ByteKnnDenseVectorDocValuesField(
            wrap(vectors, ElementType.BYTE),
            "test",
            ElementType.BYTE,
            dims
        );
        for (int i = 0; i < vectors.length; i++) {
            field.setNextDocId(i);
            DenseVector dv = field.get();
            assertEquals(1, dv.size());
            assertFalse(dv.isEmpty());
            assertEquals(dims, dv.getDims());
            UnsupportedOperationException e = expectThrows(UnsupportedOperationException.class, field::iterator);
            assertEquals("Cannot iterate over single valued dense_vector field, use get() instead", e.getMessage());
        }
        assertEquals(1, field.size());
        field.setNextDocId(vectors.length);
        DenseVector dv = field.get();
        assertEquals(dv, DenseVector.EMPTY);
    }

    protected float[][] fill(float[][] vectors, ElementType elementType) {
        for (float[] vector : vectors) {
            for (int i = 0; i < vector.length; i++) {
                vector[i] = elementType == ElementType.FLOAT ? randomFloat() : randomByte();
            }
        }
        return vectors;
    }

    public void testFloatMissingValues() throws IOException {
        int dims = 3;
        float[][] vectors = { { 1, 1, 1 }, { 1, 1, 2 }, { 1, 1, 3 } };
        DenseVectorDocValuesField field = new KnnDenseVectorDocValuesField(
            wrap(vectors, ElementType.FLOAT),
            "test",
            ElementType.FLOAT,
            dims
        );
        DenseVectorScriptDocValues scriptDocValues = field.toScriptDocValues();

        field.setNextDocId(3);
        Exception e = expectThrows(IllegalArgumentException.class, scriptDocValues::getVectorValue);
        assertEquals("A document doesn't have a value for a vector field!", e.getMessage());

        e = expectThrows(IllegalArgumentException.class, scriptDocValues::getMagnitude);
        assertEquals("A document doesn't have a value for a vector field!", e.getMessage());
    }

    public void testByteMissingValues() throws IOException {
        int dims = 3;
        float[][] vectors = { { 1, 1, 1 }, { 1, 1, 2 }, { 1, 1, 3 } };
        DenseVectorDocValuesField field = new ByteKnnDenseVectorDocValuesField(
            wrap(vectors, ElementType.BYTE),
            "test",
            ElementType.BYTE,
            dims
        );
        DenseVectorScriptDocValues scriptDocValues = field.toScriptDocValues();

        field.setNextDocId(3);
        Exception e = expectThrows(IllegalArgumentException.class, scriptDocValues::getVectorValue);
        assertEquals("A document doesn't have a value for a vector field!", e.getMessage());

        e = expectThrows(IllegalArgumentException.class, scriptDocValues::getMagnitude);
        assertEquals("A document doesn't have a value for a vector field!", e.getMessage());
    }

    public void testFloatGetFunctionIsNotAccessible() throws IOException {
        int dims = 3;
        float[][] vectors = { { 1, 1, 1 }, { 1, 1, 2 }, { 1, 1, 3 } };
        DenseVectorDocValuesField field = new KnnDenseVectorDocValuesField(
            wrap(vectors, ElementType.FLOAT),
            "test",
            ElementType.FLOAT,
            dims
        );
        DenseVectorScriptDocValues scriptDocValues = field.toScriptDocValues();

        field.setNextDocId(0);
        Exception e = expectThrows(UnsupportedOperationException.class, () -> scriptDocValues.get(0));
        assertThat(
            e.getMessage(),
            containsString(
                "accessing a vector field's value through 'get' or 'value' is not supported, use 'vectorValue' or 'magnitude' instead."
            )
        );
    }

    public void testByteGetFunctionIsNotAccessible() throws IOException {
        int dims = 3;
        float[][] vectors = { { 1, 1, 1 }, { 1, 1, 2 }, { 1, 1, 3 } };
        DenseVectorDocValuesField field = new ByteKnnDenseVectorDocValuesField(
            wrap(vectors, ElementType.BYTE),
            "test",
            ElementType.BYTE,
            dims
        );
        DenseVectorScriptDocValues scriptDocValues = field.toScriptDocValues();

        field.setNextDocId(0);
        Exception e = expectThrows(UnsupportedOperationException.class, () -> scriptDocValues.get(0));
        assertThat(
            e.getMessage(),
            containsString(
                "accessing a vector field's value through 'get' or 'value' is not supported, use 'vectorValue' or 'magnitude' instead."
            )
        );
    }

    public void testFloatMissingVectorValues() throws IOException {
        int dims = 7;
        DenseVectorDocValuesField emptyKnn = new KnnDenseVectorDocValuesField(null, "test", ElementType.FLOAT, dims);

        emptyKnn.setNextDocId(0);
        assertEquals(0, emptyKnn.toScriptDocValues().size());
        assertTrue(emptyKnn.toScriptDocValues().isEmpty());
        assertEquals(DenseVector.EMPTY, emptyKnn.get());
        assertNull(emptyKnn.get(null));
        assertNull(emptyKnn.getInternal());
        UnsupportedOperationException e = expectThrows(UnsupportedOperationException.class, emptyKnn::iterator);
        assertEquals("Cannot iterate over single valued dense_vector field, use get() instead", e.getMessage());
    }

    public void testByteMissingVectorValues() throws IOException {
        int dims = 7;
        DenseVectorDocValuesField emptyKnn = new ByteKnnDenseVectorDocValuesField(null, "test", ElementType.BYTE, dims);

        emptyKnn.setNextDocId(0);
        assertEquals(0, emptyKnn.toScriptDocValues().size());
        assertTrue(emptyKnn.toScriptDocValues().isEmpty());
        assertEquals(DenseVector.EMPTY, emptyKnn.get());
        assertNull(emptyKnn.get(null));
        assertNull(emptyKnn.getInternal());
        UnsupportedOperationException e = expectThrows(UnsupportedOperationException.class, emptyKnn::iterator);
        assertEquals("Cannot iterate over single valued dense_vector field, use get() instead", e.getMessage());
    }

    public static VectorValues wrap(float[][] vectors, ElementType elementType) {
        return new VectorValues() {
            int index = 0;

            @Override
            public int dimension() {
                return 0;
            }

            @Override
            public int size() {
                return vectors.length;
            }

            @Override
            public float[] vectorValue() {
                return vectors[index];
            }

            @Override
            public BytesRef binaryValue() {
                if (elementType == ElementType.FLOAT) {
                    ByteBuffer byteBuffer = ByteBuffer.allocate(elementType.elementBytes * vectors[index].length);
                    for (float value : vectors[index]) {
                        elementType.writeValue(byteBuffer, value);
                    }
                    return new BytesRef(byteBuffer.array());
                } else if (elementType == ElementType.BYTE) {
                    return VectorUtil.toBytesRef(vectors[index]);
                } else {
                    throw new IllegalStateException("unknown element_type [" + elementType + "]");
                }
            }

            @Override
            public int docID() {
                return index;
            }

            @Override
            public int nextDoc() {
                throw new UnsupportedOperationException();
            }

            @Override
            public int advance(int target) {
                if (target >= size()) {
                    return NO_MORE_DOCS;
                }
                return index = target;
            }

            @Override
            public long cost() {
                return size();
            }
        };
    }
}
