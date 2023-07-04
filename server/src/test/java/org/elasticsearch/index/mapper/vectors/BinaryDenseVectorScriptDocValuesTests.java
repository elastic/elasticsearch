/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper.vectors;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper.ElementType;
import org.elasticsearch.script.field.vectors.BinaryDenseVectorDocValuesField;
import org.elasticsearch.script.field.vectors.ByteBinaryDenseVectorDocValuesField;
import org.elasticsearch.script.field.vectors.DenseVector;
import org.elasticsearch.script.field.vectors.DenseVectorDocValuesField;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import static org.hamcrest.Matchers.containsString;

public class BinaryDenseVectorScriptDocValuesTests extends ESTestCase {

    public void testFloatGetVectorValueAndGetMagnitude() throws IOException {
        int dims = 3;
        float[][] vectors = { { 1, 1, 1 }, { 1, 1, 2 }, { 1, 1, 3 } };
        float[] expectedMagnitudes = { 1.7320f, 2.4495f, 3.3166f };

        for (IndexVersion indexVersion : List.of(IndexVersion.V_7_4_0, IndexVersion.current())) {
            BinaryDocValues docValues = wrap(vectors, ElementType.FLOAT, indexVersion);
            DenseVectorDocValuesField field = new BinaryDenseVectorDocValuesField(docValues, "test", ElementType.FLOAT, dims, indexVersion);
            DenseVectorScriptDocValues scriptDocValues = field.toScriptDocValues();
            for (int i = 0; i < vectors.length; i++) {
                field.setNextDocId(i);
                assertEquals(1, field.size());
                assertEquals(dims, scriptDocValues.dims());
                assertArrayEquals(vectors[i], scriptDocValues.getVectorValue(), 0.0001f);
                assertEquals(expectedMagnitudes[i], scriptDocValues.getMagnitude(), 0.0001f);
            }
        }
    }

    public void testByteGetVectorValueAndGetMagnitude() throws IOException {
        int dims = 3;
        float[][] vectors = { { 1, 1, 1 }, { 1, 1, 2 }, { 1, 1, 3 } };
        float[] expectedMagnitudes = { 1.7320f, 2.4495f, 3.3166f };

        BinaryDocValues docValues = wrap(vectors, ElementType.BYTE, IndexVersion.current());
        DenseVectorDocValuesField field = new ByteBinaryDenseVectorDocValuesField(docValues, "test", ElementType.BYTE, dims);
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
        IndexVersion indexVersion = IndexVersion.current();
        float[][] vectors = fill(new float[randomIntBetween(1, 5)][dims], ElementType.FLOAT);
        BinaryDocValues docValues = wrap(vectors, ElementType.FLOAT, indexVersion);
        DenseVectorDocValuesField field = new BinaryDenseVectorDocValuesField(docValues, "test", ElementType.FLOAT, dims, indexVersion);
        for (int i = 0; i < vectors.length; i++) {
            field.setNextDocId(i);
            DenseVector dv = field.get();
            assertEquals(1, dv.size());
            assertFalse(dv.isEmpty());
            assertEquals(dims, dv.getDims());
            UnsupportedOperationException e = expectThrows(UnsupportedOperationException.class, field::iterator);
            assertEquals("Cannot iterate over single valued dense_vector field, use get() instead", e.getMessage());
        }
        field.setNextDocId(vectors.length);
        DenseVector dv = field.get();
        assertEquals(dv, DenseVector.EMPTY);
    }

    public void testByteMetadataAndIterator() throws IOException {
        int dims = 3;
        IndexVersion indexVersion = IndexVersion.current();
        float[][] vectors = fill(new float[randomIntBetween(1, 5)][dims], ElementType.BYTE);
        BinaryDocValues docValues = wrap(vectors, ElementType.BYTE, indexVersion);
        DenseVectorDocValuesField field = new ByteBinaryDenseVectorDocValuesField(docValues, "test", ElementType.BYTE, dims);
        for (int i = 0; i < vectors.length; i++) {
            field.setNextDocId(i);
            DenseVector dv = field.get();
            assertEquals(1, dv.size());
            assertFalse(dv.isEmpty());
            assertEquals(dims, dv.getDims());
            UnsupportedOperationException e = expectThrows(UnsupportedOperationException.class, field::iterator);
            assertEquals("Cannot iterate over single valued dense_vector field, use get() instead", e.getMessage());
        }
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
        BinaryDocValues docValues = wrap(vectors, ElementType.FLOAT, IndexVersion.current());
        DenseVectorDocValuesField field = new BinaryDenseVectorDocValuesField(
            docValues,
            "test",
            ElementType.FLOAT,
            dims,
            IndexVersion.current()
        );
        DenseVectorScriptDocValues scriptDocValues = field.toScriptDocValues();

        field.setNextDocId(3);
        assertEquals(0, field.size());
        Exception e = expectThrows(IllegalArgumentException.class, scriptDocValues::getVectorValue);
        assertEquals("A document doesn't have a value for a vector field!", e.getMessage());

        e = expectThrows(IllegalArgumentException.class, scriptDocValues::getMagnitude);
        assertEquals("A document doesn't have a value for a vector field!", e.getMessage());
    }

    public void testByteMissingValues() throws IOException {
        int dims = 3;
        float[][] vectors = { { 1, 1, 1 }, { 1, 1, 2 }, { 1, 1, 3 } };
        BinaryDocValues docValues = wrap(vectors, ElementType.FLOAT, IndexVersion.current());
        DenseVectorDocValuesField field = new ByteBinaryDenseVectorDocValuesField(docValues, "test", ElementType.BYTE, dims);
        DenseVectorScriptDocValues scriptDocValues = field.toScriptDocValues();

        field.setNextDocId(3);
        assertEquals(0, field.size());
        Exception e = expectThrows(IllegalArgumentException.class, scriptDocValues::getVectorValue);
        assertEquals("A document doesn't have a value for a vector field!", e.getMessage());

        e = expectThrows(IllegalArgumentException.class, scriptDocValues::getMagnitude);
        assertEquals("A document doesn't have a value for a vector field!", e.getMessage());
    }

    public void testFloatGetFunctionIsNotAccessible() throws IOException {
        int dims = 3;
        float[][] vectors = { { 1, 1, 1 }, { 1, 1, 2 }, { 1, 1, 3 } };
        BinaryDocValues docValues = wrap(vectors, ElementType.FLOAT, IndexVersion.current());
        DenseVectorDocValuesField field = new BinaryDenseVectorDocValuesField(
            docValues,
            "test",
            ElementType.FLOAT,
            dims,
            IndexVersion.current()
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
        BinaryDocValues docValues = wrap(vectors, ElementType.BYTE, IndexVersion.current());
        DenseVectorDocValuesField field = new ByteBinaryDenseVectorDocValuesField(docValues, "test", ElementType.BYTE, dims);
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

    public static BinaryDocValues wrap(float[][] vectors, ElementType elementType, IndexVersion indexVersion) {
        return new BinaryDocValues() {
            int idx = -1;
            int maxIdx = vectors.length;

            @Override
            public BytesRef binaryValue() {
                if (idx >= maxIdx) {
                    throw new IllegalStateException("max index exceeded");
                }
                return mockEncodeDenseVector(vectors[idx], elementType, indexVersion);
            }

            @Override
            public boolean advanceExact(int target) {
                idx = target;
                if (target < maxIdx) {
                    return true;
                }
                return false;
            }

            @Override
            public int docID() {
                return idx;
            }

            @Override
            public int nextDoc() {
                return idx++;
            }

            @Override
            public int advance(int target) {
                throw new IllegalArgumentException("not defined!");
            }

            @Override
            public long cost() {
                throw new IllegalArgumentException("not defined!");
            }
        };
    }

    public static BytesRef mockEncodeDenseVector(float[] values, ElementType elementType, IndexVersion indexVersion) {
        int numBytes = indexVersion.onOrAfter(DenseVectorFieldMapper.MAGNITUDE_STORED_INDEX_VERSION)
            ? elementType.elementBytes * values.length + DenseVectorFieldMapper.MAGNITUDE_BYTES
            : elementType.elementBytes * values.length;
        double dotProduct = 0f;
        ByteBuffer byteBuffer = elementType.createByteBuffer(indexVersion, numBytes);
        for (float value : values) {
            if (elementType == ElementType.FLOAT) {
                byteBuffer.putFloat(value);
            } else if (elementType == ElementType.BYTE) {
                byteBuffer.put((byte) value);
            } else {
                throw new IllegalStateException("unknown element_type [" + elementType + "]");
            }
            dotProduct += value * value;
        }

        if (indexVersion.onOrAfter(DenseVectorFieldMapper.MAGNITUDE_STORED_INDEX_VERSION)) {
            // encode vector magnitude at the end
            float vectorMagnitude = (float) Math.sqrt(dotProduct);
            byteBuffer.putFloat(vectorMagnitude);
        }
        return new BytesRef(byteBuffer.array());
    }

}
