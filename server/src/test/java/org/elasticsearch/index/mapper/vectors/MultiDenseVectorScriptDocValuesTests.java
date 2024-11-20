/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.vectors;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper.ElementType;
import org.elasticsearch.script.field.vectors.ByteMultiDenseVectorDocValuesField;
import org.elasticsearch.script.field.vectors.FloatMultiDenseVectorDocValuesField;
import org.elasticsearch.script.field.vectors.MultiDenseVector;
import org.elasticsearch.script.field.vectors.MultiDenseVectorDocValuesField;
import org.elasticsearch.test.ESTestCase;
import org.junit.BeforeClass;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Iterator;

import static org.hamcrest.Matchers.containsString;

public class MultiDenseVectorScriptDocValuesTests extends ESTestCase {

    @BeforeClass
    public static void setup() {
        assumeTrue("Requires multi-dense vector support", MultiDenseVectorFieldMapper.FEATURE_FLAG.isEnabled());
    }

    public void testFloatGetVectorValueAndGetMagnitude() throws IOException {
        int dims = 3;
        float[][][] vectors = { { { 1, 1, 1 }, { 1, 1, 2 }, { 1, 1, 3 } }, { { 1, 0, 2 } } };
        float[][] expectedMagnitudes = { { 1.7320f, 2.4495f, 3.3166f }, { 2.2361f } };

        BinaryDocValues docValues = wrap(vectors, ElementType.FLOAT);
        BinaryDocValues magnitudeValues = wrap(expectedMagnitudes);
        MultiDenseVectorDocValuesField field = new FloatMultiDenseVectorDocValuesField(
            docValues,
            magnitudeValues,
            "test",
            ElementType.FLOAT,
            dims
        );
        MultiDenseVectorScriptDocValues scriptDocValues = field.toScriptDocValues();
        for (int i = 0; i < vectors.length; i++) {
            field.setNextDocId(i);
            assertEquals(vectors[i].length, field.size());
            assertEquals(dims, scriptDocValues.dims());
            Iterator<float[]> iterator = scriptDocValues.getVectorValues();
            float[] magnitudes = scriptDocValues.getMagnitudes();
            assertEquals(expectedMagnitudes[i].length, magnitudes.length);
            for (int j = 0; j < vectors[i].length; j++) {
                assertTrue(iterator.hasNext());
                assertArrayEquals(vectors[i][j], iterator.next(), 0.0001f);
                assertEquals(expectedMagnitudes[i][j], magnitudes[j], 0.0001f);
            }
        }
    }

    public void testByteGetVectorValueAndGetMagnitude() throws IOException {
        int dims = 3;
        float[][][] vectors = { { { 1, 1, 1 }, { 1, 1, 2 }, { 1, 1, 3 } }, { { 1, 0, 2 } } };
        float[][] expectedMagnitudes = { { 1.7320f, 2.4495f, 3.3166f }, { 2.2361f } };

        BinaryDocValues docValues = wrap(vectors, ElementType.BYTE);
        BinaryDocValues magnitudeValues = wrap(expectedMagnitudes);
        MultiDenseVectorDocValuesField field = new ByteMultiDenseVectorDocValuesField(
            docValues,
            magnitudeValues,
            "test",
            ElementType.BYTE,
            dims
        );
        MultiDenseVectorScriptDocValues scriptDocValues = field.toScriptDocValues();
        for (int i = 0; i < vectors.length; i++) {
            field.setNextDocId(i);
            assertEquals(vectors[i].length, field.size());
            assertEquals(dims, scriptDocValues.dims());
            Iterator<float[]> iterator = scriptDocValues.getVectorValues();
            float[] magnitudes = scriptDocValues.getMagnitudes();
            assertEquals(expectedMagnitudes[i].length, magnitudes.length);
            for (int j = 0; j < vectors[i].length; j++) {
                assertTrue(iterator.hasNext());
                assertArrayEquals(vectors[i][j], iterator.next(), 0.0001f);
                assertEquals(expectedMagnitudes[i][j], magnitudes[j], 0.0001f);
            }
        }
    }

    public void testFloatMetadataAndIterator() throws IOException {
        int dims = 3;
        float[][][] vectors = new float[][][] { fill(new float[3][dims], ElementType.FLOAT), fill(new float[2][dims], ElementType.FLOAT) };
        float[][] magnitudes = new float[][] { new float[3], new float[2] };
        BinaryDocValues docValues = wrap(vectors, ElementType.FLOAT);
        BinaryDocValues magnitudeValues = wrap(magnitudes);

        MultiDenseVectorDocValuesField field = new FloatMultiDenseVectorDocValuesField(
            docValues,
            magnitudeValues,
            "test",
            ElementType.FLOAT,
            dims
        );
        for (int i = 0; i < vectors.length; i++) {
            field.setNextDocId(i);
            MultiDenseVector dv = field.get();
            assertEquals(vectors[i].length, dv.size());
            assertFalse(dv.isEmpty());
            assertEquals(dims, dv.getDims());
            UnsupportedOperationException e = expectThrows(UnsupportedOperationException.class, field::iterator);
            assertEquals("Cannot iterate over single valued multi_dense_vector field, use get() instead", e.getMessage());
        }
        field.setNextDocId(vectors.length);
        MultiDenseVector dv = field.get();
        assertEquals(dv, MultiDenseVector.EMPTY);
    }

    public void testByteMetadataAndIterator() throws IOException {
        int dims = 3;
        float[][][] vectors = new float[][][] { fill(new float[3][dims], ElementType.BYTE), fill(new float[2][dims], ElementType.BYTE) };
        float[][] magnitudes = new float[][] { new float[3], new float[2] };
        BinaryDocValues docValues = wrap(vectors, ElementType.BYTE);
        BinaryDocValues magnitudeValues = wrap(magnitudes);
        MultiDenseVectorDocValuesField field = new ByteMultiDenseVectorDocValuesField(
            docValues,
            magnitudeValues,
            "test",
            ElementType.BYTE,
            dims
        );
        for (int i = 0; i < vectors.length; i++) {
            field.setNextDocId(i);
            MultiDenseVector dv = field.get();
            assertEquals(vectors[i].length, dv.size());
            assertFalse(dv.isEmpty());
            assertEquals(dims, dv.getDims());
            UnsupportedOperationException e = expectThrows(UnsupportedOperationException.class, field::iterator);
            assertEquals("Cannot iterate over single valued multi_dense_vector field, use get() instead", e.getMessage());
        }
        field.setNextDocId(vectors.length);
        MultiDenseVector dv = field.get();
        assertEquals(dv, MultiDenseVector.EMPTY);
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
        float[][][] vectors = { { { 1, 1, 1 }, { 1, 1, 2 }, { 1, 1, 3 } }, { { 1, 0, 2 } } };
        float[][] magnitudes = { { 1.7320f, 2.4495f, 3.3166f }, { 2.2361f } };
        BinaryDocValues docValues = wrap(vectors, ElementType.FLOAT);
        BinaryDocValues magnitudeValues = wrap(magnitudes);
        MultiDenseVectorDocValuesField field = new FloatMultiDenseVectorDocValuesField(
            docValues,
            magnitudeValues,
            "test",
            ElementType.FLOAT,
            dims
        );
        MultiDenseVectorScriptDocValues scriptDocValues = field.toScriptDocValues();

        field.setNextDocId(3);
        assertEquals(0, field.size());
        Exception e = expectThrows(IllegalArgumentException.class, scriptDocValues::getVectorValues);
        assertEquals("A document doesn't have a value for a multi-vector field!", e.getMessage());

        e = expectThrows(IllegalArgumentException.class, scriptDocValues::getMagnitudes);
        assertEquals("A document doesn't have a value for a multi-vector field!", e.getMessage());
    }

    public void testByteMissingValues() throws IOException {
        int dims = 3;
        float[][][] vectors = { { { 1, 1, 1 }, { 1, 1, 2 }, { 1, 1, 3 } }, { { 1, 0, 2 } } };
        float[][] magnitudes = { { 1.7320f, 2.4495f, 3.3166f }, { 2.2361f } };
        BinaryDocValues docValues = wrap(vectors, ElementType.BYTE);
        BinaryDocValues magnitudeValues = wrap(magnitudes);
        MultiDenseVectorDocValuesField field = new ByteMultiDenseVectorDocValuesField(
            docValues,
            magnitudeValues,
            "test",
            ElementType.BYTE,
            dims
        );
        MultiDenseVectorScriptDocValues scriptDocValues = field.toScriptDocValues();

        field.setNextDocId(3);
        assertEquals(0, field.size());
        Exception e = expectThrows(IllegalArgumentException.class, scriptDocValues::getVectorValues);
        assertEquals("A document doesn't have a value for a multi-vector field!", e.getMessage());

        e = expectThrows(IllegalArgumentException.class, scriptDocValues::getMagnitudes);
        assertEquals("A document doesn't have a value for a multi-vector field!", e.getMessage());
    }

    public void testFloatGetFunctionIsNotAccessible() throws IOException {
        int dims = 3;
        float[][][] vectors = { { { 1, 1, 1 }, { 1, 1, 2 }, { 1, 1, 3 } }, { { 1, 0, 2 } } };
        float[][] magnitudes = { { 1.7320f, 2.4495f, 3.3166f }, { 2.2361f } };
        BinaryDocValues docValues = wrap(vectors, ElementType.FLOAT);
        BinaryDocValues magnitudeValues = wrap(magnitudes);
        MultiDenseVectorDocValuesField field = new FloatMultiDenseVectorDocValuesField(
            docValues,
            magnitudeValues,
            "test",
            ElementType.FLOAT,
            dims
        );
        MultiDenseVectorScriptDocValues scriptDocValues = field.toScriptDocValues();

        field.setNextDocId(0);
        Exception e = expectThrows(UnsupportedOperationException.class, () -> scriptDocValues.get(0));
        assertThat(
            e.getMessage(),
            containsString(
                "accessing a multi-vector field's value through 'get' or 'value' is not supported,"
                    + " use 'vectorValues' or 'magnitudes' instead."
            )
        );
    }

    public void testByteGetFunctionIsNotAccessible() throws IOException {
        int dims = 3;
        float[][][] vectors = { { { 1, 1, 1 }, { 1, 1, 2 }, { 1, 1, 3 } }, { { 1, 0, 2 } } };
        float[][] magnitudes = { { 1.7320f, 2.4495f, 3.3166f }, { 2.2361f } };
        BinaryDocValues docValues = wrap(vectors, ElementType.BYTE);
        BinaryDocValues magnitudeValues = wrap(magnitudes);
        MultiDenseVectorDocValuesField field = new ByteMultiDenseVectorDocValuesField(
            docValues,
            magnitudeValues,
            "test",
            ElementType.BYTE,
            dims
        );
        MultiDenseVectorScriptDocValues scriptDocValues = field.toScriptDocValues();

        field.setNextDocId(0);
        Exception e = expectThrows(UnsupportedOperationException.class, () -> scriptDocValues.get(0));
        assertThat(
            e.getMessage(),
            containsString(
                "accessing a multi-vector field's value through 'get' or 'value' is not supported,"
                    + " use 'vectorValues' or 'magnitudes' instead."
            )
        );
    }

    public static BinaryDocValues wrap(float[][] magnitudes) {
        return new BinaryDocValues() {
            int idx = -1;
            int maxIdx = magnitudes.length;

            @Override
            public BytesRef binaryValue() {
                if (idx >= maxIdx) {
                    throw new IllegalStateException("max index exceeded");
                }
                ByteBuffer magnitudeBuffer = ByteBuffer.allocate(magnitudes[idx].length * Float.BYTES).order(ByteOrder.LITTLE_ENDIAN);
                for (float magnitude : magnitudes[idx]) {
                    magnitudeBuffer.putFloat(magnitude);
                }
                return new BytesRef(magnitudeBuffer.array());
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

    public static BinaryDocValues wrap(float[][][] vectors, ElementType elementType) {
        return new BinaryDocValues() {
            int idx = -1;
            int maxIdx = vectors.length;

            @Override
            public BytesRef binaryValue() {
                if (idx >= maxIdx) {
                    throw new IllegalStateException("max index exceeded");
                }
                return mockEncodeDenseVector(vectors[idx], elementType, IndexVersion.current());
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

    public static BytesRef mockEncodeDenseVector(float[][] values, ElementType elementType, IndexVersion indexVersion) {
        int dims = values[0].length;
        if (elementType == ElementType.BIT) {
            dims *= Byte.SIZE;
        }
        int numBytes = elementType.getNumBytes(dims);
        ByteBuffer byteBuffer = elementType.createByteBuffer(indexVersion, numBytes * values.length);
        for (float[] vector : values) {
            for (float value : vector) {
                if (elementType == ElementType.FLOAT) {
                    byteBuffer.putFloat(value);
                } else if (elementType == ElementType.BYTE || elementType == ElementType.BIT) {
                    byteBuffer.put((byte) value);
                } else {
                    throw new IllegalStateException("unknown element_type [" + elementType + "]");
                }
            }
        }
        return new BytesRef(byteBuffer.array());
    }

}
