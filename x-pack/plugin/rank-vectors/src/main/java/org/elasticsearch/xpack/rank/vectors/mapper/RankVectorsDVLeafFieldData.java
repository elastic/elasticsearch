/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.rank.vectors.mapper;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.fielddata.FormattedDocValues;
import org.elasticsearch.index.fielddata.LeafFieldData;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.script.field.DocValuesScriptFieldFactory;
import org.elasticsearch.script.field.vectors.BitRankVectorsDocValuesField;
import org.elasticsearch.script.field.vectors.ByteRankVectorsDocValuesField;
import org.elasticsearch.script.field.vectors.FloatRankVectorsDocValuesField;
import org.elasticsearch.script.field.vectors.VectorIterator;
import org.elasticsearch.search.DocValueFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

final class RankVectorsDVLeafFieldData implements LeafFieldData {
    private final LeafReader reader;
    private final String field;
    private final DenseVectorFieldMapper.ElementType elementType;
    private final int dims;

    RankVectorsDVLeafFieldData(LeafReader reader, String field, DenseVectorFieldMapper.ElementType elementType, int dims) {
        this.reader = reader;
        this.field = field;
        this.elementType = elementType;
        this.dims = dims;
    }

    @Override
    public FormattedDocValues getFormattedValues(DocValueFormat format) {
        int dims = elementType == DenseVectorFieldMapper.ElementType.BIT ? this.dims / Byte.SIZE : this.dims;
        return switch (elementType) {
            case BYTE, BIT -> new FormattedDocValues() {
                private final byte[] vector = new byte[dims];
                private BytesRef ref = null;
                private int numVecs = -1;
                private final BinaryDocValues binary;
                {
                    try {
                        binary = DocValues.getBinary(reader, field);
                    } catch (IOException e) {
                        throw new IllegalStateException("Cannot load doc values", e);
                    }
                }

                @Override
                public boolean advanceExact(int docId) throws IOException {
                    if (binary == null || binary.advanceExact(docId) == false) {
                        return false;
                    }
                    ref = binary.binaryValue();
                    assert ref.length % dims == 0;
                    numVecs = ref.length / dims;
                    return true;
                }

                @Override
                public int docValueCount() {
                    return 1;
                }

                public Object nextValue() {
                    // Boxed to keep from `byte[]` being transformed into a string
                    List<Byte[]> vectors = new ArrayList<>(numVecs);
                    VectorIterator<byte[]> iterator = new ByteRankVectorsDocValuesField.ByteVectorIterator(ref, vector, numVecs);
                    while (iterator.hasNext()) {
                        byte[] v = iterator.next();
                        Byte[] vec = new Byte[dims];
                        for (int i = 0; i < dims; i++) {
                            vec[i] = v[i];
                        }
                        vectors.add(vec);
                    }
                    return vectors;
                }
            };
            case FLOAT -> new FormattedDocValues() {
                private final float[] vector = new float[dims];
                private BytesRef ref = null;
                private int numVecs = -1;
                private final BinaryDocValues binary;
                {
                    try {
                        binary = DocValues.getBinary(reader, field);
                    } catch (IOException e) {
                        throw new IllegalStateException("Cannot load doc values", e);
                    }
                }

                @Override
                public boolean advanceExact(int docId) throws IOException {
                    if (binary == null || binary.advanceExact(docId) == false) {
                        return false;
                    }
                    ref = binary.binaryValue();
                    assert ref.length % (Float.BYTES * dims) == 0;
                    numVecs = ref.length / (Float.BYTES * dims);
                    return true;
                }

                @Override
                public int docValueCount() {
                    return 1;
                }

                @Override
                public Object nextValue() {
                    List<float[]> vectors = new ArrayList<>(numVecs);
                    VectorIterator<float[]> iterator = new FloatRankVectorsDocValuesField.FloatVectorIterator(ref, vector, numVecs);
                    while (iterator.hasNext()) {
                        float[] v = iterator.next();
                        vectors.add(Arrays.copyOf(v, v.length));
                    }
                    return vectors;
                }
            };
        };
    }

    @Override
    public DocValuesScriptFieldFactory getScriptFieldFactory(String name) {
        try {
            BinaryDocValues values = DocValues.getBinary(reader, field);
            BinaryDocValues magnitudeValues = DocValues.getBinary(reader, field + RankVectorsFieldMapper.VECTOR_MAGNITUDES_SUFFIX);
            return switch (elementType) {
                case BYTE -> new ByteRankVectorsDocValuesField(values, magnitudeValues, name, elementType, dims);
                case FLOAT -> new FloatRankVectorsDocValuesField(values, magnitudeValues, name, elementType, dims);
                case BIT -> new BitRankVectorsDocValuesField(values, magnitudeValues, name, elementType, dims);
            };
        } catch (IOException e) {
            throw new IllegalStateException("Cannot load doc values for multi-vector field!", e);
        }
    }

    @Override
    public SortedBinaryDocValues getBytesValues() {
        throw new UnsupportedOperationException("String representation of doc values for multi-vector fields is not supported");
    }

    @Override
    public long ramBytesUsed() {
        return 0;
    }
}
