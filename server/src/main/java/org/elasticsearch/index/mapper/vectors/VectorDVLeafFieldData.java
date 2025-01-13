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
import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.KnnVectorValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.fielddata.FormattedDocValues;
import org.elasticsearch.index.fielddata.LeafFieldData;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper.ElementType;
import org.elasticsearch.script.field.DocValuesScriptFieldFactory;
import org.elasticsearch.script.field.vectors.BinaryDenseVectorDocValuesField;
import org.elasticsearch.script.field.vectors.BitBinaryDenseVectorDocValuesField;
import org.elasticsearch.script.field.vectors.BitKnnDenseVectorDocValuesField;
import org.elasticsearch.script.field.vectors.ByteBinaryDenseVectorDocValuesField;
import org.elasticsearch.script.field.vectors.ByteKnnDenseVectorDocValuesField;
import org.elasticsearch.script.field.vectors.KnnDenseVectorDocValuesField;
import org.elasticsearch.search.DocValueFormat;

import java.io.IOException;
import java.util.Arrays;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

final class VectorDVLeafFieldData implements LeafFieldData {

    private final LeafReader reader;
    private final String field;
    private final IndexVersion indexVersion;
    private final ElementType elementType;
    private final int dims;
    private final boolean indexed;

    VectorDVLeafFieldData(LeafReader reader, String field, IndexVersion indexVersion, ElementType elementType, int dims, boolean indexed) {
        this.reader = reader;
        this.field = field;
        this.indexVersion = indexVersion;
        this.elementType = elementType;
        this.dims = dims;
        this.indexed = indexed;
    }

    @Override
    public long ramBytesUsed() {
        return 0; // not exposed by Lucene
    }

    @Override
    public SortedBinaryDocValues getBytesValues() {
        throw new UnsupportedOperationException("String representation of doc values for vector fields is not supported");
    }

    @Override
    public DocValuesScriptFieldFactory getScriptFieldFactory(String name) {
        try {
            if (indexed) {
                return switch (elementType) {
                    case BYTE -> new ByteKnnDenseVectorDocValuesField(reader.getByteVectorValues(field), name, dims);
                    case FLOAT -> new KnnDenseVectorDocValuesField(reader.getFloatVectorValues(field), name, dims);
                    case BIT -> new BitKnnDenseVectorDocValuesField(reader.getByteVectorValues(field), name, dims);
                };
            } else {
                BinaryDocValues values = DocValues.getBinary(reader, field);
                return switch (elementType) {
                    case BYTE -> new ByteBinaryDenseVectorDocValuesField(values, name, elementType, dims);
                    case FLOAT -> new BinaryDenseVectorDocValuesField(values, name, elementType, dims, indexVersion);
                    case BIT -> new BitBinaryDenseVectorDocValuesField(values, name, elementType, dims);
                };
            }
        } catch (IOException e) {
            throw new IllegalStateException("Cannot load doc values for vector field!", e);
        }
    }

    @Override
    public FormattedDocValues getFormattedValues(DocValueFormat format) {
        int dims = elementType == ElementType.BIT ? this.dims / Byte.SIZE : this.dims;
        return switch (elementType) {
            case BYTE, BIT -> new FormattedDocValues() {
                private byte[] vector = new byte[dims];
                private ByteVectorValues byteVectorValues; // use when indexed
                private KnnVectorValues.DocIndexIterator iterator; // use when indexed
                private BinaryDocValues binary; // use when not indexed
                {
                    try {
                        if (indexed) {
                            byteVectorValues = reader.getByteVectorValues(field);
                            iterator = (byteVectorValues == null) ? null : byteVectorValues.iterator();
                        } else {
                            binary = DocValues.getBinary(reader, field);
                        }
                    } catch (IOException e) {
                        throw new IllegalStateException("Cannot load doc values", e);
                    }

                }

                @Override
                public boolean advanceExact(int docId) throws IOException {
                    if (indexed) {
                        if (iteratorAdvanceExact(iterator, docId) == false) {
                            return false;
                        }
                        vector = byteVectorValues.vectorValue(iterator.index());
                    } else {
                        if (binary == null || binary.advanceExact(docId) == false) {
                            return false;
                        }
                        BytesRef ref = binary.binaryValue();
                        System.arraycopy(ref.bytes, ref.offset, vector, 0, dims);
                    }
                    return true;
                }

                @Override
                public int docValueCount() {
                    return 1;
                }

                public Object nextValue() {
                    Byte[] vectorValue = new Byte[dims];
                    for (int i = 0; i < dims; i++) {
                        vectorValue[i] = vector[i];
                    }
                    return vectorValue;
                }
            };
            case FLOAT -> new FormattedDocValues() {
                float[] vector = new float[dims];
                private FloatVectorValues floatVectorValues; // use when indexed
                private KnnVectorValues.DocIndexIterator iterator; // use when indexed
                private BinaryDocValues binary; // use when not indexed
                {
                    try {
                        if (indexed) {
                            floatVectorValues = reader.getFloatVectorValues(field);
                            iterator = (floatVectorValues == null) ? null : floatVectorValues.iterator();
                        } else {
                            binary = DocValues.getBinary(reader, field);
                        }
                    } catch (IOException e) {
                        throw new IllegalStateException("Cannot load doc values", e);
                    }

                }

                @Override
                public boolean advanceExact(int docId) throws IOException {
                    if (indexed) {
                        if (iteratorAdvanceExact(iterator, docId) == false) {
                            return false;
                        }
                        vector = floatVectorValues.vectorValue(iterator.index());
                    } else {
                        if (binary == null || binary.advanceExact(docId) == false) {
                            return false;
                        }
                        BytesRef ref = binary.binaryValue();
                        VectorEncoderDecoder.decodeDenseVector(indexVersion, ref, vector);
                    }
                    return true;
                }

                @Override
                public int docValueCount() {
                    return 1;
                }

                @Override
                public Object nextValue() {
                    return Arrays.copyOf(vector, vector.length);
                }
            };
        };
    }

    private static boolean iteratorAdvanceExact(KnnVectorValues.DocIndexIterator iterator, int docId) throws IOException {
        if (iterator == null) return false;
        int currentDoc = iterator.docID();
        if (currentDoc == NO_MORE_DOCS || docId < currentDoc) {
            return false;
        } else if (docId > currentDoc) {
            currentDoc = iterator.advance(docId);
            if (currentDoc != docId) {
                return false;
            }
        }
        return true;
    }
}
