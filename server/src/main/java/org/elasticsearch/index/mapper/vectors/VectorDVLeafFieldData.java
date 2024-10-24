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
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

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
                private byte[] values;
                private int valuesCursor;
                private ByteVectorValues byteVectorValues; // use when indexed
                private KnnVectorValues.DocIndexIterator iterator; // use when indexed
                private BinaryDocValues binary; // use when not indexed

                @Override
                public boolean advanceExact(int docId) throws IOException {
                    if (indexed) {
                        if (byteVectorValues == null) {
                            this.byteVectorValues = reader.getByteVectorValues(field);
                            this.iterator = byteVectorValues.iterator();
                        }
                        if (iterator.advance(docId) == NO_MORE_DOCS) {
                            return false;
                        }
                        values = byteVectorValues.vectorValue(iterator.index());
                    } else {
                        if (binary == null) {
                            binary = DocValues.getBinary(reader, field);
                        }
                        if (binary.advance(docId) == NO_MORE_DOCS) {
                            return false;
                        }
                        BytesRef ref = binary.binaryValue();
                        ByteBuffer byteBuffer = ByteBuffer.wrap(ref.bytes, ref.offset, ref.length);
                        if (indexVersion.onOrAfter(DenseVectorFieldMapper.LITTLE_ENDIAN_FLOAT_STORED_INDEX_VERSION)) {
                            byteBuffer.order(ByteOrder.LITTLE_ENDIAN);
                        }
                        values = new byte[dims];
                        for (int dim = 0; dim < dims; dim++) {
                            values[dim] = byteBuffer.get();
                        }
                    }
                    valuesCursor = 0;
                    return true;
                }

                @Override
                public int docValueCount() {
                    return dims;
                }

                public Object nextValue() {
                    return values[valuesCursor++];
                }
            };
            case FLOAT -> new FormattedDocValues() {
                private float[] values;
                private int valuesCursor;
                private FloatVectorValues floatVectorValues; // use when indexed
                private KnnVectorValues.DocIndexIterator iterator; // use when indexed
                private BinaryDocValues binary; // use when not indexed

                @Override
                public boolean advanceExact(int docId) throws IOException {
                    if (indexed) {
                        if (floatVectorValues == null) {
                            this.floatVectorValues = reader.getFloatVectorValues(field);
                            this.iterator = floatVectorValues.iterator();
                        }
                        if (iterator.advance(docId) == NO_MORE_DOCS) {
                            return false;
                        }
                        values = floatVectorValues.vectorValue(iterator.index());
                    } else {
                        if (binary == null) {
                            binary = DocValues.getBinary(reader, field);
                        }
                        if (binary.advance(docId) == NO_MORE_DOCS) {
                            return false;
                        }
                        BytesRef ref = binary.binaryValue();
                        ByteBuffer byteBuffer = ByteBuffer.wrap(ref.bytes, ref.offset, ref.length);
                        if (indexVersion.onOrAfter(DenseVectorFieldMapper.LITTLE_ENDIAN_FLOAT_STORED_INDEX_VERSION)) {
                            byteBuffer.order(ByteOrder.LITTLE_ENDIAN);
                        }
                        values = new float[dims];
                        for (int dim = 0; dim < dims; dim++) {
                            values[dim] = byteBuffer.getFloat();
                        }
                    }
                    valuesCursor = 0;
                    return true;
                }

                @Override
                public int docValueCount() {
                    return dims;
                }

                @Override
                public Object nextValue() {
                    return values[valuesCursor++];
                }
            };
        };
    }
}
