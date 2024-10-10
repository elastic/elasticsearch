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
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.fielddata.DenseVectorNumericByteValues;
import org.elasticsearch.index.fielddata.DenseVectorNumericFloatValues;
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

        return switch (elementType) {
            case BYTE, BIT -> new FormattedDocValues() {
                final DenseVectorNumericByteValues values = getByteValues();

                @Override
                public boolean advanceExact(int docId) throws IOException {
                    return values.advanceExact(docId);
                }

                @Override
                public int docValueCount() throws IOException {
                    return values.docValueCount();
                }

                @Override
                public Object nextValue() throws IOException {
                    return values.nextValue();
                }
            };
            case FLOAT -> new FormattedDocValues() {
                final DenseVectorNumericFloatValues values = getFloatValues();

                @Override
                public boolean advanceExact(int docId) throws IOException {
                    return values.advanceExact(docId);
                }

                @Override
                public int docValueCount() throws IOException {
                    return values.docValueCount();
                }

                @Override
                public Object nextValue() throws IOException {
                    return values.nextValue();
                }
            };
        };
    }

    private DenseVectorNumericByteValues getByteValues() {
        int dims = elementType == ElementType.BIT ? this.dims / Byte.SIZE : this.dims;
        return new DenseVectorNumericByteValues(dims) {
            @Override
            public boolean advanceExact(int docID) throws IOException {
                if (indexed) {
                    final ByteVectorValues byteVectorValues = reader.getByteVectorValues(field);
                    if (byteVectorValues.advance(docID) == NO_MORE_DOCS) {
                        return false;
                    }
                    values = byteVectorValues.vectorValue();
                } else {
                    BinaryDocValues binary = DocValues.getBinary(reader, field);
                    if (binary.advance(docID) == NO_MORE_DOCS) {
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
        };
    }

    private DenseVectorNumericFloatValues getFloatValues() {
        return new DenseVectorNumericFloatValues(dims) {
            @Override
            public boolean advanceExact(int docID) throws IOException {
                if (indexed) {
                    final FloatVectorValues floatVectorValues = reader.getFloatVectorValues(field);
                    if (floatVectorValues.advance(docID) == NO_MORE_DOCS) {
                        return false;
                    }
                    values = floatVectorValues.vectorValue();
                } else {
                    BinaryDocValues binary = DocValues.getBinary(reader, field);
                    if (binary.advance(docID) == NO_MORE_DOCS) {
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
        };
    }
}
