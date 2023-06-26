/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper.vectors;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReader;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.fielddata.LeafFieldData;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper.ElementType;
import org.elasticsearch.script.field.DocValuesScriptFieldFactory;
import org.elasticsearch.script.field.vectors.BinaryDenseVectorDocValuesField;
import org.elasticsearch.script.field.vectors.ByteBinaryDenseVectorDocValuesField;
import org.elasticsearch.script.field.vectors.ByteKnnDenseVectorDocValuesField;
import org.elasticsearch.script.field.vectors.KnnDenseVectorDocValuesField;

import java.io.IOException;

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
                };
            } else {
                BinaryDocValues values = DocValues.getBinary(reader, field);
                return switch (elementType) {
                    case BYTE -> new ByteBinaryDenseVectorDocValuesField(values, name, elementType, dims);
                    case FLOAT -> new BinaryDenseVectorDocValuesField(values, name, elementType, dims, indexVersion);
                };
            }
        } catch (IOException e) {
            throw new IllegalStateException("Cannot load doc values for vector field!", e);
        }
    }

    @Override
    public void close() {
        // no-op
    }
}
