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
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReader;
import org.elasticsearch.index.fielddata.LeafFieldData;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.script.field.DocValuesScriptFieldFactory;
import org.elasticsearch.script.field.vectors.BitRankVectorsDocValuesField;
import org.elasticsearch.script.field.vectors.ByteRankVectorsDocValuesField;
import org.elasticsearch.script.field.vectors.FloatRankVectorsDocValuesField;

import java.io.IOException;

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
