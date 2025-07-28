/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.script.field.vectors;

import org.apache.lucene.index.BinaryDocValues;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper.ElementType;

public class BitRankVectorsDocValuesField extends ByteRankVectorsDocValuesField {

    public BitRankVectorsDocValuesField(BinaryDocValues input, BinaryDocValues magnitudes, String name, ElementType elementType, int dims) {
        super(input, magnitudes, name, elementType, dims / 8);
    }

    @Override
    protected RankVectors getVector() {
        return new BitRankVectors(vectorValue, magnitudesValue, numVecs, dims);
    }
}
