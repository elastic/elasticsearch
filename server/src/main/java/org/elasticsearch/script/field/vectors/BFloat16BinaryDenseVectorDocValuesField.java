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
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.index.mapper.vectors.VectorEncoderDecoder;

public class BFloat16BinaryDenseVectorDocValuesField extends BinaryDenseVectorDocValuesField {
    public BFloat16BinaryDenseVectorDocValuesField(
        BinaryDocValues input,
        String name,
        DenseVectorFieldMapper.ElementType elementType,
        int dims,
        IndexVersion indexVersion
    ) {
        super(input, name, elementType, dims, indexVersion);
    }

    @Override
    void decodeDenseVector(IndexVersion indexVersion, BytesRef vectorBR, float[] vector) {
        VectorEncoderDecoder.decodeBFloat16DenseVector(vectorBR, vector);
    }
}
