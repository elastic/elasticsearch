/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script.field.vectors;

import org.apache.lucene.index.ByteVectorValues;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;

public class BitKnnDenseVectorDocValuesField extends ByteKnnDenseVectorDocValuesField {

    public BitKnnDenseVectorDocValuesField(@Nullable ByteVectorValues input, String name, int dims) {
        super(input, name, dims / 8, DenseVectorFieldMapper.ElementType.BIT);
    }

    @Override
    protected DenseVector getVector() {
        return new BitKnnDenseVector(vector);
    }

}
