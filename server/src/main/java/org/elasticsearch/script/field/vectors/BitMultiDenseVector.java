/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.script.field.vectors;

import org.apache.lucene.util.BytesRef;

import java.util.List;

public class BitMultiDenseVector extends ByteMultiDenseVector {
    public BitMultiDenseVector(List<byte[]> vectorValues, BytesRef magnitudesBytes, int dims) {
        super(vectorValues, magnitudesBytes, dims);
    }

    @Override
    public void checkDimensions(int qvDims) {
        if (qvDims != dims) {
            throw new IllegalArgumentException(
                "The query vector has a different number of dimensions ["
                    + qvDims * Byte.SIZE
                    + "] than the document vectors ["
                    + dims * Byte.SIZE
                    + "]."
            );
        }
    }

    @Override
    public int getDims() {
        return dims * Byte.SIZE;
    }
}
