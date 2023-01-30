/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.apache.lucene.util.BytesRef;

/**
 * Vector that stores BytesRef values.
 * This class is generated. Do not edit it.
 */
public sealed interface BytesRefVector extends Vector permits ConstantBytesRefVector,FilterBytesRefVector,BytesRefArrayVector {

    BytesRef getBytesRef(int position, BytesRef dest);

    @Override
    BytesRefBlock asBlock();

    @Override
    BytesRefVector filter(int... positions);

    static Builder newVectorBuilder(int estimatedSize) {
        return new BytesRefVectorBuilder(estimatedSize);
    }

    sealed interface Builder extends Vector.Builder permits BytesRefVectorBuilder {
        /**
         * Appends a BytesRef to the current entry.
         */
        Builder appendBytesRef(BytesRef value);

        @Override
        BytesRefVector build();
    }
}
