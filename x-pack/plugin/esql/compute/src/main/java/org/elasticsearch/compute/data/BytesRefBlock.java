/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.util.BytesRefArray;

import java.util.BitSet;

/**
 * Block implementation that stores an array of {@link org.apache.lucene.util.BytesRef}.
 */
final class BytesRefBlock extends AbstractBlock {

    static final BytesRef NULL_VALUE = new BytesRef();

    private final BytesRefArray bytesRefArray;

    BytesRefBlock(BytesRefArray bytesRefArray, int positionCount, int[] firstValueIndexes, BitSet nullsMask) {
        super(positionCount, firstValueIndexes, nullsMask);
        assert bytesRefArray.size() == positionCount : bytesRefArray.size() + " != " + positionCount;
        this.bytesRefArray = bytesRefArray;
    }

    @Override
    public BytesRef getBytesRef(int position, BytesRef spare) {
        return bytesRefArray.get(position, spare);
    }

    @Override
    public Object getObject(int position) {
        return getBytesRef(position, new BytesRef());
    }

    @Override
    public ElementType elementType() {
        return ElementType.BYTES_REF;
    }

    @Override
    public String toString() {
        return "BytesRefBlock[positions=" + getPositionCount() + "]";
    }
}
