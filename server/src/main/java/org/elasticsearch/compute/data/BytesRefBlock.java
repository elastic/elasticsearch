/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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
    public Class<?> elementType() {
        return BytesRef.class;
    }

    @Override
    public String toString() {
        return "BytesRefBlock[positions=" + getPositionCount() + "]";
    }
}
