/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.bytes;

import org.elasticsearch.common.util.IntArray;

class ReleasableIntArray implements IntArray {
    private ReleasableBytesReference ref;

    public ReleasableIntArray(ReleasableBytesReference ref) {
        this.ref = ref;
    }

    @Override
    public long size() {
        return ref.length() / 4;
    }

    @Override
    public int get(long index) {
        if (index > Integer.MAX_VALUE / 4) {
            throw new UnsupportedOperationException(); // NOCOMMIT Oh god, what do we do here?
        }
        return ref.getInt((int) index * 4);
    }

    @Override
    public int set(long index, int value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int increment(long index, int inc) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void fill(long fromIndex, long toIndex, int value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void set(long index, byte[] buf, int offset, int len) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long ramBytesUsed() {
        return 0; // How much? 2 objects? I think we'd double count if we used the usage of the bytes ref
    }

    @Override
    public void close() {
        ref.decRef();
        ref = null;
    }
}
