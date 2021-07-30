/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.xpack.core.security.authz.accesscontrol;

import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.RamUsageEstimator;

final class MatchAllRoleBitSet extends BitSet {
    private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(MatchAllRoleBitSet.class);
    private final int length;

    MatchAllRoleBitSet(int length) {
        this.length = length;
    }

    @Override
    public void set(int i) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void clear(int i) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void clear(int startIndex, int endIndex) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int cardinality() {
        return length();
    }

    @Override
    public int prevSetBit(int index) {
        return index;
    }

    @Override
    public int nextSetBit(int index) {
        return index;
    }

    @Override
    public long ramBytesUsed() {
        return BASE_RAM_BYTES_USED;
    }

    @Override
    public boolean get(int index) {
        assert 0 <= index && index < this.length();
        return true;
    }

    @Override
    public int length() {
        return length;
    }
}
