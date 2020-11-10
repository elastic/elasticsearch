/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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
