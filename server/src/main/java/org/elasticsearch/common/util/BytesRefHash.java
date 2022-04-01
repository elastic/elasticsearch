/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.util;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.hppc.BitMixer;

public final class BytesRefHash extends AbstractBytesRefHash {

    // Constructor with configurable capacity and default maximum load factor.
    public BytesRefHash(long capacity, BigArrays bigArrays) {
        this(capacity, DEFAULT_MAX_LOAD_FACTOR, bigArrays);
    }

    // Constructor with configurable capacity and load factor.
    public BytesRefHash(long capacity, float maxLoadFactor, BigArrays bigArrays) {
        super(capacity, maxLoadFactor, false, bigArrays);
    }

    // BytesRef has a weak hashCode function so we try to improve it by rehashing using Murmur3
    // Feel free to remove rehashing if BytesRef gets a better hash function
    static int rehash(int hash) {
        return BitMixer.mix32(hash);
    }

    @Override
    public long find(BytesRef key) {
        return find(key, rehash(key.hashCode()));
    }

    @Override
    public long add(BytesRef key) {
        return add(key, rehash(key.hashCode()));
    }

}
