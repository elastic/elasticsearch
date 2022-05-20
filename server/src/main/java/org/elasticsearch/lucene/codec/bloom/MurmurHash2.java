/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.elasticsearch.lucene.codec.bloom;

import org.apache.lucene.util.BytesRef;

/**
 * This is a very fast, non-cryptographic hash suitable for general hash-based lookup. See
 * http://murmurhash.googlepages.com/ for more details.
 *
 * <p>The C version of MurmurHash 2.0 found at that site was ported to Java by Andrzej Bialecki (ab
 * at getopt org).
 *
 * <p>The code from getopt.org was adapted by Mark Harwood in the form here as one of a pluggable
 * choice of hashing functions as the core function had to be adapted to work with BytesRefs with
 * offsets and lengths rather than raw byte arrays.
 *
 * @lucene.experimental
 */
public final class MurmurHash2 extends HashFunction {

    public static final MurmurHash2 INSTANCE = new MurmurHash2();

    private MurmurHash2() {}

    public static int hash(byte[] data, int seed, int offset, int len) {
        int m = 0x5bd1e995;
        int r = 24;
        int h = seed ^ len;
        int len_4 = len >> 2;
        for (int i = 0; i < len_4; i++) {
            int i_4 = offset + (i << 2);
            int k = data[i_4 + 3];
            k = k << 8;
            k = k | (data[i_4 + 2] & 0xff);
            k = k << 8;
            k = k | (data[i_4 + 1] & 0xff);
            k = k << 8;
            k = k | (data[i_4 + 0] & 0xff);
            k *= m;
            k ^= k >>> r;
            k *= m;
            h *= m;
            h ^= k;
        }
        int len_m = len_4 << 2;
        int left = len - len_m;
        if (left != 0) {
            if (left >= 3) {
                h ^= data[offset + len - 3] << 16;
            }
            if (left >= 2) {
                h ^= data[offset + len - 2] << 8;
            }
            if (left >= 1) {
                h ^= data[offset + len - 1];
            }
            h *= m;
        }
        h ^= h >>> 13;
        h *= m;
        h ^= h >>> 15;
        return h;
    }

    /**
     * Generates 32 bit hash from byte array with default seed value.
     *
     * @param data byte array to hash
     * @param offset the start position in the array to hash
     * @param len length of the array elements to hash
     * @return 32 bit hash of the given array
     */
    public static final int hash32(final byte[] data, int offset, int len) {
        return MurmurHash2.hash(data, 0x9747b28c, offset, len);
    }

    @Override
    public final int hash(BytesRef br) {
        return hash32(br.bytes, br.offset, br.length);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName();
    }
}
