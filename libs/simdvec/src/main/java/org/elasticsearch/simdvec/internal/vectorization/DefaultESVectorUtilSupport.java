/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.simdvec.internal.vectorization;

import org.apache.lucene.util.BitUtil;

final class DefaultESVectorUtilSupport implements ESVectorUtilSupport {

    DefaultESVectorUtilSupport() {}

    @Override
    public long ipByteBinByte(byte[] q, byte[] d) {
        return ipByteBinByteImpl(q, d);
    }

    public static long ipByteBinByteImpl(byte[] q, byte[] d) {
        long ret = 0;
        int size = d.length;
        for (int i = 0; i < B_QUERY; i++) {
            int r = 0;
            long subRet = 0;
            for (final int upperBound = d.length & -Integer.BYTES; r < upperBound; r += Integer.BYTES) {
                subRet += Integer.bitCount((int) BitUtil.VH_NATIVE_INT.get(q, i * size + r) & (int) BitUtil.VH_NATIVE_INT.get(d, r));
            }
            for (; r < d.length; r++) {
                subRet += Integer.bitCount((q[i * size + r] & d[r]) & 0xFF);
            }
            ret += subRet << i;
        }
        return ret;
    }
}
