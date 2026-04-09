/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing;

import org.apache.lucene.util.StringHelper;
import org.elasticsearch.common.util.ByteUtils;

/**
 * Hash function based on the Murmur3 algorithm, which is the default as of Elasticsearch 2.0.
 */
public final class Murmur3HashFunction {

    private Murmur3HashFunction() {
        // no instance
    }

    private static final int MAX_SCRATCH_SIZE = 1024;
    private static final ThreadLocal<byte[]> scratch = ThreadLocal.withInitial(() -> new byte[MAX_SCRATCH_SIZE]);

    public static int hash(String routing) {
        assert assertHashWithoutInformationLoss(routing);
        final int strLen = routing.length();
        final byte[] bytesToHash = strLen * 2 <= MAX_SCRATCH_SIZE ? scratch.get() : new byte[strLen * 2];
        for (int i = 0; i < strLen; ++i) {
            ByteUtils.LITTLE_ENDIAN_CHAR.set(bytesToHash, 2 * i, routing.charAt(i));
        }
        return hash(bytesToHash, 0, strLen * 2);
    }

    private static boolean assertHashWithoutInformationLoss(String routing) {
        for (int i = 0; i < routing.length(); ++i) {
            final char c = routing.charAt(i);
            final byte b1 = (byte) c, b2 = (byte) (c >>> 8);
            assert ((b1 & 0xFF) | ((b2 & 0xFF) << 8)) == c; // no information loss
        }
        return true;
    }

    public static int hash(byte[] bytes, int offset, int length) {
        return StringHelper.murmurhash3_x86_32(bytes, offset, length, 0);
    }
}
