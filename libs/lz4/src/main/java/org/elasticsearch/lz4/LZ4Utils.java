/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.lz4;

import static org.elasticsearch.lz4.LZ4Constants.HASH_LOG;
import static org.elasticsearch.lz4.LZ4Constants.HASH_LOG_64K;
import static org.elasticsearch.lz4.LZ4Constants.HASH_LOG_HC;
import static org.elasticsearch.lz4.LZ4Constants.MIN_MATCH;

/*
 * This file is forked from https://github.com/lz4/lz4-java, which is licensed under Apache-2 and Copyright
 * 2020 Adrien Grand and the lz4-java contributors. In particular, it forks the following file
 * net.jpountz.lz4.LZ4Utils.
 *
 * There are no modifications. It is copied to this package for reuse as the original implementation is
 * package private.
 */
enum LZ4Utils {
    ;

    private static final int MAX_INPUT_SIZE = 0x7E000000;

    static int maxCompressedLength(int length) {
        if (length < 0) {
            throw new IllegalArgumentException("length must be >= 0, got " + length);
        } else if (length >= MAX_INPUT_SIZE) {
            throw new IllegalArgumentException("length must be < " + MAX_INPUT_SIZE);
        }
        return length + length / 255 + 16;
    }

    static int hash(int i) {
        return (i * -1640531535) >>> ((MIN_MATCH * 8) - HASH_LOG);
    }

    static int hash64k(int i) {
        return (i * -1640531535) >>> ((MIN_MATCH * 8) - HASH_LOG_64K);
    }

    static int hashHC(int i) {
        return (i * -1640531535) >>> ((MIN_MATCH * 8) - HASH_LOG_HC);
    }

    static class Match {
        int start, ref, len;

        void fix(int correction) {
            start += correction;
            ref += correction;
            len -= correction;
        }

        int end() {
            return start + len;
        }
    }

    static void copyTo(LZ4Utils.Match m1, LZ4Utils.Match m2) {
        m2.len = m1.len;
        m2.start = m1.start;
        m2.ref = m1.ref;
    }
}
