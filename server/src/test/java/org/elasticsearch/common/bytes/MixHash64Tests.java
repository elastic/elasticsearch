/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.bytes;

import org.elasticsearch.test.ESTestCase;

import static org.elasticsearch.common.util.PageCacheRecycler.BYTE_PAGE_SIZE;
import static org.hamcrest.Matchers.equalTo;

public class MixHash64Tests extends ESTestCase {

    public void testEmpty() {
        checkHash(new byte[0]);
    }

    public void testSmall() {
        for (int len = 0; len < 1024; len++) {
            byte[] bytes = randomByteArrayOfLength(len);
            checkHash(bytes);
        }
    }

    public void testRandom() {
        for (int i = 0; i < 10; i++) {
            byte[] bytes = randomByteArrayOfLength(between(0, BYTE_PAGE_SIZE * 20));
            checkHash(bytes);
        }
    }

    private void checkHash(byte[] bytes) {
        long h1 = MixHash64.hash64(bytes, 0, bytes.length);
        MixHash64 hasher = new MixHash64();
        int offset = 0;
        while (offset < bytes.length) {
            int len = randomIntBetween(1, bytes.length - offset);
            hasher.update(bytes, offset, len);
            offset += len;
        }
        long h2 = hasher.finish();
        assertThat(h1, equalTo(h2));
    }
}
