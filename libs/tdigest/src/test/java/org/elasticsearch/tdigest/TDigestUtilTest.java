/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 *
 * This project is based on a modification of https://github.com/tdunning/t-digest which is licensed under the Apache 2.0 License.
 */

package org.elasticsearch.tdigest;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.junit.Test;

public class TDigestUtilTest extends AbstractTest {

    @Test
    public void testIntEncoding() {
        Random gen = random();
        ByteBuffer buf = ByteBuffer.allocate(10000);
        List<Integer> ref = new ArrayList<>();
        for (int i = 0; i < 3000; i++) {
            int n = gen.nextInt();
            n = n >>> (i / 100);
            ref.add(n);
            AbstractTDigest.encode(buf, n);
        }

        buf.flip();

        for (int i = 0; i < 3000; i++) {
            int n = AbstractTDigest.decode(buf);
            assertEquals(String.format("%d:", i), ref.get(i).intValue(), n);
        }
    }
}
