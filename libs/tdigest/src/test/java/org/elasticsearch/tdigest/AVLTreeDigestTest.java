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

import org.junit.BeforeClass;

import java.io.IOException;
import java.nio.ByteBuffer;

public class AVLTreeDigestTest extends TDigestTest {
    @BeforeClass
    public static void setup() throws IOException {
        TDigestTest.setup("avl-tree");
    }

    protected DigestFactory factory(final double compression) {
        return new DigestFactory() {
            @Override
            public TDigest create() {
                AVLTreeDigest digest = new AVLTreeDigest(compression);
                digest.setRandomSeed(randomLong());
                return digest;
            }
        };
    }

    @Override
    protected TDigest fromBytes(ByteBuffer bytes) {
        return AVLTreeDigest.fromBytes(bytes);
    }
}
