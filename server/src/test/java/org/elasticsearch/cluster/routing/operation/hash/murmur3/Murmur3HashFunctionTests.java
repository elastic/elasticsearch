/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing.operation.hash.murmur3;

import org.elasticsearch.cluster.routing.Murmur3HashFunction;
import org.elasticsearch.test.ESTestCase;

public class Murmur3HashFunctionTests extends ESTestCase {

    public void testKnownValues() {
        assertHash(0x5a0cb7c3, "hell");
        assertHash(0xd7c31989, "hello");
        assertHash(0x22ab2984, "hello w");
        assertHash(0xdf0ca123, "hello wo");
        assertHash(0xe7744d61, "hello wor");
        assertHash(0xe07db09c, "The quick brown fox jumps over the lazy dog");
        assertHash(0x4e63d2ad, "The quick brown fox jumps over the lazy cog");
    }

    private static void assertHash(int expected, String stringInput) {
        assertEquals(expected, Murmur3HashFunction.hash(stringInput));
    }
}
