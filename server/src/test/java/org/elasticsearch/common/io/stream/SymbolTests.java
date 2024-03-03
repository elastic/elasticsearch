/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.io.stream;

import org.elasticsearch.test.ESTestCase;

import static java.nio.charset.StandardCharsets.UTF_8;

public class SymbolTests extends ESTestCase {

    public void testOfConstant() {
        String constant = randomAlphaOfLengthBetween(10, 20);
        String other = randomAlphaOfLengthBetween(10, 20);

        Symbol symbol1 = Symbol.ofConstant(constant);
        Symbol symbol2 = Symbol.ofConstant(constant);
        Symbol symbol3 = Symbol.ofConstant(other);

        assertNotNull(symbol1);
        assertNotNull(symbol2);
        assertNotNull(symbol3);
        assertSame(symbol1, symbol2);
        assertNotSame(symbol1, symbol3);
    }

    public void testLookupWithBytes() {
        String constant = randomAlphaOfLengthBetween(10, 20);

        Symbol symbol1 = Symbol.ofConstant(constant);
        Symbol symbol2 = Symbol.lookupOrThrow(constant.getBytes(UTF_8));

        assertNotNull(symbol1);
        assertNotNull(symbol2);
        assertSame(symbol1, symbol2);

        assertThrows(IllegalArgumentException.class, () -> Symbol.lookupOrThrow(randomAlphaOfLength(20).getBytes(UTF_8)));
    }

    public void testLookupWithBytesAndRange() {
        String constant = randomAlphaOfLengthBetween(10, 20);
        String ignore = randomAlphaOfLengthBetween(0, 20);

        Symbol symbol1 = Symbol.ofConstant(constant);
        Symbol symbol2 = Symbol.lookupOrThrow(constant.getBytes(UTF_8), 0, constant.length());
        Symbol symbol3 = Symbol.lookupOrThrow(
            (ignore + constant + ignore).getBytes(UTF_8),
            ignore.length(),
            ignore.length() + constant.length()
        );

        assertNotNull(symbol1);
        assertNotNull(symbol2);
        assertNotNull(symbol3);
        assertSame(symbol1, symbol2);
        assertSame(symbol2, symbol3);

        assertThrows(
            IllegalArgumentException.class,
            () -> Symbol.lookupOrThrow((constant + "x").getBytes(UTF_8), 0, constant.length() + 1)
        );
    }
}
