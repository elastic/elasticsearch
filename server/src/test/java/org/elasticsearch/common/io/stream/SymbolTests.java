/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.io.stream;

import org.elasticsearch.test.ESTestCase;
import org.mockito.Mockito;

import static java.nio.charset.StandardCharsets.ISO_8859_1;
import static java.nio.charset.StandardCharsets.US_ASCII;
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

        assertEquals(constant, symbol1.toString());
        assertEquals(constant, symbol2.toString());
        assertEquals(other, symbol3.toString());
    }

    public void testWriteSymbol() throws Exception {
        String constant = randomAlphaOfLengthBetween(10, 20);
        Symbol symbol = Symbol.ofConstant(constant);

        StreamOutput mockOut = Mockito.mock(StreamOutput.class);
        symbol.writeTo(mockOut);

        Mockito.verify(mockOut).writeByteArray(Mockito.eq(constant.getBytes(US_ASCII)));
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

    public void testHashCollision() {
        String s1String = "TPdkLA";
        byte[] s1Bytes = s1String.getBytes(ISO_8859_1);

        String s2String = "VxsG";
        byte[] s2Bytes = s2String.getBytes(ISO_8859_1);

        assumeFalse("Test cannot run repeatedly", Symbol.exists(s1String) || Symbol.exists(s2String));

        Symbol s1 = Symbol.ofConstant(s1String);
        assertThrows(IllegalArgumentException.class, () -> Symbol.lookupOrThrow(s2Bytes));
        assertThrows(IllegalArgumentException.class, () -> Symbol.lookupOrThrow(s2Bytes, 0, s2Bytes.length));

        Symbol s2 = Symbol.ofConstant(s2String);

        assertSame(s1, Symbol.create(s1String));
        assertSame(s2, Symbol.create(s2String));

        // hash collision for s1 and s2
        assertEquals(s1.hashCode(), s2.hashCode());
        assertNotEquals(s1, s2);

        assertSame(s1, Symbol.lookupOrThrow(s1Bytes));
        assertSame(s2, Symbol.lookupOrThrow(s2Bytes));

        assertSame(s1, Symbol.lookupOrThrow(s1Bytes, 0, s1Bytes.length));
        assertSame(s2, Symbol.lookupOrThrow(s2Bytes, 0, s2Bytes.length));
    }

    public void testRejectNonAsciiSymbol() {
        assertThrows(AssertionError.class, () -> Symbol.ofConstant("©"));
        assertThrows(AssertionError.class, () -> Symbol.ofConstant("💩"));
    }
}
