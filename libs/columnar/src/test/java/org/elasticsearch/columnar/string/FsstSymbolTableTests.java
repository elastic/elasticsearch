/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.string;

import org.elasticsearch.test.ESTestCase;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public class FsstSymbolTableTests extends ESTestCase {

    public void testRoundTripKnownSymbol() {
        final FsstSymbolTable table = new FsstSymbolTable(new byte[][] { new byte[] { 'a', 'b' } });
        final byte[] src = "ababab".getBytes(StandardCharsets.US_ASCII);
        final byte[] compressed = new byte[src.length * 2];
        final int compLen = table.encode(src, 0, src.length, compressed, 0);

        final byte[] decoded = new byte[src.length];
        final int decLen = table.decode(compressed, 0, compLen, decoded, 0);

        assertArrayEquals(src, Arrays.copyOf(decoded, decLen));
    }

    public void testEmptyValue() {
        final FsstSymbolTable table = new FsstSymbolTable(new byte[][] { new byte[] { 'x' } });
        final byte[] dst = new byte[4];
        final int compLen = table.encode(new byte[0], 0, 0, dst, 0);
        assertEquals(0, compLen);

        final byte[] out = new byte[4];
        final int decLen = table.decode(dst, 0, compLen, out, 0);
        assertEquals(0, decLen);
    }

    public void testAllByteValuesAreEncodableViaEscape() {
        // Build a table with zero symbols so every byte must escape.
        final FsstSymbolTable table = new FsstSymbolTable(new byte[0][]);
        final byte[] src = new byte[256];
        for (int i = 0; i < 256; i++) {
            src[i] = (byte) i;
        }
        final byte[] compressed = new byte[src.length * 2];
        final int compLen = table.encode(src, 0, src.length, compressed, 0);

        final byte[] decoded = new byte[src.length];
        final int decLen = table.decode(compressed, 0, compLen, decoded, 0);

        assertArrayEquals(src, Arrays.copyOf(decoded, decLen));
    }

    public void testEmbeddedZeroBytes() {
        final FsstSymbolTable table = new FsstSymbolTable(new byte[][] { new byte[] { 0, 0 } });
        final byte[] src = new byte[] { 0, 0, 0, 0, 1 };
        final byte[] compressed = new byte[src.length * 2];
        final int compLen = table.encode(src, 0, src.length, compressed, 0);

        final byte[] decoded = new byte[src.length + 2];
        final int decLen = table.decode(compressed, 0, compLen, decoded, 0);

        assertArrayEquals(src, Arrays.copyOf(decoded, decLen));
    }

    public void testSerialiseRoundTrip() {
        final byte[][] symbols = { new byte[] { 'h', 'e', 'l' }, new byte[] { 'l', 'o' }, new byte[] { ' ', 'w' } };
        final FsstSymbolTable original = new FsstSymbolTable(symbols);
        final byte[] buf = new byte[original.serializedSize()];
        original.writeTo(buf, 0);

        final int[] cursor = { 0 };
        final FsstSymbolTable restored = FsstSymbolTable.readFrom(buf, cursor);

        assertEquals("cursor advanced past the table", original.serializedSize(), cursor[0]);
        assertEquals("same number of symbols", original.numSymbols(), restored.numSymbols());

        final byte[] src = "hello world".getBytes(StandardCharsets.US_ASCII);
        final byte[] origComp = new byte[src.length * 2];
        final int origLen = original.encode(src, 0, src.length, origComp, 0);
        final byte[] restComp = new byte[src.length * 2];
        final int restLen = restored.encode(src, 0, src.length, restComp, 0);

        assertEquals("same compressed length", origLen, restLen);
        assertArrayEquals(Arrays.copyOf(origComp, origLen), Arrays.copyOf(restComp, restLen));
    }

    public void testMaxSymbolCount() {
        final byte[][] symbols = new byte[FsstSymbolTable.MAX_SYMBOLS][];
        for (int i = 0; i < FsstSymbolTable.MAX_SYMBOLS; i++) {
            symbols[i] = new byte[] { (byte) i };
        }
        final FsstSymbolTable table = new FsstSymbolTable(symbols);
        final byte[] buf = new byte[table.serializedSize()];
        table.writeTo(buf, 0);

        final int[] cursor = { 0 };
        final FsstSymbolTable restored = FsstSymbolTable.readFrom(buf, cursor);
        assertEquals(FsstSymbolTable.MAX_SYMBOLS, restored.numSymbols());
    }

    public void testMultipleCodesPerValue() {
        final FsstSymbolTable table = new FsstSymbolTable(
            new byte[][] { new byte[] { 'h', 'o', 's', 't' }, new byte[] { '.', 'e', 'l', 'a' }, new byte[] { 's', 't', 'i', 'c' } }
        );
        final byte[] src = "host.elastic".getBytes(StandardCharsets.US_ASCII);
        final byte[] compressed = new byte[src.length * 2];
        final int compLen = table.encode(src, 0, src.length, compressed, 0);

        final byte[] decoded = new byte[src.length * 4];
        final int decLen = table.decode(compressed, 0, compLen, decoded, 0);

        assertArrayEquals(src, Arrays.copyOf(decoded, decLen));
    }
}
