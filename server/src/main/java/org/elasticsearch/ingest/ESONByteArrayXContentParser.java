/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.DeprecationHandler;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.spi.SymbolTable;
import org.elasticsearch.xcontent.spi.XContentProvider;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;

public class ESONByteArrayXContentParser extends ESONXContentParser {

    private static final VarHandle VH_BE_INT = MethodHandles.byteArrayViewVarHandle(int[].class, ByteOrder.BIG_ENDIAN);
    private static final VarHandle VH_BE_SHORT = MethodHandles.byteArrayViewVarHandle(short[].class, ByteOrder.BIG_ENDIAN);

    private final SymbolTable symbolTable;

    private final int lenth;
    private final byte[] bytes;
    private boolean readOpenObject = false;
    private int offset;

    public ESONByteArrayXContentParser(
        BytesRef keyBytes,
        ESONSource.Values values,
        @Nullable SymbolTable stringCache,
        NamedXContentRegistry registry,
        DeprecationHandler deprecationHandler,
        XContentType xContentType
    ) {
        super(values, registry, deprecationHandler, xContentType);
        this.bytes = keyBytes.bytes;
        this.lenth = keyBytes.length;
        this.offset = keyBytes.offset;
        this.symbolTable = XContentProvider.provider().newSymbolTable();
    }

    public static ESONByteArrayXContentParser readFrom(
        BytesReference bytesReference,
        HashMap<BytesRef, String> stringCache,
        NamedXContentRegistry registry,
        DeprecationHandler deprecationHandler,
        XContentType xContentType
    ) {
        int keysLength = bytesReference.getInt(0);
        return new ESONByteArrayXContentParser(
            bytesReference.slice(4, keysLength).toBytesRef(),
            new ESONSource.Values(bytesReference.slice(keysLength + 4 + 4, bytesReference.length() - (keysLength + 4 + 4))),
            null,
            registry,
            deprecationHandler,
            xContentType
        );
    }

    public static ESONByteArrayXContentParser readFrom(
        BytesReference keysReference,
        ESONSource.Values values,
        @Nullable HashMap<BytesRef, String> stringCache,
        NamedXContentRegistry registry,
        DeprecationHandler deprecationHandler,
        XContentType xContentType
    ) {
        return new ESONByteArrayXContentParser(keysReference.toBytesRef(), values, null, registry, deprecationHandler, xContentType);
    }

    @Override
    protected ESONEntry nextEntry() {
        if (readOpenObject) {
            String key;
            if (ESONStack.isObject(containerStack.currentStackValue())) {
                int stringLength = readShortInt();
                key = getString(stringLength);
                offset += stringLength;
            } else {
                key = null;
            }
            byte type = bytes[offset++];
            int offsetOrCount;
            if (type == ESONEntry.TYPE_NULL || type == ESONEntry.TYPE_TRUE || type == ESONEntry.TYPE_FALSE) {
                offsetOrCount = -1;
            } else {
                offsetOrCount = readInt();
            }
            return switch (type) {
                case ESONEntry.TYPE_OBJECT -> new ESONEntry.ObjectEntry(key, offsetOrCount);
                case ESONEntry.TYPE_ARRAY -> new ESONEntry.ArrayEntry(key, offsetOrCount);
                default -> new ESONEntry.FieldEntry(key, type, offsetOrCount);
            };
        } else {
            // Skip field count
            readShortInt();
            byte startType = bytes[offset++];
            assert startType == ESONEntry.TYPE_OBJECT;
            int count = readInt();
            readOpenObject = true;
            return new ESONEntry.ObjectEntry(null, count);
        }
    }

    private String getString(int stringLength) {
        if (symbolTable != null) {
            int[] quads = bytesToQuads(bytes, offset, stringLength);
            int qlen = (stringLength + 3) / 4;

            // Try to find existing string first
            String cached = symbolTable.findName(quads, qlen);
            if (cached != null) {
                return cached;
            }

            // Not found, create new string and add to cache
            String newString = new String(bytes, offset, stringLength, StandardCharsets.UTF_8);
            return symbolTable.addName(newString, quads, qlen);
        } else {
            return new String(bytes, offset, stringLength, StandardCharsets.UTF_8);
        }
    }

    private int[] bytesToQuads(byte[] bytes, int offset, int length) {
        int quadCount = (length + 3) / 4; // Round up
        int[] quads = new int[quadCount];

        for (int i = 0; i < quadCount; i++) {
            int quad = 0;
            int baseOffset = offset + (i * 4);

            for (int j = 0; j < 4; j++) {
                int byteIndex = baseOffset + j;
                if (byteIndex < offset + length) {
                    quad = (quad << 8) | (bytes[byteIndex] & 0xFF);
                } else {
                    quad = quad << 8;
                }
            }
            quads[i] = quad;
        }

        return quads;
    }

    private int readInt() {
        int x = (int) VH_BE_INT.get(bytes, offset);
        offset += 4;
        return x;
    }

    private int readShortInt() {
        short x = (short) VH_BE_SHORT.get(bytes, offset);
        offset += 2;
        if (x >= 0) {
            return x;
        } else {
            short y = (short) VH_BE_SHORT.get(bytes, offset);
            offset += 2;
            int upperBits = (x & 0x7FFF) << 16;
            int lowerBits = Short.toUnsignedInt(y);
            return upperBits | lowerBits;
        }
    }

    public static int readShortInt(InputStream inputStream) throws IOException {
        short x = readShort(inputStream);

        if (x > 0) {
            return x;
        } else {
            short y = readShort(inputStream);

            int upperBits = (x & 0x7FFF) << 16;
            int lowerBits = Short.toUnsignedInt(y);
            return upperBits | lowerBits;
        }
    }

    private static short readShort(InputStream inputStream) throws IOException {
        int b3 = inputStream.read();
        int b4 = inputStream.read();
        if (b3 == -1 || b4 == -1) {
            throw new EOFException("Unexpected end of stream");
        }

        short y = (short) ((b3 << 8) | b4);
        return y;
    }

    @Override
    public void close() {
        super.close();
        symbolTable.close();
    }
}
