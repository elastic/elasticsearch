/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.io.stream;

import org.elasticsearch.cluster.routing.Murmur3HashFunction;
import org.elasticsearch.core.Nullable;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;

import static java.nio.charset.StandardCharsets.ISO_8859_1;
import static java.util.Objects.requireNonNull;

// FIXME
// - Keep String for toString?
// - Possibly integrate with SerializableString?
// - remove BY_NAME map
public final class Symbol implements Writeable {
    private static final ConcurrentHashMap<String, Symbol> BY_NAME = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<Integer, Symbol[]> BY_HASH = new ConcurrentHashMap<>();

    private final byte[] bytes;
    private final int hashCode;

    public static Symbol ofConstant(String constant) {
        return BY_NAME.computeIfAbsent(requireNonNull(constant), n -> create(constant));
    }

    public static Symbol lookupOrThrow(byte[] buffer, int start, int end) {
        Symbol[] symbols = BY_HASH.get(Murmur3HashFunction.hash(buffer, start, end - start));
        Symbol symbol = find(symbols, buffer, start, end);
        if (symbol == null) {
            throw new IllegalArgumentException("Unknown symbol[" + new String(buffer, start, end - start, ISO_8859_1) + "]");
        }
        return symbol;
    }

    public static Symbol lookupOrThrow(byte[] bytes) {
        Symbol[] symbols = BY_HASH.get(Murmur3HashFunction.hash(bytes, 0, bytes.length));
        Symbol symbol = find(symbols, bytes);
        if (symbol == null) {
            throw new IllegalArgumentException("Unknown symbol[" + new String(bytes, ISO_8859_1) + "]");
        }
        return symbol;
    }

    private static Symbol create(String name) {
        // require symbol names to contain only ASCII characters, so they are backwards compatible with
        // write/readString encoding using {#chars}{bytes} rather than {#bytes}{bytes}.
        assert name.length() == name.codePointCount(0, name.length()) : "only ASCII characters allowed";

        // get ISO_8859_1 bytes to check if it's a valid ASCII string
        final byte[] bytes = name.getBytes(ISO_8859_1);
        assert isASCII(bytes) : "only ASCII characters allowed";

        Symbol[] symbols = BY_HASH.compute(
            Murmur3HashFunction.hash(requireNonNull(bytes), 0, bytes.length),
            (hash, current) -> find(current, bytes) == null ? prepend(new Symbol(bytes, hash), current) : current
        );
        if (symbols.length == 1 || symbols[0].bytes == bytes) {
            return symbols[0];
        }
        return find(symbols, bytes); // collisions should be rare
    }

    private static boolean isASCII(byte[] bytes) {
        for (int i = 0; i < bytes.length; i++) {
            if (bytes[i] < 0) {
                return false; // not in range [0,127]
            }
        }
        return true;
    }

    private static Symbol[] prepend(Symbol symbol, @Nullable Symbol[] symbols) {
        if (symbols == null) {
            return new Symbol[] { symbol };
        }
        Symbol[] resized = new Symbol[symbols.length + 1];
        System.arraycopy(symbols, 0, resized, 1, symbols.length);
        resized[0] = symbol;
        return resized;
    }

    @Nullable
    private static Symbol find(@Nullable Symbol[] symbols, byte[] bytes) {
        if (symbols == null) return null;
        for (Symbol s : symbols) {
            if (Arrays.equals(s.bytes, bytes)) return s;
        }
        return null;
    }

    @Nullable
    private static Symbol find(@Nullable Symbol[] symbols, final byte[] bytes, final int start, final int end) {
        if (symbols == null) return null;
        for (Symbol s : symbols) {
            if (Arrays.equals(s.bytes, 0, s.bytes.length, bytes, start, end)) return s;
        }
        return null;
    }

    private Symbol(byte[] bytes, int hashCode) {
        this.bytes = bytes;
        this.hashCode = hashCode;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeByteArray(bytes);
    }

    @Override
    public int hashCode() {
        return hashCode;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Symbol symbol = (Symbol) o;
        return hashCode == symbol.hashCode && Arrays.equals(bytes, symbol.bytes);
    }

    @Override
    public String toString() {
        // bytes are valid ASCII, use ISO_8859_1 to not check again
        return new String(bytes, ISO_8859_1); // avoid!
    }
}
