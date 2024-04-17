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

/**
 * Symbols provide more efficient binary de-/serialization for a set of know ASCII constants.
 * <p>
 * On deserialization a symbol can be looked up by its hash code rather than initializing another instance.
 */
public final class Symbol implements Writeable {
    /**
     * Lookup table by name, necessary to provide a performant default implementation for {@link NamedWriteable#getNameSymbol()}.
     * Once replaced, this lookup table is obsolete and can be removed.
     */
    private static final ConcurrentHashMap<String, Symbol> BY_NAME = new ConcurrentHashMap<>();

    /**
     * Lookup table by symbol hash.
     * <p>
     * Collisions are resolved storing symbols in an array per hash using a linear search for lookup.
     * With an expected decent low, constant number of symbols collisions should be fairly unlikely.
     * <p>
     * The estimated size of this table is currently around 150 kb with roughly 500 entries.
     */
    private static final ConcurrentHashMap<Integer, Symbol[]> BY_HASH = new ConcurrentHashMap<>();

    private final byte[] bytes;
    private final int hashCode;

    /**
     * Get or create symbol for a constant.
     * <p>
     * Be careful to not use this unless the string is known to be a constant.
     */
    public static Symbol ofConstant(String constant) {
        return BY_NAME.computeIfAbsent(requireNonNull(constant), n -> create(constant));
    }

    // visible for testing
    static boolean exists(String name) {
        return BY_NAME.containsKey(name);
    }

    /**
     * Lookup the symbol of the given name throwing an {@link IllegalArgumentException} if not found.
     */
    public static Symbol lookupOrThrow(String name) {
        Symbol symbol = BY_NAME.get(name);
        if (symbol == null) {
            throw new IllegalArgumentException("Unknown symbol[" + name + "]");
        }
        return symbol;
    }

    /**
     * Lookup the symbol matching the bytes throwing an {@link IllegalArgumentException} if no match is found.
     */
    public static Symbol lookupOrThrow(byte[] bytes) {
        Symbol[] symbols = BY_HASH.get(Murmur3HashFunction.hash(bytes, 0, bytes.length));
        Symbol symbol = find(symbols, bytes);
        if (symbol == null) {
            throw new IllegalArgumentException("Unknown symbol[" + new String(bytes, ISO_8859_1) + "]");
        }
        return symbol;
    }

    /**
     * Lookup the symbol matching the bytes from start to end index, throwing an {@link IllegalArgumentException} if no match is found.
     */
    public static Symbol lookupOrThrow(byte[] buffer, int start, int end) {
        Symbol[] symbols = BY_HASH.get(Murmur3HashFunction.hash(buffer, start, end - start));
        Symbol symbol = find(symbols, buffer, start, end);
        if (symbol == null) {
            throw new IllegalArgumentException("Unknown symbol[" + new String(buffer, start, end - start, ISO_8859_1) + "]");
        }
        return symbol;
    }

    // visible for testing only
    static Symbol create(String name) {
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

    /**
     * Limit usage to exceptional cases. If toString is used frequently, consider storing the original string.
     */
    @Override
    public String toString() {
        // bytes are valid ASCII, use ISO_8859_1 to not check again
        return new String(bytes, ISO_8859_1);
    }
}
