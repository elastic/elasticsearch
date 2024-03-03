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

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

// FIXME
// - Keep String for toString?
// - Possibly integrate with SerializableString?
// - keep charSequence?
// - unboxed intMap?
public final class Symbol implements Writeable, CharSequence {
    private static final ConcurrentHashMap<String, Symbol> BY_NAME = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<Integer, Symbol[]> BY_HASH = new ConcurrentHashMap<>();

    private final byte[] bytes;
    private final int hashCode;

    public static Symbol ofConstant(String constant) {
        return BY_NAME.computeIfAbsent(requireNonNull(constant), n -> create(constant));
    }

    @Nullable
    public static Symbol lookup(byte[] buffer, int start, int end) {
        Symbol[] symbols = BY_HASH.get(Murmur3HashFunction.hash(buffer, start, end - start));
        return find(symbols, buffer, start, end);
    }

    @Nullable
    public static Symbol lookup(byte[] bytes) {
        Symbol[] symbols = BY_HASH.get(Murmur3HashFunction.hash(bytes, 0, bytes.length));
        return find(symbols, bytes);
    }

    private static Symbol create(String name) {
        final byte[] bytes = name.getBytes(UTF_8);

        // validate that the resulting bytes are compatible with the old write/readString encoding
        // which used {#chars}{bytes} rather than {#bytes}{bytes}
        // FIXME improve check: [0, 127]
        assert bytes.length == name.length() : "only ASCII characters allowed";

        Symbol[] symbols = BY_HASH.compute(
            Murmur3HashFunction.hash(requireNonNull(bytes), 0, bytes.length),
            (hash, all) -> find(all, bytes) == null ? prepend(new Symbol(bytes, hash), all) : all
        );
        if (symbols.length == 1) {
            return symbols[0];
        }
        return find(symbols, bytes); // collisions should be rare
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
    public boolean equals(Object other) {
        if (this == other) return true;

        if (other == null || other instanceof Symbol == false) {
            return false;
        }
        Symbol key = (Symbol) other;
        return hashCode == key.hashCode && Arrays.equals(bytes, key.bytes);
    }

    @Override
    public String toString() {
        return new String(bytes, UTF_8); // avoid!
    }

    @Override
    public int length() {
        return bytes.length;
    }

    @Override
    public char charAt(int index) {
        if (index < 0 || index >= length()) throw new IndexOutOfBoundsException(index);
        return (char) bytes[index];
    }

    @Override
    public CharSequence subSequence(int start, int end) {
        if (start < 0 || start > end || end > length()) {
            throw new IndexOutOfBoundsException("start " + start + ", end " + end + ", length " + length());
        }
        if (start == 0 && end == bytes.length) {
            return this;
        }
        return new SlicedSymbol(this, start, end);
    }

    private static final class SlicedSymbol implements CharSequence {
        private final Symbol symbol;
        private final int start;
        private final int length;

        private SlicedSymbol(Symbol symbol, int start, int end) {
            this.symbol = symbol;
            this.start = start;
            this.length = end - start;
        }

        @Override
        public int length() {
            return length;
        }

        @Override
        public char charAt(int index) {
            if (index < 0 || index >= length) throw new IndexOutOfBoundsException(index);
            return symbol.charAt(start + index);
        }

        @Override
        public CharSequence subSequence(int start, int end) {
            if (start < 0 || start > end || end > symbol.length()) {
                throw new IndexOutOfBoundsException("start " + start + ", end " + end + ", length " + symbol.length());
            }
            return new SlicedSymbol(this.symbol, this.start + start, this.start + end);
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }
            if (other == null || getClass() != other.getClass()) {
                return false;
            }
            SlicedSymbol o = (SlicedSymbol) other;
            return Arrays.equals(symbol.bytes, start, start + length, o.symbol.bytes, o.start, o.start + o.length);
        }

        @Override
        public int hashCode() {
            return Murmur3HashFunction.hash(symbol.bytes, start, length);
        }

        @Override
        public String toString() {
            return new String(symbol.bytes, start, length, UTF_8);
        }
    }
}
