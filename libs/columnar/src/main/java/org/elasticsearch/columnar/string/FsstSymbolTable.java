/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.string;

import java.util.Arrays;

/**
 * An immutable per-block symbol table for FSST compression. Codes 0-254 each name one symbol (a
 * byte sequence of 1 to {@link #MAX_SYMBOL_LENGTH} bytes). Code 255 is the escape: the byte
 * following it is passed through literally, covering byte values no symbol names.
 *
 * <p>This class encodes and decodes individual byte values and serialises/deserialises the table.
 * It knows nothing about blocks, I/O, or symbol selection — those belong to
 * {@link FsstSymbolTableBuilder} and {@link FsstBlockCodec}.
 */
final class FsstSymbolTable {

    /** Escape code: the byte after it is emitted as-is into the output. */
    static final int ESCAPE = 0xFF;

    /** Maximum number of symbols (codes 0 through 254 inclusive). */
    static final int MAX_SYMBOLS = 255;

    /** Maximum byte length of a single symbol. */
    static final int MAX_SYMBOL_LENGTH = 8;

    private final byte[][] symbols;
    // byFirstByte[b] = codes whose first byte is b, sorted longest-first for greedy longest-match
    private final int[][] byFirstByte;

    FsstSymbolTable(byte[][] symbols) {
        this.symbols = symbols;
        this.byFirstByte = buildLookup(symbols);
    }

    int numSymbols() {
        return symbols.length;
    }

    /**
     * Compresses {@code srcLen} bytes from {@code src[srcOff..]} into {@code dst[dstOff..]}.
     * Returns the number of compressed bytes written.
     *
     * <p>The worst-case output size is {@code srcLen * 2}: every input byte emits one escape byte
     * plus the byte itself, so callers must size {@code dst} accordingly.
     */
    int encode(byte[] src, int srcOff, int srcLen, byte[] dst, int dstOff) {
        final int srcEnd = srcOff + srcLen;
        int pos = srcOff;
        int out = dstOff;
        while (pos < srcEnd) {
            final int[] candidates = byFirstByte[src[pos] & 0xFF];
            int matched = -1;
            for (final int code : candidates) {
                final byte[] sym = symbols[code];
                if (pos + sym.length <= srcEnd && matchesAt(src, pos, sym)) {
                    matched = code;
                    break; // sorted longest-first, so the first match is the longest
                }
            }
            if (matched >= 0) {
                dst[out++] = (byte) matched;
                pos += symbols[matched].length;
            } else {
                dst[out++] = (byte) ESCAPE;
                dst[out++] = src[pos++];
            }
        }
        return out - dstOff;
    }

    /**
     * Decompresses {@code srcLen} bytes from {@code src[srcOff..]} into {@code dst[dstOff..]}.
     * Returns the number of decompressed bytes written.
     */
    int decode(byte[] src, int srcOff, int srcLen, byte[] dst, int dstOff) {
        final int srcEnd = srcOff + srcLen;
        int pos = srcOff;
        int out = dstOff;
        while (pos < srcEnd) {
            final int code = src[pos++] & 0xFF;
            if (code == ESCAPE) {
                dst[out++] = src[pos++];
            } else {
                final byte[] sym = symbols[code];
                System.arraycopy(sym, 0, dst, out, sym.length);
                out += sym.length;
            }
        }
        return out - dstOff;
    }

    /**
     * Returns the exact number of bytes {@link #writeTo} will write:
     * 1 byte for the symbol count, then {@code (1 + sym.length)} per symbol.
     */
    int serializedSize() {
        int size = 1; // numSymbols
        for (final byte[] sym : symbols) {
            size += 1 + sym.length;
        }
        return size;
    }

    /**
     * Serialises the symbol table into {@code dst} starting at {@code at}.
     * Returns the number of bytes written (same as {@link #serializedSize()}).
     */
    int writeTo(byte[] dst, int at) {
        dst[at++] = (byte) symbols.length;
        for (final byte[] sym : symbols) {
            dst[at++] = (byte) sym.length;
            System.arraycopy(sym, 0, dst, at, sym.length);
            at += sym.length;
        }
        return serializedSize();
    }

    /**
     * Deserialises a symbol table from {@code src}. {@code cursor[0]} must point at the first byte
     * of the serialised form; on return it is advanced past the symbol table bytes.
     */
    static FsstSymbolTable readFrom(byte[] src, int[] cursor) {
        final int numSymbols = src[cursor[0]++] & 0xFF;
        final byte[][] syms = new byte[numSymbols][];
        for (int i = 0; i < numSymbols; i++) {
            final int len = src[cursor[0]++] & 0xFF;
            syms[i] = Arrays.copyOfRange(src, cursor[0], cursor[0] + len);
            cursor[0] += len;
        }
        return new FsstSymbolTable(syms);
    }

    private static boolean matchesAt(byte[] data, int pos, byte[] sym) {
        for (int k = 0; k < sym.length; k++) {
            if (data[pos + k] != sym[k]) {
                return false;
            }
        }
        return true;
    }

    private static int[][] buildLookup(byte[][] symbols) {
        final int[] counts = new int[256];
        for (final byte[] sym : symbols) {
            counts[sym[0] & 0xFF]++;
        }
        final int[][] result = new int[256][];
        for (int b = 0; b < 256; b++) {
            result[b] = new int[counts[b]];
        }
        final int[] pos = new int[256];
        for (int code = 0; code < symbols.length; code++) {
            final int b = symbols[code][0] & 0xFF;
            result[b][pos[b]++] = code;
        }
        // Sort each per-byte list by symbol length descending so encode() picks the longest match first.
        for (int b = 0; b < 256; b++) {
            final int[] list = result[b];
            // Insertion sort — each list is small (at most MAX_SYMBOLS entries total across 256 buckets).
            for (int i = 1; i < list.length; i++) {
                final int key = list[i];
                final int keyLen = symbols[key].length;
                int j = i - 1;
                while (j >= 0 && symbols[list[j]].length < keyLen) {
                    list[j + 1] = list[j];
                    j--;
                }
                list[j + 1] = key;
            }
        }
        return result;
    }
}
