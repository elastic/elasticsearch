/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.simdjson;

import org.elasticsearch.simdjson.fieldnames.FieldNameHash;
import org.elasticsearch.simdjson.fieldnames.FieldNameLookup;
import org.elasticsearch.simdjson.internal.BitIndexes;
import org.elasticsearch.simdjson.internal.DoubleParser;
import org.elasticsearch.simdjson.internal.StringParser;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.math.BigInteger;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import static org.elasticsearch.simdjson.internal.CharacterUtils.isStructuralOrWhitespace;

/**
 * Fused stage-2 + token-walk that reads structural indices produced by
 * {@link SimdJsonBatchParser} and emits events to a {@link JsonDocumentHandler}, without
 * building an intermediate representation.
 *
 * <p>Strings go from the source buffer directly into the name cache or as raw byte slices
 * to the handler; numbers are parsed inline.
 *
 * <p>Field name resolution uses a {@link FieldNameLookup} which freezes into a
 * compact hash table after the first document. Lookups use a prefix-8 fast
 * rejection to minimize full key comparisons. Cross-thread sharing happens at batch
 * boundaries via {@link #releaseNames()}.
 *
 * <h2>Lifecycle</h2>
 *
 * <ol>
 *   <li>Create one walker per thread, passing a thread-confined {@link FieldNameLookup}
 *       (e.g. {@link org.elasticsearch.simdjson.fieldnames.FrozenFieldNameTable.Child
 *       FrozenFieldNameTable.Child}).</li>
 *   <li>For each document, call {@link #walkDocument(byte[], int, SimdJsonBatchParser,
 *       JsonDocumentHandler)}. The batch parser must have its document window prepared first.</li>
 *   <li>After processing a batch, call {@link #releaseNames()} to merge newly discovered
 *       field names back to the shared parent table. Omitting this call is safe but means
 *       field names learned in this batch won't be available to other threads.</li>
 *   <li>The walker is reusable across batches - do not create a new instance per batch.</li>
 * </ol>
 *
 * <p><strong>Not thread-safe.</strong> Pool one instance per thread.
 */
public final class SimdJsonDirectWalker {

    private static final int DEFAULT_MAX_DEPTH = 64;

    private final FieldNameLookup nameCache;
    private final StringParser stringParser = new StringParser();
    private final DoubleParser doubleParser = new DoubleParser();
    private final int maxDepth;
    private byte[] stringBuf = new byte[4096];
    private int currentDepth;
    private int docCount;

    public SimdJsonDirectWalker(FieldNameLookup nameCache) {
        this(nameCache, DEFAULT_MAX_DEPTH);
    }

    public SimdJsonDirectWalker(FieldNameLookup nameCache, int maxDepth) {
        this.nameCache = nameCache;
        this.maxDepth = maxDepth;
    }

    /**
     * Walks a single JSON object document using the structural indices from the given batch
     * parser. The parser's read window must already be set to cover this document (via
     * {@link SimdJsonBatchParser#prepareDocumentWindow} or
     * {@link SimdJsonBatchParser#prepareDocumentWindowChunked}).
     *
     * <p>The {@code docLen} parameter is used only for validation; actual byte positions come
     * from the structural indices in the batch parser, which are absolute offsets into
     * {@code buffer}. This means the document does not need to start at offset 0.
     *
     * @param buffer the contiguous byte buffer containing the JSON data
     * @param docLen length of the document in bytes (for bounds context, not offset)
     * @param parser the batch parser with a prepared document window
     * @param handler receives parsed JSON events
     * @throws JsonParsingException if the JSON is malformed or nesting exceeds {@code maxDepth}
     */
    public void walkDocument(byte[] buffer, int docLen, SimdJsonBatchParser parser, JsonDocumentHandler handler) {
        walkDocument(buffer, docLen, parser.bitIndexes(), handler);
    }

    /** Package-private: walks using raw {@link BitIndexes}. */
    void walkDocument(byte[] buffer, int docLen, BitIndexes bitIndexes, JsonDocumentHandler handler) {
        if (bitIndexes.isEnd()) {
            throw new JsonParsingException("No structural element found.");
        }

        int idx = bitIndexes.getAndAdvance();
        if (buffer[idx] != '{') {
            throw new JsonParsingException("Expected document to start with '{' but got '" + (char) buffer[idx] + "'");
        }

        if (buffer[bitIndexes.peek()] == '}') {
            bitIndexes.advance();
            freezeAfterFirstDoc();
            return;
        }

        currentDepth = 0;
        walkObject(buffer, bitIndexes, handler);
        freezeAfterFirstDoc();
    }

    private void freezeAfterFirstDoc() {
        if (docCount++ == 0) {
            nameCache.freeze();
        }
    }

    /**
     * Merges any newly discovered field names back to the shared parent table.
     * Should be called after processing a batch of documents.
     */
    public void releaseNames() {
        nameCache.release();
    }

    private void walkObject(byte[] buffer, BitIndexes bi, JsonDocumentHandler handler) {
        if (++currentDepth > maxDepth) {
            throw new JsonParsingException("Document exceeds maximum nesting depth of " + maxDepth);
        }

        try {
            while (true) {
                int keyIdx = bi.getAndAdvance();
                if (buffer[keyIdx] == '}') {
                    return;
                }
                if (buffer[keyIdx] != '"') {
                    throw new JsonParsingException("Expected field name or '}' but got '" + (char) buffer[keyIdx] + "'");
                }

                String fieldName = resolveFieldName(buffer, keyIdx);

                int colonIdx = bi.getAndAdvance();
                if (buffer[colonIdx] != ':') {
                    throw new JsonParsingException("Missing colon after key in object");
                }

                int valIdx = bi.getAndAdvance();
                byte valByte = buffer[valIdx];

                switch (valByte) {
                    case '{' -> {
                        if (buffer[bi.peek()] == '}') {
                            bi.advance();
                            handler.emptyObject(fieldName);
                        } else {
                            handler.startObject(fieldName);
                            walkObject(buffer, bi, handler);
                            handler.endObject();
                        }
                    }
                    case '[' -> {
                        handler.startArray(fieldName);
                        walkArray(buffer, bi, handler);
                        handler.endArray();
                    }
                    case '"' -> {
                        int off = valIdx + 1;
                        int len = scalarStringLength(buffer, off);
                        boolean hasEscape = containsBackslash(buffer, off, len);
                        if (hasEscape) {
                            int parsed = stringParser.parseString(buffer, valIdx, ensureStringBuf(len));
                            byte[] copy = Arrays.copyOf(stringBuf, parsed);
                            handler.stringField(fieldName, copy, 0, parsed);
                        } else {
                            handler.stringField(fieldName, buffer, off, len);
                        }
                    }
                    case 't' -> {
                        validateTrue(buffer, valIdx);
                        handler.booleanField(fieldName, true, buffer, valIdx, 4);
                    }
                    case 'f' -> {
                        validateFalse(buffer, valIdx);
                        handler.booleanField(fieldName, false, buffer, valIdx, 5);
                    }
                    case 'n' -> {
                        validateNull(buffer, valIdx);
                        handler.nullField(fieldName);
                    }
                    case '-', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9' -> {
                        handleNumber(buffer, valIdx, fieldName, handler);
                    }
                    default -> throw new JsonParsingException("Unexpected value byte: " + (char) valByte);
                }

                int sep = bi.getAndAdvance();
                if (buffer[sep] == '}') {
                    return;
                }
                if (buffer[sep] != ',') {
                    throw new JsonParsingException("Expected ',' or '}' but got '" + (char) buffer[sep] + "'");
                }
            }
        } finally {
            currentDepth--;
        }
    }

    private void walkArray(byte[] buffer, BitIndexes bi, JsonDocumentHandler handler) {
        while (true) {
            int idx = bi.getAndAdvance();
            byte b = buffer[idx];

            if (b == ']') return;

            switch (b) {
                case '"' -> {
                    int off = idx + 1;
                    int len = scalarStringLength(buffer, off);
                    boolean hasEscape = containsBackslash(buffer, off, len);
                    if (hasEscape) {
                        int parsed = stringParser.parseString(buffer, idx, ensureStringBuf(len));
                        handler.arrayElemString(Arrays.copyOf(stringBuf, parsed), 0, parsed);
                    } else {
                        handler.arrayElemString(buffer, off, len);
                    }
                }
                case 't' -> {
                    validateTrue(buffer, idx);
                    handler.arrayElemBoolean(true);
                }
                case 'f' -> {
                    validateFalse(buffer, idx);
                    handler.arrayElemBoolean(false);
                }
                case 'n' -> {
                    validateNull(buffer, idx);
                    handler.arrayElemNull();
                }
                case '{' -> {
                    handler.arrayElemStartObject();
                    if (buffer[bi.peek()] == '}') {
                        bi.advance();
                    } else {
                        walkObjectInArray(buffer, bi, handler);
                    }
                    handler.arrayElemEndObject();
                }
                case '[' -> {
                    handler.arrayElemStartArray();
                    walkArray(buffer, bi, handler);
                    handler.arrayElemEndArray();
                }
                case '-', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9' -> {
                    handleArrayNumber(buffer, idx, handler);
                }
                default -> throw new JsonParsingException("Unexpected byte in array: " + (char) b);
            }

            int sep = bi.getAndAdvance();
            if (buffer[sep] == ']') return;
            if (buffer[sep] != ',') {
                throw new JsonParsingException("Expected ',' or ']' in array but got '" + (char) buffer[sep] + "'");
            }
        }
    }

    /**
     * Walks the fields of an object nested inside an array. Uses the same named field
     * methods on the handler so the handler can serialize them appropriately.
     */
    private void walkObjectInArray(byte[] buffer, BitIndexes bi, JsonDocumentHandler handler) {
        while (true) {
            int keyIdx = bi.getAndAdvance();
            if (buffer[keyIdx] == '}') return;
            if (buffer[keyIdx] != '"') {
                throw new JsonParsingException("Expected field name in nested object");
            }

            int keyStart = keyIdx + 1;
            int keyLen = scalarStringLength(buffer, keyStart);
            boolean keyEscaped = containsBackslash(buffer, keyStart, keyLen);
            String fieldName;
            if (keyEscaped) {
                int parsed = stringParser.parseString(buffer, keyIdx, ensureStringBuf(keyLen));
                fieldName = new String(stringBuf, 0, parsed, StandardCharsets.UTF_8);
            } else {
                fieldName = new String(buffer, keyStart, keyLen, StandardCharsets.UTF_8);
            }

            int colonIdx = bi.getAndAdvance();
            if (buffer[colonIdx] != ':') {
                throw new JsonParsingException("Missing colon in nested object");
            }

            int valIdx = bi.getAndAdvance();
            byte valByte = buffer[valIdx];

            switch (valByte) {
                case '"' -> {
                    int off = valIdx + 1;
                    int len = scalarStringLength(buffer, off);
                    boolean hasEscape = containsBackslash(buffer, off, len);
                    if (hasEscape) {
                        int parsed = stringParser.parseString(buffer, valIdx, ensureStringBuf(len));
                        handler.stringField(fieldName, Arrays.copyOf(stringBuf, parsed), 0, parsed);
                    } else {
                        handler.stringField(fieldName, buffer, off, len);
                    }
                }
                case 't' -> {
                    validateTrue(buffer, valIdx);
                    handler.booleanField(fieldName, true, buffer, valIdx, 4);
                }
                case 'f' -> {
                    validateFalse(buffer, valIdx);
                    handler.booleanField(fieldName, false, buffer, valIdx, 5);
                }
                case 'n' -> {
                    validateNull(buffer, valIdx);
                    handler.nullField(fieldName);
                }
                case '{' -> {
                    if (buffer[bi.peek()] == '}') {
                        bi.advance();
                        handler.emptyObject(fieldName);
                    } else {
                        handler.startObject(fieldName);
                        walkObjectInArray(buffer, bi, handler);
                        handler.endObject();
                    }
                }
                case '[' -> {
                    handler.startArray(fieldName);
                    walkArray(buffer, bi, handler);
                    handler.endArray();
                }
                case '-', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9' -> {
                    handleNumber(buffer, valIdx, fieldName, handler);
                }
                default -> throw new JsonParsingException("Unexpected byte in nested value: " + (char) valByte);
            }

            int sep = bi.getAndAdvance();
            if (buffer[sep] == '}') return;
            if (buffer[sep] != ',') {
                throw new JsonParsingException("Expected ',' or '}' in nested object");
            }
        }
    }

    // ------------------------------------------------------------------
    // Number parsing
    // ------------------------------------------------------------------

    private void handleNumber(byte[] buffer, int idx, String fieldName, JsonDocumentHandler handler) {
        boolean negative = buffer[idx] == '-';
        int pos = negative ? idx + 1 : idx;

        long digits = 0;
        int digitStart = pos;
        int loopBound = buffer.length - 8;

        while (pos <= loopBound) {
            long word = (long) LONG_LE.get(buffer, pos);
            long t = word - 0x3030303030303030L;
            if ((t & 0xF0F0F0F0F0F0F0F0L) != 0) {
                // Not all 8 bytes are digits — parse the leading digits from this word scalar
                break;
            }
            digits = digits * 100_000_000L + parse8Digits(t);
            pos += 8;
        }
        // Scalar tail for remaining digits
        byte ch = buffer[pos];
        while (ch >= '0' && ch <= '9') {
            digits = digits * 10 + (ch - '0');
            ch = buffer[++pos];
        }

        if (ch == '.' || ch == 'e' || ch == 'E') {
            handleFloatingPoint(buffer, idx, negative, digits, pos, fieldName, handler);
            return;
        }

        int digitCount = pos - digitStart;
        if (digitCount == 0) {
            throw new JsonParsingException("Invalid number at " + idx);
        }
        int len = pos - idx;
        if (digitCount >= 19) {
            if (digitCount > 19 || (negative ? digits == Long.MIN_VALUE ? false : digits < 0 : digits < 0)) {
                BigInteger bigVal = new BigInteger(new String(buffer, idx, len, java.nio.charset.StandardCharsets.US_ASCII));
                handler.bigIntegerField(fieldName, bigVal, buffer, idx, len);
                return;
            }
        }

        long val = negative ? -digits : digits;
        boolean fitsInt = val >= Integer.MIN_VALUE && val <= Integer.MAX_VALUE;
        handler.longField(fieldName, val, fitsInt, buffer, idx, len);
    }

    /**
     * Converts 8 pre-validated digit bytes (each 0x00..0x09) packed in a little-endian long
     * into their decimal value. Byte 0 (bits 0-7) is the most significant digit.
     * Uses SWAR pair/quad widening to avoid a per-byte multiply loop.
     */
    private static long parse8Digits(long t) {
        // t has digits 0-9 in each byte, LE order (byte 0 = first/leftmost digit)
        // Step 1: combine adjacent byte-pairs into 16-bit values: d0*10+d1, d2*10+d3, ...
        // even bytes (d0, d2, d4, d6) are at positions 0,16,32,48
        // odd bytes (d1, d3, d5, d7) are at positions 8,24,40,56
        long m1 = (t & 0x00FF00FF00FF00FFL) * 10 + ((t >>> 8) & 0x00FF00FF00FF00FFL);
        // m1 has 4x 16-bit values at positions 0, 16, 32, 48: (d0*10+d1), (d2*10+d3), (d4*10+d5), (d6*10+d7)

        // Step 2: combine adjacent 16-bit pairs into 32-bit values
        long m2 = (m1 & 0x0000FFFF0000FFFFL) * 100 + ((m1 >>> 16) & 0x0000FFFF0000FFFFL);
        // m2 has 2x 32-bit values at positions 0 and 32

        // Step 3: combine into single 64-bit value
        long m3 = (m2 & 0xFFFFFFFFL) * 10000 + (m2 >>> 32);
        return m3;
    }

    private void handleFloatingPoint(
        byte[] buffer,
        int startIdx,
        boolean negative,
        long intDigits,
        int pos,
        String fieldName,
        JsonDocumentHandler handler
    ) {
        int digitsStartIdx = negative ? startIdx + 1 : startIdx;
        long digits = intDigits;
        long exponent = 0;
        int digitCountEnd = pos;

        if (buffer[pos] == '.') {
            pos++;
            int fracStart = pos;
            byte ch = buffer[pos];
            while (ch >= '0' && ch <= '9') {
                digits = digits * 10 + (ch - '0');
                ch = buffer[++pos];
            }
            exponent = fracStart - pos;
            digitCountEnd = pos;
        }

        if (buffer[pos] == 'e' || buffer[pos] == 'E') {
            pos++;
            boolean expNeg = false;
            if (buffer[pos] == '-') {
                expNeg = true;
                pos++;
            } else if (buffer[pos] == '+') {
                pos++;
            }
            long exp = 0;
            byte ch = buffer[pos];
            while (ch >= '0' && ch <= '9') {
                exp = exp * 10 + (ch - '0');
                ch = buffer[++pos];
            }
            exponent += expNeg ? -exp : exp;
        }

        int digitCount = digitCountEnd - digitsStartIdx;
        double val = doubleParser.parse(buffer, startIdx, negative, digitsStartIdx, digitCount, digits, exponent);

        int len = pos - startIdx;
        float fval = (float) val;
        boolean fitsFloat = (double) fval == val;
        handler.doubleField(fieldName, val, fitsFloat, buffer, startIdx, len);
    }

    private void handleArrayNumber(byte[] buffer, int idx, JsonDocumentHandler handler) {
        boolean negative = buffer[idx] == '-';
        int pos = negative ? idx + 1 : idx;

        long digits = 0;
        int digitStart = pos;
        int loopBound = buffer.length - 8;
        while (pos <= loopBound) {
            long word = (long) LONG_LE.get(buffer, pos);
            long t = word - 0x3030303030303030L;
            if ((t & 0xF0F0F0F0F0F0F0F0L) != 0) {
                break;
            }
            digits = digits * 100_000_000L + parse8Digits(t);
            pos += 8;
        }
        byte ch = buffer[pos];
        while (ch >= '0' && ch <= '9') {
            digits = digits * 10 + (ch - '0');
            ch = buffer[++pos];
        }

        if (ch == '.' || ch == 'e' || ch == 'E') {
            long exponent = 0;
            int digitCountEnd = pos;

            if (buffer[pos] == '.') {
                pos++;
                int fracStart = pos;
                ch = buffer[pos];
                while (ch >= '0' && ch <= '9') {
                    digits = digits * 10 + (ch - '0');
                    ch = buffer[++pos];
                }
                exponent = fracStart - pos;
                digitCountEnd = pos;
            }

            if (buffer[pos] == 'e' || buffer[pos] == 'E') {
                pos++;
                boolean expNeg = false;
                if (buffer[pos] == '-') {
                    expNeg = true;
                    pos++;
                } else if (buffer[pos] == '+') {
                    pos++;
                }
                long exp = 0;
                ch = buffer[pos];
                while (ch >= '0' && ch <= '9') {
                    exp = exp * 10 + (ch - '0');
                    ch = buffer[++pos];
                }
                exponent += expNeg ? -exp : exp;
            }

            int digitCount = digitCountEnd - digitStart;
            double val = doubleParser.parse(buffer, idx, negative, digitStart, digitCount, digits, exponent);
            float fval = (float) val;
            handler.arrayElemDouble(val, (double) fval == val);
        } else {
            int digitCount = pos - digitStart;
            if (digitCount >= 19 && (digitCount > 19 || (negative ? digits == Long.MIN_VALUE ? false : digits < 0 : digits < 0))) {
                int len = pos - idx;
                BigInteger bigVal = new BigInteger(new String(buffer, idx, len, java.nio.charset.StandardCharsets.US_ASCII));
                handler.arrayElemBigInteger(bigVal);
            } else {
                long val = negative ? -digits : digits;
                handler.arrayElemLong(val, val >= Integer.MIN_VALUE && val <= Integer.MAX_VALUE);
            }
        }
    }

    // ------------------------------------------------------------------
    // Field name resolution
    // ------------------------------------------------------------------

    private static final long QUOTE_XOR = 0x2222222222222222L;
    private static final long BACKSLASH_XOR = 0x5C5C5C5C5C5C5C5CL;
    private static final long LO_BITS = 0x0101010101010101L;
    private static final long HI_BITS = 0x8080808080808080L;
    private static final VarHandle LONG_LE = MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.LITTLE_ENDIAN);

    private String resolveFieldName(byte[] buffer, int quoteIdx) {
        int start = quoteIdx + 1;
        int pos = start;
        int loopBound = buffer.length - 8;

        if (pos <= loopBound) {
            long word = (long) LONG_LE.get(buffer, pos);
            long xq = word ^ QUOTE_XOR;
            long xb = word ^ BACKSLASH_XOR;
            long qh = (xq - LO_BITS) & ~xq & HI_BITS;
            long bh = (xb - LO_BITS) & ~xb & HI_BITS;

            if ((qh | bh) != 0) {
                if (bh != 0 && (qh == 0 || (Long.numberOfTrailingZeros(bh) <= Long.numberOfTrailingZeros(qh)))) {
                    return resolveEscapedFieldName(buffer, quoteIdx, start);
                }
                int len = Long.numberOfTrailingZeros(qh) >>> 3;
                int h = FieldNameHash.hashWord(word, len);
                long pfx = FieldNameHash.maskWord(word, len);
                String s = nameCache.lookup(buffer, start, len, h, pfx);
                return s != null ? s : nameCache.insert(buffer, start, len, h);
            }
            pos = start + 8;
            while (pos <= loopBound) {
                word = (long) LONG_LE.get(buffer, pos);
                xq = word ^ QUOTE_XOR;
                xb = word ^ BACKSLASH_XOR;
                qh = (xq - LO_BITS) & ~xq & HI_BITS;
                bh = (xb - LO_BITS) & ~xb & HI_BITS;

                if ((qh | bh) != 0) {
                    if (bh != 0 && (qh == 0 || (Long.numberOfTrailingZeros(bh) <= Long.numberOfTrailingZeros(qh)))) {
                        return resolveEscapedFieldName(buffer, quoteIdx, start);
                    }
                    int len = (pos - start) + (Long.numberOfTrailingZeros(qh) >>> 3);
                    int h = FieldNameHash.hashName(buffer, start, len);
                    String s = nameCache.lookup(buffer, start, len, h);
                    return s != null ? s : nameCache.insert(buffer, start, len, h);
                }
                pos += 8;
            }
        }
        return resolveFieldNameScalar(buffer, quoteIdx, start, pos);
    }

    /** Handles field names containing backslash escapes. */
    private String resolveEscapedFieldName(byte[] buffer, int quoteIdx, int start) {
        int end = start;
        while (buffer[end] != '"') {
            if (buffer[end] == '\\') end += 2;
            else end++;
        }
        int parsed = stringParser.parseString(buffer, quoteIdx, ensureStringBuf(end - start));
        int h = FieldNameHash.hashName(stringBuf, 0, parsed);
        String s = nameCache.lookup(stringBuf, 0, parsed, h);
        return s != null ? s : nameCache.insert(stringBuf, 0, parsed, h);
    }

    /** Byte-at-a-time fallback when the field name is near the end of the buffer. */
    private String resolveFieldNameScalar(byte[] buffer, int quoteIdx, int start, int pos) {
        while (true) {
            byte b = buffer[pos];
            if (b == '"') {
                int len = pos - start;
                int h = FieldNameHash.hashName(buffer, start, len);
                String s = nameCache.lookup(buffer, start, len, h);
                return s != null ? s : nameCache.insert(buffer, start, len, h);
            }
            if (b == '\\') {
                return resolveEscapedFieldName(buffer, quoteIdx, start);
            }
            pos++;
        }
    }

    // ------------------------------------------------------------------
    // String helpers
    // ------------------------------------------------------------------

    private static int scalarStringLength(byte[] buffer, int start) {
        int i = start;
        while (buffer[i] != '"') {
            if (buffer[i] == '\\') i += 2;
            else i++;
        }
        return i - start;
    }

    private static boolean containsBackslash(byte[] buffer, int off, int len) {
        for (int i = off; i < off + len; i++) {
            if (buffer[i] == '\\') return true;
        }
        return false;
    }

    private byte[] ensureStringBuf(int minLen) {
        if (stringBuf.length < minLen + 64) {
            stringBuf = new byte[minLen + 64];
        }
        return stringBuf;
    }

    // ------------------------------------------------------------------
    // Atom validation
    // ------------------------------------------------------------------

    private static void validateTrue(byte[] buffer, int idx) {
        if (buffer[idx] != 't'
            || buffer[idx + 1] != 'r'
            || buffer[idx + 2] != 'u'
            || buffer[idx + 3] != 'e'
            || !isStructuralOrWhitespace(buffer[idx + 4])) {
            throw new JsonParsingException("Invalid value at " + idx + ". Expected 'true'.");
        }
    }

    private static void validateFalse(byte[] buffer, int idx) {
        if (buffer[idx] != 'f'
            || buffer[idx + 1] != 'a'
            || buffer[idx + 2] != 'l'
            || buffer[idx + 3] != 's'
            || buffer[idx + 4] != 'e'
            || !isStructuralOrWhitespace(buffer[idx + 5])) {
            throw new JsonParsingException("Invalid value at " + idx + ". Expected 'false'.");
        }
    }

    private static void validateNull(byte[] buffer, int idx) {
        if (buffer[idx] != 'n'
            || buffer[idx + 1] != 'u'
            || buffer[idx + 2] != 'l'
            || buffer[idx + 3] != 'l'
            || !isStructuralOrWhitespace(buffer[idx + 4])) {
            throw new JsonParsingException("Invalid value at " + idx + ". Expected 'null'.");
        }
    }
}
