/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb.patterntext;

import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.hash.MurmurHash3;
import org.elasticsearch.common.util.ByteUtils;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * Byte-level equivalent of {@link PatternTextValueProcessor#split(String)} for the columnar batch
 * indexing path.
 *
 * <p>Accepts a UTF-8 {@link BytesRef} and scans it directly for the pattern-text delimiters
 * ({@code \t \n \x0B \f \r space [ ]}) without allocating a {@code String}, running a regex, or
 * collecting into {@code List} or {@code StringBuilder}. Results are exposed via {@link BytesRef}
 * views into reused internal scratch buffers; they are valid only until the next call to
 * {@link #split(BytesRef)}.
 *
 * <p>Produces results that are byte-identical to the row path: same template bytes, same
 * {@code templateId}, same base64url-encoded {@code argsInfo}, and the same space-joined args.
 *
 * <h2>Delimiter set</h2>
 * All delimiters are ASCII (≤ 0x7F), so they can never appear as a UTF-8 continuation byte
 * (0x80–0xBF). A raw byte scan therefore cannot false-positive inside a multi-byte character.
 *
 * <h2>arg detection</h2>
 * A token is an arg when {@link Arg#isArg(String)} would return {@code true}: at least one
 * {@code Character.isDigit(char)} call on a UTF-16 code unit of the token returns true. For the
 * byte-level path:
 * <ul>
 *   <li>ASCII bytes 0x30–0x39 ('0'–'9'): fast-path digit check.</li>
 *   <li>2- and 3-byte sequences: decode the code point and apply
 *       {@link Character#isDigit(int)}.</li>
 *   <li>4-byte sequences (supplementary): the two surrogates are never digits, so the result
 *       is always {@code false}, matching {@code Character.isDigit(char surrogate) == false}.
 *   </li>
 * </ul>
 *
 * <h2>{@code Arg.Info} offsets</h2>
 * {@code Arg.Info.offsetInTemplate} is a UTF-16 char offset in the template string. These are
 * tracked incrementally here: ASCII runs and 2-/3-byte runs each contribute one UTF-16 unit per
 * byte sequence, while a 4-byte (supplementary) run contributes two UTF-16 units. The on-disk
 * encoding is therefore bit-identical to the row path's output.
 *
 * <h2>Values over {@value PatternTextValueProcessor#MAX_LOG_LEN_TO_STORE_AS_DOC_VALUE} chars</h2>
 * The length limit is in UTF-16 units. A fast check on the UTF-8 byte count avoids creating a
 * {@code String} for the common case (ASCII log lines, where byte count equals char count). When
 * truncation is required, the method goes through {@code String.substring} + {@code getBytes} to
 * reproduce the row path's lone-surrogate {@code '?'} replacement, then returns
 * {@link Result#LENGTH_EXCEEDED}. Only {@link #templateId()} is valid in that case.
 */
final class PatternTextUtf8Splitter {

    /**
     * Result of a {@link #split(BytesRef)} call.
     */
    enum Result {
        /**
         * The value was processed normally. All getters are valid.
         */
        TEMPLATED,
        /**
         * The value exceeded {@value PatternTextValueProcessor#MAX_LOG_LEN_TO_STORE_AS_DOC_VALUE}
         * UTF-16 chars. Only {@link #templateId()} is valid; the caller must store the full
         * original value as raw text.
         */
        LENGTH_EXCEEDED
    }

    // Lookup table for the 8 delimiter bytes, all of which are < 0x80.
    private static final boolean[] IS_DELIMITER = new boolean[128];
    static {
        IS_DELIMITER['\t'] = true;  // 0x09
        IS_DELIMITER['\n'] = true;  // 0x0A
        IS_DELIMITER[0x0B] = true;  // vertical tab
        IS_DELIMITER['\f'] = true;  // 0x0C
        IS_DELIMITER['\r'] = true;  // 0x0D
        IS_DELIMITER[' '] = true;   // 0x20
        IS_DELIMITER['['] = true;   // 0x5B
        IS_DELIMITER[']'] = true;   // 0x5D
    }

    // Reusable buffers — grown with ArrayUtil.oversize to amortize reallocations.
    private byte[] templateBuf;
    private byte[] hashBuf;       // 8 bytes (MurmurHash3 h1, little-endian)
    private byte[] templateIdBuf; // 11 bytes (base64url of hashBuf, no padding)
    private byte[] argsBuf;       // space-joined arg bytes
    private int[] argOffsets;     // UTF-16 template offsets of each arg
    private byte[] argsInfoRawScratch; // scratch for the binary vint encoding
    private byte[] argsInfoBuf;   // base64url-encoded args info

    // Per-split output lengths.
    private int templateLen;
    private int argsLen;
    private int argsInfoLen;
    private int argCount;

    // Stable BytesRef views into the buffers above.
    private final BytesRef templateRef = new BytesRef();
    private final BytesRef templateIdRef = new BytesRef();
    private final BytesRef argsInfoRef = new BytesRef();
    private final BytesRef joinedArgsRef = new BytesRef();

    PatternTextUtf8Splitter() {
        templateBuf = new byte[256];
        hashBuf = new byte[8];
        templateIdBuf = new byte[11];
        argsBuf = new byte[64];
        argOffsets = new int[8];
        argsInfoRawScratch = new byte[Arg.VINT_MAX_BYTES];
        argsInfoBuf = new byte[8]; // base64url of [0x00] fits in 2 bytes; start small
    }

    /**
     * Splits the UTF-8 value into template and args components.
     *
     * <p>The internally-used {@link java.io.IOException} from {@link Arg#encodeInfoBytes} is a
     * nominal declaration: {@link org.apache.lucene.store.ByteArrayDataOutput} never actually
     * throws when writing to a pre-sized byte array. Any unexpected IOException is wrapped in an
     * {@link UncheckedIOException} so callers need not declare checked exceptions.
     *
     * @return {@link Result#TEMPLATED} when all getters are valid; {@link Result#LENGTH_EXCEEDED}
     *         when the value exceeded the length limit (only {@link #templateId()} is valid)
     */
    Result split(BytesRef utf8) {
        final byte[] src = utf8.bytes;
        final int start = utf8.offset;
        final int end = utf8.offset + utf8.length;

        // Fast path: UTF-16 length ≤ UTF-8 byte length, so if byte length ≤ limit we're safe.
        if (utf8.length <= PatternTextValueProcessor.MAX_LOG_LEN_TO_STORE_AS_DOC_VALUE) {
            return splitBytes(src, start, end);
        }

        // Need to count UTF-16 code units. Decode to String once.
        final String s = new String(src, start, utf8.length, StandardCharsets.UTF_8);
        if (s.length() <= PatternTextValueProcessor.MAX_LOG_LEN_TO_STORE_AS_DOC_VALUE) {
            // Multi-byte-heavy value that fits within the char limit — proceed with byte scan.
            return splitBytes(src, start, end);
        }

        // Value exceeds the limit. Truncate exactly as the row path does:
        // PatternTextValueProcessor.split → CharBuffer.subSequence(0, 8192) → splitInternal
        // → Parts.lengthExceeded(text.toString()) → templateId(text.toString())
        //
        // Going through String.substring / getBytes is deliberate: it faithfully reproduces
        // CharBuffer.subSequence splitting a surrogate pair at index 8192, with the resulting
        // lone surrogate encoded as '?' (0x3F) by the JDK UTF-8 encoder — the same replacement
        // a hand-rolled byte-level truncation would get wrong.
        final byte[] truncated = s.substring(0, PatternTextValueProcessor.MAX_LOG_LEN_TO_STORE_AS_DOC_VALUE)
            .getBytes(StandardCharsets.UTF_8);
        computeTemplateId(truncated, 0, truncated.length);
        // Update only the templateId ref; other refs are not valid for LENGTH_EXCEEDED.
        templateIdRef.bytes = templateIdBuf;
        templateIdRef.offset = 0;
        templateIdRef.length = templateIdBuf.length;
        return Result.LENGTH_EXCEEDED;
    }

    // ── Result accessors ──────────────────────────────────────────────────────────────────────

    /** The template bytes. Only valid when {@link #split} returned {@link Result#TEMPLATED}. */
    BytesRef template() {
        return templateRef;
    }

    /**
     * The 11-byte base64url-encoded template ID (no padding). Valid for both
     * {@link Result#TEMPLATED} and {@link Result#LENGTH_EXCEEDED}.
     */
    BytesRef templateId() {
        return templateIdRef;
    }

    /**
     * The base64url-encoded args-info bytes. Byte-identical to
     * {@link Arg#encodeInfo(java.util.List)} on the same args. Only valid when
     * {@link #split} returned {@link Result#TEMPLATED}.
     */
    BytesRef argsInfo() {
        return argsInfoRef;
    }

    /**
     * The space-joined arg bytes. Only valid when {@link #split} returned
     * {@link Result#TEMPLATED} and {@link #argCount()} is greater than zero.
     */
    BytesRef joinedArgs() {
        return joinedArgsRef;
    }

    /** Number of args in the current result. Only valid when {@link #split} returned {@link Result#TEMPLATED}. */
    int argCount() {
        return argCount;
    }

    // ── Core byte scan ────────────────────────────────────────────────────────────────────────

    private Result splitBytes(byte[] src, int start, int end) {
        templateLen = 0;
        argsLen = 0;
        argCount = 0;
        int templateUtf16Len = 0;

        int pos = start;
        while (pos < end) {
            final int b0 = src[pos] & 0xFF;
            if (b0 < 0x80 && IS_DELIMITER[b0]) {
                // Delimiter: always copies to template. All delimiters are ASCII (1 UTF-16 unit).
                if (templateLen == templateBuf.length) {
                    templateBuf = Arrays.copyOf(templateBuf, ArrayUtil.oversize(templateLen + 1, Byte.BYTES));
                }
                templateBuf[templateLen++] = (byte) b0;
                templateUtf16Len++;
                pos++;
            } else {
                // Non-delimiter run: find its end, check for digit presence.
                final int runStart = pos;
                boolean sawAsciiDigit = false;
                boolean sawNonAscii = false;
                while (pos < end) {
                    final int rb = src[pos] & 0xFF;
                    if (rb < 0x80) {
                        if (IS_DELIMITER[rb]) break;
                        if (rb >= '0' && rb <= '9') sawAsciiDigit = true;
                        pos++;
                    } else {
                        sawNonAscii = true;
                        // Advance past the multi-byte sequence without validating each byte.
                        if ((rb & 0xE0) == 0xC0) {
                            pos += 2; // 2-byte
                        } else if ((rb & 0xF0) == 0xE0) {
                            pos += 3; // 3-byte
                        } else {
                            pos += 4; // 4-byte (supplementary)
                        }
                    }
                }
                final int runEnd = pos;
                final int runLen = runEnd - runStart;

                // A run is an arg when any UTF-16 code unit in it is a Unicode digit.
                // ASCII digit check is the fast path; the slow path handles BMP non-ASCII digits.
                final boolean isArg = sawAsciiDigit || (sawNonAscii && slowIsArg(src, runStart, runEnd));

                if (isArg) {
                    // Record the arg's UTF-16 offset in the (in-progress) template.
                    if (argCount == argOffsets.length) {
                        argOffsets = Arrays.copyOf(argOffsets, ArrayUtil.oversize(argCount + 1, Integer.BYTES));
                    }
                    argOffsets[argCount++] = templateUtf16Len;

                    // Append " arg" to the joined-args buffer (space-separated).
                    final int needed = (argCount > 1 ? 1 : 0) + runLen;
                    if (argsLen + needed > argsBuf.length) {
                        argsBuf = Arrays.copyOf(argsBuf, ArrayUtil.oversize(argsLen + needed, Byte.BYTES));
                    }
                    if (argCount > 1) {
                        argsBuf[argsLen++] = ' ';
                    }
                    System.arraycopy(src, runStart, argsBuf, argsLen, runLen);
                    argsLen += runLen;
                    // templateUtf16Len does NOT advance — the arg is NOT copied into the template.
                } else {
                    // Non-arg run: copy to template and track UTF-16 length.
                    if (templateLen + runLen > templateBuf.length) {
                        templateBuf = Arrays.copyOf(templateBuf, ArrayUtil.oversize(templateLen + runLen, Byte.BYTES));
                    }
                    System.arraycopy(src, runStart, templateBuf, templateLen, runLen);
                    templateLen += runLen;
                    // UTF-16 length: ASCII-only runs contribute runLen units; runs with multi-byte
                    // characters require counting lead bytes explicitly.
                    if (sawNonAscii) {
                        templateUtf16Len += countUtf16Units(src, runStart, runEnd);
                    } else {
                        templateUtf16Len += runLen;
                    }
                }
            }
        }

        computeTemplateId(templateBuf, 0, templateLen);
        encodeArgsInfo();
        updateRefs();
        return Result.TEMPLATED;
    }

    // ── Template ID ──────────────────────────────────────────────────────────────────────────

    private void computeTemplateId(byte[] bytes, int offset, int length) {
        final MurmurHash3.Hash128 hash = new MurmurHash3.Hash128();
        MurmurHash3.hash128(bytes, offset, length, 0, hash);
        ByteUtils.writeLongLE(hash.h1, hashBuf, 0);
        Arg.ENCODER.encode(hashBuf, templateIdBuf);
    }

    // ── Args-info encoding ───────────────────────────────────────────────────────────────────

    private void encodeArgsInfo() {
        // Always encode, even for 0 args: the row path always emits the argsInfo column and
        // Arg.encodeInfo([]) produces base64url([0x00]) = "AA".
        final int rawMax = Arg.VINT_MAX_BYTES + argCount * 2 * Arg.VINT_MAX_BYTES;
        if (argsInfoRawScratch.length < rawMax) {
            argsInfoRawScratch = new byte[rawMax];
        }
        final int base64Max = Arg.argsInfoMaxBase64Bytes(argCount);
        if (argsInfoBuf.length < base64Max) {
            argsInfoBuf = new byte[base64Max];
        }
        try {
            argsInfoLen = Arg.encodeInfoBytes(argOffsets, argCount, argsInfoRawScratch, argsInfoBuf);
        } catch (IOException e) {
            // ByteArrayDataOutput.writeVInt never actually throws — this branch is unreachable.
            throw new UncheckedIOException(e);
        }
    }

    // ── BytesRef view updates ────────────────────────────────────────────────────────────────

    private void updateRefs() {
        templateRef.bytes = templateBuf;
        templateRef.offset = 0;
        templateRef.length = templateLen;

        templateIdRef.bytes = templateIdBuf;
        templateIdRef.offset = 0;
        templateIdRef.length = templateIdBuf.length; // always 11

        argsInfoRef.bytes = argsInfoBuf;
        argsInfoRef.offset = 0;
        argsInfoRef.length = argsInfoLen;

        joinedArgsRef.bytes = argsBuf;
        joinedArgsRef.offset = 0;
        joinedArgsRef.length = argsLen;
    }

    // ── UTF-16 / digit helpers ────────────────────────────────────────────────────────────────

    /**
     * Returns {@code true} if any code point decoded from the UTF-8 bytes {@code src[start, end)}
     * satisfies {@link Character#isDigit(int)}, following the same rules as the row path's
     * {@link Arg#isArg(String)}:
     * <ul>
     *   <li>2- and 3-byte sequences: decoded code point is tested with {@code Character.isDigit(int)}.</li>
     *   <li>4-byte sequences: the two surrogates are not digits, so always {@code false}.</li>
     * </ul>
     * This method is only invoked when the run contains at least one non-ASCII byte.
     */
    private static boolean slowIsArg(byte[] src, int start, int end) {
        int pos = start;
        while (pos < end) {
            final int b0 = src[pos] & 0xFF;
            if (b0 < 0x80) {
                // ASCII bytes are already checked for digit status in the fast path, but handle
                // them here for correctness in case this is called on a mixed run.
                if (Character.isDigit(b0)) return true;
                pos++;
            } else if ((b0 & 0xE0) == 0xC0) {
                final int cp = ((b0 & 0x1F) << 6) | (src[pos + 1] & 0x3F);
                if (Character.isDigit(cp)) return true;
                pos += 2;
            } else if ((b0 & 0xF0) == 0xE0) {
                final int cp = ((b0 & 0x0F) << 12) | ((src[pos + 1] & 0x3F) << 6) | (src[pos + 2] & 0x3F);
                if (Character.isDigit(cp)) return true;
                pos += 3;
            } else {
                // 4-byte (supplementary) sequence. The row path calls Character.isDigit(char)
                // on each UTF-16 code unit; surrogates are never Unicode digits, so skip.
                pos += 4;
            }
        }
        return false;
    }

    /**
     * Counts the number of UTF-16 code units encoded in {@code src[start, end)}.
     * Each ASCII and 2-/3-byte sequence contributes 1 unit; each 4-byte (supplementary) sequence
     * contributes 2 units (a surrogate pair).
     */
    private static int countUtf16Units(byte[] src, int start, int end) {
        int count = 0;
        for (int pos = start; pos < end; pos++) {
            final int b = src[pos] & 0xFF;
            // Count lead bytes (not continuation bytes 0x80–0xBF).
            if ((b & 0xC0) != 0x80) {
                count++;
                if ((b & 0xF8) == 0xF0) {
                    // 4-byte lead: supplementary code point encodes as two UTF-16 units.
                    count++;
                }
            }
        }
        return count;
    }
}
