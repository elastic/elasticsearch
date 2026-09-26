/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb.patterntext;

import org.apache.lucene.store.ByteArrayDataOutput;
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
 * indexing path: scans a UTF-8 {@link BytesRef} directly instead of building a {@code String} and
 * running a regex over it. Output is byte-identical to the row path.
 *
 * <p>Results are exposed as {@link BytesRef} views that may point into splitter scratch or
 * <em>into the source bytes</em>. They are valid until the next {@link #split(BytesRef)} call or
 * until the caller's source {@code BytesRef} is invalidated, whichever comes first, so each result
 * must be consumed before calling {@link #split} again.
 *
 * <p>All delimiters ({@code \t \n \x0B \f \r space [ ]}) are ASCII, so they can never appear as a
 * UTF-8 continuation byte and a raw byte scan cannot false-positive mid-character. And the template
 * is exactly the source with the arg runs deleted and every delimiter left in place, so template
 * slices are the complements of the arg runs: the scan records arg boundaries only, and
 * {@link #template()} and {@link #joinedArgs()} point straight into the source whenever the result
 * is a single contiguous slice.
 *
 * <p>{@code Arg.Info.offsetInTemplate} is a UTF-16 offset rather than a byte offset, so the scan
 * also tracks the template's UTF-16 length as it goes.
 */
final class PatternTextUtf8Splitter {

    /** Outcome of a {@link #split(BytesRef)} call. */
    enum Result {
        /** All getters are valid. */
        TEMPLATED,
        /**
         * The value exceeded {@value PatternTextValueProcessor#MAX_LOG_LEN_TO_STORE_AS_DOC_VALUE}
         * UTF-16 chars. Only {@link #templateId()} is valid; the caller stores the raw value instead.
         */
        LENGTH_EXCEEDED
    }

    // Sized 256 rather than 128 so scans can index it with an unsigned byte and skip a `b < 0x80`
    // branch; every entry at or above 0x80 is false.
    private static final boolean[] IS_DELIMITER = new boolean[256];
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

    // Reusable scratch, grown with ArrayUtil.oversize.
    private byte[] templateBuf;         // multi-slice template assembly
    private final byte[] hashBuf;       // 8 bytes: MurmurHash3 h1, little-endian
    private final byte[] templateIdBuf; // 11 bytes: unpadded base64url of hashBuf
    private byte[] argsBuf;             // multi-arg joined-args assembly
    private byte[] argsInfoRawScratch;  // args info as binary vints, pre-base64
    private byte[] argsInfoBuf;         // base64url args info

    // Arg boundaries recorded by the scan. Kept at the same length and grown together.
    private int[] argOffsets;           // UTF-16 offset of each arg within the template
    private int[] argRunStarts;         // byte offset in src where the arg begins
    private int[] argRunEnds;           // byte offset in src where the arg ends (exclusive)

    // Held as fields so that neither is allocated per document.
    private final MurmurHash3.Hash128 templateHash = new MurmurHash3.Hash128();
    private final ByteArrayDataOutput argsInfoOut = new ByteArrayDataOutput();

    private int argsInfoLen;
    private int argCount;

    // Output views; see the class Javadoc for their lifetime.
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
        argRunStarts = new int[8];
        argRunEnds = new int[8];
        argsInfoRawScratch = new byte[Arg.VINT_MAX_BYTES];
        argsInfoBuf = new byte[8]; // base64url of [0x00] fits in 2 bytes; start small
    }

    /**
     * Splits the UTF-8 value into its template and args components.
     *
     * @return {@link Result#TEMPLATED} when all getters are valid, {@link Result#LENGTH_EXCEEDED}
     *         when only {@link #templateId()} is
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

        // Truncate via String.substring / getBytes deliberately: that reproduces the row path's
        // CharBuffer.subSequence splitting a surrogate pair at the limit, leaving a lone surrogate
        // that the JDK UTF-8 encoder replaces with '?' (0x3F). Byte-level truncation gets this wrong.
        // The row path discards the split output for over-long values, so no split is run here.
        final byte[] truncated = s.substring(0, PatternTextValueProcessor.MAX_LOG_LEN_TO_STORE_AS_DOC_VALUE)
            .getBytes(StandardCharsets.UTF_8);
        computeTemplateId(truncated, 0, truncated.length);
        templateIdRef.bytes = templateIdBuf;
        templateIdRef.offset = 0;
        templateIdRef.length = templateIdBuf.length;
        return Result.LENGTH_EXCEEDED;
    }

    /** Template bytes; valid on {@link Result#TEMPLATED}. May point into the source. */
    BytesRef template() {
        return templateRef;
    }

    /** The 11-byte unpadded base64url template ID. Valid on both results. */
    BytesRef templateId() {
        return templateIdRef;
    }

    /**
     * Base64url args info, byte-identical to {@link Arg#encodeInfo(java.util.List)} on the same args.
     * Valid on {@link Result#TEMPLATED}.
     */
    BytesRef argsInfo() {
        return argsInfoRef;
    }

    /**
     * Space-joined args; valid on {@link Result#TEMPLATED} when {@link #argCount()} is non-zero.
     * May point into the source.
     */
    BytesRef joinedArgs() {
        return joinedArgsRef;
    }

    /** Number of args in the current result. */
    int argCount() {
        return argCount;
    }

    private Result splitBytes(byte[] src, int start, int end) {
        argCount = 0;
        int templateUtf16Len = 0;

        int pos = start;
        while (pos < end) {
            if (IS_DELIMITER[src[pos] & 0xFF]) {
                // Delimiters are ASCII, so the run's byte count is also its UTF-16 unit count.
                final int delimStart = pos;
                do {
                    pos++;
                } while (pos < end && IS_DELIMITER[src[pos] & 0xFF]);
                templateUtf16Len += pos - delimStart;
            } else {
                // Non-delimiter run: find its end, noting whether it holds ASCII digits or non-ASCII.
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

                // Non-ASCII runs delegate to Arg.isArg(String), which also yields the UTF-16 length via
                // String.length(). The decode is deferred past sawAsciiDigit because an ASCII digit
                // already settles isArg and that branch never needs the decoded form.
                final boolean isArg;
                String runStr = null;
                if (sawAsciiDigit) {
                    isArg = true;
                } else if (sawNonAscii) {
                    // TODO: Consider if we need to make this work directly on bytes. Pathological non-ASCII data will allocate a
                    // ton of strings.
                    runStr = new String(src, runStart, runLen, StandardCharsets.UTF_8);
                    isArg = Arg.isArg(runStr);
                } else {
                    isArg = false;
                }

                if (isArg) {
                    if (argCount == argOffsets.length) {
                        final int newCap = ArrayUtil.oversize(argCount + 1, Integer.BYTES);
                        argOffsets = Arrays.copyOf(argOffsets, newCap);
                        argRunStarts = Arrays.copyOf(argRunStarts, newCap);
                        argRunEnds = Arrays.copyOf(argRunEnds, newCap);
                    }
                    argOffsets[argCount] = templateUtf16Len;
                    argRunStarts[argCount] = runStart;
                    argRunEnds[argCount] = runEnd;
                    argCount++;
                    // templateUtf16Len does not advance: the arg is not part of the template.
                } else {
                    templateUtf16Len += runStr != null ? runStr.length() : runLen;
                }
            }
        }

        materializeTemplate(src, start, end);
        materializeArgs(src);
        encodeArgsInfo();

        templateIdRef.bytes = templateIdBuf;
        templateIdRef.offset = 0;
        templateIdRef.length = templateIdBuf.length; // always 11

        argsInfoRef.bytes = argsInfoBuf;
        argsInfoRef.offset = 0;
        argsInfoRef.length = argsInfoLen;

        return Result.TEMPLATED;
    }

    /**
     * Sets {@link #templateRef} to the complement of the arg runs. When that complement is a single
     * contiguous slice the ref points into {@code src} and nothing is copied; otherwise the slices are
     * assembled into {@link #templateBuf}.
     */
    private void materializeTemplate(byte[] src, int start, int end) {
        int nonEmptySlices = 0;
        int singleSliceStart = start; // the sole non-empty slice, when there is only one
        int singleSliceEnd = start;
        int lastArgEnd = start;
        for (int i = 0; i < argCount; i++) {
            final int sliceEnd = argRunStarts[i];
            if (sliceEnd > lastArgEnd) {
                nonEmptySlices++;
                singleSliceStart = lastArgEnd;
                singleSliceEnd = sliceEnd;
            }
            lastArgEnd = argRunEnds[i];
        }
        if (end > lastArgEnd) {
            nonEmptySlices++;
            singleSliceStart = lastArgEnd;
            singleSliceEnd = end;
        }

        if (nonEmptySlices <= 1) {
            // Zero copies: hash straight out of src, leaving templateBuf untouched.
            computeTemplateId(src, singleSliceStart, singleSliceEnd - singleSliceStart);
            templateRef.bytes = src;
            templateRef.offset = singleSliceStart;
            templateRef.length = singleSliceEnd - singleSliceStart;
        } else {
            int totalLen = 0;
            lastArgEnd = start;
            for (int i = 0; i < argCount; i++) {
                totalLen += argRunStarts[i] - lastArgEnd;
                lastArgEnd = argRunEnds[i];
            }
            totalLen += end - lastArgEnd;

            if (totalLen > templateBuf.length) {
                templateBuf = new byte[ArrayUtil.oversize(totalLen, Byte.BYTES)];
            }
            int written = 0;
            lastArgEnd = start;
            for (int i = 0; i < argCount; i++) {
                final int sliceLen = argRunStarts[i] - lastArgEnd;
                if (sliceLen > 0) {
                    System.arraycopy(src, lastArgEnd, templateBuf, written, sliceLen);
                    written += sliceLen;
                }
                lastArgEnd = argRunEnds[i];
            }
            final int tailLen = end - lastArgEnd;
            if (tailLen > 0) {
                System.arraycopy(src, lastArgEnd, templateBuf, written, tailLen);
                written += tailLen;
            }

            computeTemplateId(templateBuf, 0, written);
            templateRef.bytes = templateBuf;
            templateRef.offset = 0;
            templateRef.length = written;
        }
    }

    /**
     * Sets {@link #joinedArgsRef}. A single arg points into {@code src}; multiple args are joined into
     * {@link #argsBuf} with one space between each pair.
     */
    private void materializeArgs(byte[] src) {
        if (argCount == 0) {
            joinedArgsRef.bytes = argsBuf; // valid non-null backing; length is 0
            joinedArgsRef.offset = 0;
            joinedArgsRef.length = 0;
        } else if (argCount == 1) {
            joinedArgsRef.bytes = src;
            joinedArgsRef.offset = argRunStarts[0];
            joinedArgsRef.length = argRunEnds[0] - argRunStarts[0];
        } else {
            int totalLen = argCount - 1; // space separators
            for (int i = 0; i < argCount; i++) {
                totalLen += argRunEnds[i] - argRunStarts[i];
            }
            if (totalLen > argsBuf.length) {
                argsBuf = new byte[ArrayUtil.oversize(totalLen, Byte.BYTES)];
            }
            int written = 0;
            for (int i = 0; i < argCount; i++) {
                if (i > 0) argsBuf[written++] = ' ';
                final int runLen = argRunEnds[i] - argRunStarts[i];
                System.arraycopy(src, argRunStarts[i], argsBuf, written, runLen);
                written += runLen;
            }
            joinedArgsRef.bytes = argsBuf;
            joinedArgsRef.offset = 0;
            joinedArgsRef.length = written;
        }
    }

    private void computeTemplateId(byte[] bytes, int offset, int length) {
        MurmurHash3.hash128(bytes, offset, length, 0, templateHash);
        ByteUtils.writeLongLE(templateHash.h1, hashBuf, 0);
        Arg.ENCODER.encode(hashBuf, templateIdBuf);
    }

    private void encodeArgsInfo() {
        // Always encode, even for 0 args: the row path always emits the argsInfo column, where
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
            argsInfoLen = Arg.encodeInfoBytes(argOffsets, argCount, argsInfoOut, argsInfoRawScratch, argsInfoBuf);
        } catch (IOException e) {
            // ByteArrayDataOutput.writeVInt never actually throws — this branch is unreachable.
            throw new UncheckedIOException(e);
        }
    }
}
