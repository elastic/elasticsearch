/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 *
 * This Java port DoubleArray Trie Structure, precompiled charmap parsing and sentence piece normalizer was derived from
 * Huggingface's spm-precompiled.
 * project at https://github.com/huggingface/spm_precompiled
 */

package org.elasticsearch.xpack.ml.inference.nlp.tokenizers;

import com.ibm.icu.text.BreakIterator;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.UnicodeUtil;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Locale;
import java.util.Optional;
import java.util.OptionalInt;

/**
 * This is custom normalizer logic purpose built to replicate the logic in DoubleArray Trie System (darts)
 * object and the sentence piece normalizer.
 *
 * Links with further explanation of various parts of the algorithm:
 *  - <a href="https://github.com/huggingface/spm_precompiled/blob/81b911a362adef3ad3cc6d5835d2980690dbb871/src/lib.rs">
 *      huggingface lib
 *      </a>
 *  - <a href="https://github.com/google/sentencepiece/blob/bc53923a9147dc8ffa54034c8ed774de78cc4d39/third_party/darts_clone/darts.h#L469">
 *      DARTS
 *      </a>
 *  - <a href="https://github.com/google/sentencepiece/blob/91809e5c70ed0e6364267a0f0fed66c144482ce4/src/normalizer.cc">SP normalizer</a>
 */
public class PrecompiledCharMapNormalizer {

    static PrecompiledCharMapNormalizer fromBase64Str(String s) {
        int offset = 0;
        byte[] bytes = Base64.getDecoder().decode(s);
        int trieSize = ByteBuffer.wrap(bytes, offset, 4).order(java.nio.ByteOrder.LITTLE_ENDIAN).getInt();
        offset += 4;
        int size = trieSize / 4;
        int[] offsets = new int[size];
        for (int i = 0; i < size; i++) {
            offsets[i] = ByteBuffer.wrap(bytes, offset, 4).order(java.nio.ByteOrder.LITTLE_ENDIAN).getInt();
            offset += 4;
        }
        String utf8Str = new String(bytes, offset, bytes.length - offset, StandardCharsets.UTF_8);
        return new PrecompiledCharMapNormalizer(offsets, utf8Str);
    }

    // The offsets for each normalization piece. Used in DARTS algorithm to iterate and find appropriate section
    // in normalizedStrUtf8Bytes
    private final int[] offsets;
    // The entire normalized bytes representations delimited by NULL
    private final byte[] normalizedStrUtf8Bytes;
    // Continually reused to copy a single char into utf8 bytes
    private final byte[] reusableCharByteBuffer = new byte[4];

    public PrecompiledCharMapNormalizer(int[] offsets, String normalizedStr) {
        this.offsets = offsets;
        this.normalizedStrUtf8Bytes = normalizedStr.getBytes(StandardCharsets.UTF_8);
    }

    boolean hasLeaf(int v) {
        return ((v >>> 8) & 1) == 1;
    }

    int label(int v) {
        return (v & ((1 << 31) | 0xFF));
    }

    int value(int v) {
        return (v & ((1 << 31) - 1));
    }

    int offset(int v) {
        return (v >>> 10) << ((v & (1 << 9)) >>> 6);
    }

    OptionalInt commonPrefix(byte[] inputBytes) {
        return commonPrefix(inputBytes, 0, inputBytes.length);
    }

    /**
     * This finds a common prefix position within the normalization byte string.
     *
     * Since the normalization string is NULL delimited, start at the returned index and continue until you hit the NULL byte. That is
     * then the normalized string.
     *
     * The prefix search is done according to DoubleArray Trie System (DARTS).
     *
     * See:
     * <a href="https://github.com/google/sentencepiece/blob/bc53923a9147dc8ffa54034c8ed774de78cc4d39/third_party/darts_clone/darts.h#L469">
     *     DARTS
     *     </a>
     * @param inputBytes utf8 bytes to normalize
     * @param offset offset position to start given the input
     * @param len the length of bytes to consider
     * @return The starting position in the normalization string of the normalized bytes, if found.
     */
    OptionalInt commonPrefix(byte[] inputBytes, int offset, int len) {
        int pos = 0;
        OptionalInt vs = OptionalInt.empty();
        int v = offsets[pos];
        pos ^= offset(v);
        for (int i = offset; i < offset + len; i++) {
            // bytes can be negative in java, handle it and require unsigned
            int k = inputBytes[i];
            if (k < 0) {
                k += 256;
            }
            if (k == 0) {
                break;
            }
            pos ^= k;
            v = offsets[pos];
            if (label(v) != k) {
                return vs;
            }
            pos ^= offset(v);
            if (hasLeaf(v)) {
                vs = OptionalInt.of(value(offsets[pos]));
                return vs;
            }
        }
        return vs;
    }

    Optional<BytesRef> normalizePart(byte[] strBytes, int offset, int len) {
        OptionalInt index = commonPrefix(strBytes, offset, len);
        if (index.isEmpty()) {
            return Optional.empty();
        }
        int firstIndex = index.getAsInt();
        int secondIndex = firstIndex;
        // Parsed normalized string has normalization sections partitioned by \0 (NULL) byte
        while (secondIndex < normalizedStrUtf8Bytes.length && normalizedStrUtf8Bytes[secondIndex] != 0) {
            secondIndex++;
        }
        if (secondIndex == firstIndex) {
            return Optional.of(new BytesRef(BytesRef.EMPTY_BYTES));
        }
        return Optional.of(new BytesRef(normalizedStrUtf8Bytes, firstIndex, secondIndex - firstIndex));
    }

    String normalize(String str) {
        // We need to iterate actual Unicode graphemes (this includes surrogate pairs, etc.)
        // I would much rather translate the entire input string text into utf-8 bytes, and then iterate to the appropriate
        // break points from there. But, this seemed the easiest way for now
        //
        // Keep in mind, these break points aren't necessarily surrogate pairs, but also codepoints that contain a combining mark
        byte[] strBytes = str.getBytes(StandardCharsets.UTF_8);
        char[] strChars = str.toCharArray();
        int[] strCp = str.codePoints().toArray();
        BreakIterator b = BreakIterator.getCharacterInstance(Locale.ROOT);
        b.setText(str);
        // We iterate the whole string, so b.first() is always `0`
        int startIter = b.first();
        int codePointPos = 0;
        BytesRefBuilder strBuilder = new BytesRefBuilder();
        strBuilder.grow(strBytes.length);
        int bytePos = 0;
        for (int end = b.next(); end != BreakIterator.DONE; startIter = end, end = b.next()) {
            int byteLen = 0;
            int numCp = str.codePointCount(startIter, end);
            for (int i = codePointPos; i < numCp + codePointPos; i++) {
                byteLen += numUtf8Bytes(strCp[i]);
            }
            codePointPos += numCp;
            // The trie only go up to a depth of 5 bytes.
            // So even looking at it for graphemes (with combining, surrogate, etc.) that are 6+ bytes in length is useless.
            if (byteLen < 6) {
                Optional<BytesRef> subStr = normalizePart(strBytes, bytePos, byteLen);
                if (subStr.isPresent()) {
                    strBuilder.append(subStr.get());
                    bytePos += byteLen;
                    continue;
                }
            }
            int charByteIndex = 0;
            for (int i = startIter; i < end; i++) {
                int utf8CharBytes = numUtf8Bytes(strChars[i]);
                Optional<BytesRef> subStr = normalizePart(strBytes, charByteIndex + bytePos, utf8CharBytes);
                if (subStr.isPresent()) {
                    strBuilder.append(subStr.get());
                } else {
                    int numBytes = UnicodeUtil.UTF16toUTF8(strChars, i, 1, reusableCharByteBuffer);
                    strBuilder.append(reusableCharByteBuffer, 0, numBytes);
                }
                charByteIndex += utf8CharBytes;
            }
            bytePos += byteLen;
        }
        return strBuilder.get().utf8ToString();
    }

    private static int numUtf8Bytes(int c) {
        if (c < 128) {
            return 1;
        }
        if (c < 2048) {
            return 2;
        }
        if (c < 65536) {
            return 3;
        }
        return 4;
    }

}
