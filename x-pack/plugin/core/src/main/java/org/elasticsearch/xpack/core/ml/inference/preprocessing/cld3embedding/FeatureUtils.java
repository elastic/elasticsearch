/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.ml.inference.preprocessing.cld3embedding;

import java.io.UnsupportedEncodingException;
import java.util.Locale;

/**
 * A collection of messy feature extractors that are specific to google cld3 models
 */
public final class FeatureUtils {

    private FeatureUtils() {}

    // Bespoke hashing function for UTF8 strings
    // required so features align with cld3 models
    public static int Hash32WithDefaultSeed(String input) throws UnsupportedEncodingException {
        byte[] bytes = input.getBytes("UTF8");

        return Hash32(bytes, bytes.length, 0xBEEF);
    }

    private static int Hash32(byte[] data, int n, int seed) {
        // 'm' and 'r' are mixing constants generated offline.
        // They're not really 'magic', they just happen to work well.
        int m = 0x5bd1e995;
        int r = 24;

        // Initialize the hash to a 'random' value
        int h = (seed ^ n);

        // Mix 4 bytes at a time into the hash
        int i = 0;
        while (n >= 4) {
            int k = DecodeFixed32(get(data, i));
            k *= m;
            k ^= k >>> r; // use unsigned shift
            k *= m;
            h *= m;
            h ^= k;
            i += 4;
            n -= 4;
        }

        // Handle the last few bytes of the input array
        if (n == 3) {
            h ^= ByteAs32(data[i + 2]) << 16;
            h ^= ByteAs32(data[i + 1]) << 8;
            h ^= ByteAs32(data[i]);
            h *= m;
        } else if (n == 2) {
            h ^= ByteAs32(data[i + 1]) << 8;
            h ^= ByteAs32(data[i]);
            h *= m;
        } else if (n == 1) {
            h ^= ByteAs32(data[i]);
            h *= m;
        }

        // Do a few final mixes of the hash to ensure the last few
        // bytes are well-incorporated.
        h ^= h >>> 13; // use unsigned shift
        h *= m;
        h ^= h >>> 15; // use unsigned shift
        return h;
    }

    private static int DecodeFixed32(byte[] ptr) {
        return ByteAs32(ptr[0]) |
            ByteAs32(ptr[1]) << 8 |
            ByteAs32(ptr[2]) << 16 |
            ByteAs32(ptr[3]) << 24;
    }

    private static int ByteAs32(byte c) {
        return (c & 0xFF);
    }

    // Get subarray from array that starts at offset
    private static byte[] get(byte[] array, int offset) {
        return get(array, offset, array.length - offset);
    }

    // Get the subarray of length from array that starts at offset
    private static byte[] get(byte[] array, int offset, int length) {
        // TODO - check efficiency
        byte[] result = new byte[length];
        System.arraycopy(array, offset, result, 0, length);
        return result;
    }

    /**
     * Text cleaning pre-processors
     */
    public static String truncateToNumValidBytes(String text, int maxLength) throws UnsupportedEncodingException {
        byte[] bytes = text.getBytes("UTF-8");
        if (bytes.length < maxLength) {
            return text;
        }
        // Truncate the input text if it is too long and find the span containing
        // interchange-valid UTF8.
        int numValidBytes = validUTF8Length(text, maxLength);

        return new String(bytes, 0, numValidBytes, "UTF-8");
    }

    // Return truncated byte length of UTF8 string (truncating without splitting UTF8)
    // TODO - we can probably do this with Java codepoints
    public static int validUTF8Length(String input, int maxLength) {
        int resultLen = 0;
        for (int i = 0; i < input.length(); i++) {
            char c = input.charAt(i);
            int charLen = 0;
            if (c <= 0x7F) {
                charLen = 1;
            } else if (c <= 0x7FF) {
                charLen = 2;
            } else if (c <= 0xD7FF) {
                charLen = 3;
            } else if (c <= 0xDBFF) {
                charLen = 4;
            } else if (c <= 0xDFFF) {
                charLen = 0;
            } else if (c <= 0xFFFF) {
                charLen = 3;
            }
            if (resultLen + charLen > maxLength) {
                break;
            }
            resultLen += charLen;
        }
        return resultLen;
    }

    // Clean up text and lowercase it
    public static String cleanAndLowerText(String text) {
        // cld3 has complex optimised cleaning code - but generally boils down to:
        // - remove punctuation
        // - remove whitespace (except for spaces)
        // - replace multiple spaces with one space
        // - add space at ^ and $
        // ideally we can do this in a optimised pass(es) over string
        // but for simplicity we initially do this using standard java functions

        // TODO - PLEASE NOTE, this is probably the main difference between
        // TODO - cld3 and this code. This should partition text into multiple
        // TODO - scripts etc. and be more inline with cld3
        // TODO - also this is inefficient

        // 1. Start with ' '
        String newText = " ";

        // 2. Replace punctuation and whitespace with ' '
        // NOTE: we capture unicode letters AND marks as Nepalese and other languages
        // have marks that cld3 uses.
        newText += text.replaceAll("\\p{IsWhite_Space}|[^\\p{L}|\\p{M}]|\\|", " ");

        // 2.1. Replace spacing modifier characters
        newText = newText.replaceAll("\\p{InSpacing_Modifier_Letters}", " ");

        // 3. Add space at end
        newText += " ";

        // 4. Remove multiple spaces with a single space
        newText = newText.replaceAll("(\\p{IsWhite_Space})+", " ");

        // 5. Replace Turkish İ with I (TODO - check this out better...)
        newText = newText.replaceAll("\\u0130", "I");

        return newText.toLowerCase(Locale.ROOT);
    }
}
