/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.inference.preprocessing.customwordembedding;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.Locale;
import java.util.regex.Pattern;

/**
 * A collection of messy feature extractors
 */
public final class FeatureUtils {

    private static final Pattern NOT_UNICODE_OR_IS_SPECIAL = Pattern.compile("[^\\p{L}|\\p{M}|\\s]|\\||\\p{InSpacing_Modifier_Letters}");
    private static final Pattern ONE_OR_MORE_WHITESPACE = Pattern.compile("\\p{IsWhite_Space}+");
    private static final Pattern TURKISH_I = Pattern.compile("\\u0130");

    private FeatureUtils() {}

    /**
     * Truncates a string to the number of characters that fit in X bytes avoiding multi byte characters being cut in
     * half at the cut off point. Also handles surrogate pairs where 2 characters in the string is actually one literal
     * character.
     *
     * Based on: https://stackoverflow.com/a/35148974/1818849
     *
     */
    public static String truncateToNumValidBytes(String text, int maxLength) {
        if (text == null) {
            return null;
        }
        byte[] sba = text.getBytes(StandardCharsets.UTF_8);
        if (sba.length <= maxLength) {
            return text;
        }
        CharsetDecoder decoder = StandardCharsets.UTF_8.newDecoder();
        // Ensure truncation by having byte buffer = maxBytes
        ByteBuffer bb = ByteBuffer.wrap(sba, 0, maxLength);
        CharBuffer cb = CharBuffer.allocate(maxLength);
        // Ignore an incomplete character
        decoder.onMalformedInput(CodingErrorAction.IGNORE);
        decoder.decode(bb, cb, true);
        decoder.flush(cb);
        return new String(cb.array(), 0, cb.position());
    }

    /**
     * Cleanup text and lower-case it
     * NOTE: This does not do any string compression by removing duplicate tokens
     */
    public static String cleanAndLowerText(String text) {
        // 1. Start with ' ', only if the string already does not start with a space
        String newText = text.startsWith(" ") ? "" : " ";

        // 2. Replace punctuation and whitespace with ' '
        // 2.1. Replace spacing modifier characters
        // NOTE: we capture unicode letters AND marks as Nepalese and other languages
        newText += NOT_UNICODE_OR_IS_SPECIAL.matcher(text).replaceAll(" ");

        // 3. Add space at end
        newText += text.endsWith(" ") ? "" : " ";

        // 4. Remove multiple spaces (2 or more) with a single space
        newText = ONE_OR_MORE_WHITESPACE.matcher(newText).replaceAll(" ");

        // 5. Replace Turkish İ with I (TODO - check this out better...)
        newText = TURKISH_I.matcher(newText).replaceAll("I");

        return newText.toLowerCase(Locale.ROOT);
    }
}
