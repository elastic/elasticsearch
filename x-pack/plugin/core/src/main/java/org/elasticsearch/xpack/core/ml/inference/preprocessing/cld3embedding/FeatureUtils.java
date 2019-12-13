/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.ml.inference.preprocessing.cld3embedding;

import java.nio.charset.StandardCharsets;
import java.util.Locale;

/**
 * A collection of messy feature extractors
 */
public final class FeatureUtils {

    private FeatureUtils() {}

    /**
     * Text cleaning pre-processors
     */
    public static String truncateToNumValidBytes(String text, int maxLength) {
        byte[] bytes = text.getBytes(StandardCharsets.UTF_8);
        if (bytes.length < maxLength) {
            return text;
        }
        // Truncate the input text if it is too long and find the span containing
        // interchange-valid UTF8.
        int numValidBytes = validUTF8Length(text, maxLength);

        return new String(bytes, 0, numValidBytes, StandardCharsets.UTF_8);
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

    /**
     * simplified logic
     *
     * NOTE: This does not do any string compression by removing duplicate tokens
     */
    public static String cleanAndLowerText(String text) {
        // 1. Start with ' ', only if the string already does not start with a space
        String newText = text.startsWith(" ") ? "" : " ";

        // 2. Replace punctuation and whitespace with ' '
        // NOTE: we capture unicode letters AND marks as Nepalese and other languages
        newText += text.replaceAll("[^\\p{L}|\\p{M}|\\s]|\\|", " ");

        // 2.1. Replace spacing modifier characters
        newText = newText.replaceAll("\\p{InSpacing_Modifier_Letters}", " ");

        // 3. Add space at end
        newText += text.endsWith(" ") ? "" : " ";

        // 4. Remove multiple spaces (2 or more) with a single space
        newText = newText.replaceAll("\\s\\s+", " ");

        // 5. Replace Turkish İ with I (TODO - check this out better...)
        newText = newText.replaceAll("\\u0130", "I");

        return newText.toLowerCase(Locale.ROOT);
    }
}
