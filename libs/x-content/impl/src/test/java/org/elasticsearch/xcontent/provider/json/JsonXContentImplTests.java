/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.xcontent.provider.json;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Locale;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

public class JsonXContentImplTests extends ESTestCase {

    private static final String EMOJI = "🎵"; // U+1F3B5 MUSICAL NOTE

    // JSON Unicode escape sequences for EMOJI's surrogate pair, as produced by pre-9.6.0 Jackson
    private static final String EMOJI_SURROGATE_ESCAPES = asJsonUnicodeEscapes(EMOJI); // \uD83C\uDFB5

    // Proper 4-byte UTF-8 for EMOJI, viewed via ISO-8859-1 for byte-level substring matching
    private static final String EMOJI_UTF8_AS_LATIN1 = asLatin1(EMOJI.getBytes(StandardCharsets.UTF_8)); // F0 9F 8E B5

    /**
     * Verifies the byte-level encoding of supplementary Unicode characters (above U+FFFF)
     * in JSON string values. With {@link com.fasterxml.jackson.core.JsonGenerator.Feature#COMBINE_UNICODE_SURROGATES_IN_UTF8}
     * enabled, surrogate pairs are combined and written as a single 4-byte UTF-8 sequence rather
     * than as JSON Unicode escape sequences.
     */
    public void testSupplementaryCharacterByteEncoding() throws IOException {
        XContentBuilder builder = JsonXContentImpl.getContentBuilder();
        builder.startObject();
        builder.field("k", EMOJI);
        builder.endObject();
        String latin1Output = asLatin1(BytesReference.toBytes(BytesReference.bytes(builder)));

        // U+1F3B5 -> F0 9F 8E B5 (proper 4-byte UTF-8)
        assertThat(latin1Output, containsString(EMOJI_UTF8_AS_LATIN1));
        assertThat(latin1Output, not(containsString(EMOJI_SURROGATE_ESCAPES)));
    }

    /**
     * Verifies that old-format surrogate-escape bytes and new-format 4-byte UTF-8 bytes both
     * parse to the same String value. A document stored with the old encoding (as produced by
     * pre-9.6.0 Jackson) and one stored with the new encoding (as produced by
     * {@link com.fasterxml.jackson.core.JsonGenerator.Feature#COMBINE_UNICODE_SURROGATES_IN_UTF8})
     * are semantically identical. The generator always produces the new encoding, which is
     * what serialized responses (e.g. aggregation keys) look like on current nodes.
     */
    public void testSupplementaryCharacterRoundTrip() throws IOException {
        // Old format: surrogate pair written as JSON Unicode escape sequences (pre-9.6.0 behavior)
        byte[] oldFormat = ("{\"content\":\"" + EMOJI_SURROGATE_ESCAPES + "\"}").getBytes(StandardCharsets.UTF_8);

        // New format: written by XContentBuilder with COMBINE_UNICODE_SURROGATES_IN_UTF8 enabled
        XContentBuilder builder = JsonXContentImpl.getContentBuilder();
        builder.startObject().field("content", EMOJI).endObject();
        byte[] newFormat = BytesReference.toBytes(BytesReference.bytes(builder));

        // The two formats have different bytes
        assertThat(asLatin1(oldFormat), not(containsString(EMOJI_UTF8_AS_LATIN1)));
        assertThat(asLatin1(newFormat), containsString(EMOJI_UTF8_AS_LATIN1));

        // Both parse to the same emoji value
        Map<String, Object> fromOld = XContentHelper.convertToMap(new BytesArray(oldFormat), false, XContentType.JSON).v2();
        Map<String, Object> fromNew = XContentHelper.convertToMap(new BytesArray(newFormat), false, XContentType.JSON).v2();
        assertThat(fromOld.get("content"), equalTo(EMOJI));
        assertThat(fromNew.get("content"), equalTo(EMOJI));
    }

    private static String asJsonUnicodeEscapes(String s) {
        StringBuilder sb = new StringBuilder();
        for (char c : s.toCharArray()) {
            sb.append(String.format(Locale.ROOT, "\\u%04X", (int) c));
        }
        return sb.toString();
    }

    private static String asLatin1(byte[] bytes) {
        return new String(bytes, StandardCharsets.ISO_8859_1);
    }
}
