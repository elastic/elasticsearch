/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.xcontent.provider.json;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;

public class JsonXContentImplTests extends ESTestCase {

    /**
     * Verifies the byte-level encoding of supplementary Unicode characters (above U+FFFF)
     * in JSON string values. With {@link com.fasterxml.jackson.core.JsonGenerator.Feature#COMBINE_UNICODE_SURROGATES_IN_UTF8}
     * enabled, surrogate pairs are combined and written as a single 4-byte UTF-8 sequence rather
     * than as JSON Unicode escape sequences.
     */
    public void testSupplementaryCharacterByteEncoding() throws IOException {
        String emoji = "🎵"; // U+1F3B5 MUSICAL NOTE

        XContentBuilder builder = JsonXContentImpl.getContentBuilder();
        builder.startObject();
        builder.field("k", emoji);
        builder.endObject();
        byte[] output = BytesReference.toBytes(BytesReference.bytes(builder));
        String latin1Output = new String(output, StandardCharsets.ISO_8859_1);

        // U+1F3B5 -> F0 9F 8E B5 (proper 4-byte UTF-8)
        String utf8 = new String(new byte[] { (byte) 0xF0, (byte) 0x9F, (byte) 0x8E, (byte) 0xB5 }, StandardCharsets.ISO_8859_1);
        assertThat(latin1Output, containsString(utf8));
        assertThat(latin1Output, not(containsString("\\uD83C\\uDFB5")));
    }
}
