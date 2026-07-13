/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.xcontent.smile;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.xcontent.BaseXContentTestCase;
import org.elasticsearch.xcontent.XContentGenerator;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParser.Token;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.smile.SmileXContent;

import java.io.ByteArrayOutputStream;

public class SmileXContentTests extends BaseXContentTestCase {

    @Override
    public XContentType xcontentType() {
        return XContentType.SMILE;
    }

    public void testBigInteger() throws Exception {
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        XContentGenerator generator = SmileXContent.smileXContent.createGenerator(os);
        doTestBigInteger(generator, os);
    }

    public void testAllowsDuplicates() throws Exception {
        try (XContentParser xParser = createParser(builder().startObject().endObject())) {
            expectThrows(UnsupportedOperationException.class, () -> xParser.allowDuplicateKeys(true));
        }
    }

    /**
     * Binary SMILE values (VALUE_EMBEDDED_OBJECT tokens) have no text representation.
     * {@code optimizedTextOrNull()} must return {@code null} for them, not {@code Text}
     * object with null internals, to avoid a NullPointerException when callers access the
     * string content (e.g. via {@code stringLength()}).
     */
    public void testOptimizedTextOrNullReturnNullForBinaryValue() throws Exception {
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        try (XContentGenerator generator = SmileXContent.smileXContent.createGenerator(os)) {
            generator.writeStartObject();
            generator.writeBinaryField("field", new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16 });
            generator.writeEndObject();
        }
        try (XContentParser parser = createParser(SmileXContent.smileXContent, new BytesArray(os.toByteArray()))) {
            assertSame(Token.START_OBJECT, parser.nextToken());
            assertSame(Token.FIELD_NAME, parser.nextToken());
            assertSame(Token.VALUE_EMBEDDED_OBJECT, parser.nextToken());
            // Binary SMILE values have no text representation; optimizedTextOrNull() must return null,
            // not new Text(null), which would NPE when stringLength() is called on it.
            assertNull(parser.optimizedTextOrNull());
        }
    }
}
