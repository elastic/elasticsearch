/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with,
 * at your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.xcontent.provider.smile;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xcontent.XContentParserConfiguration;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;

public class SmileXContentImplTests extends ESTestCase {

    public void testStreamParserRejectsJson() {
        byte[] json = "{\"foo\":\"bar\"}".getBytes(StandardCharsets.UTF_8);
        XContentParseException e = expectThrows(
            XContentParseException.class,
            () -> SmileXContentImpl.smileXContent().createParser(XContentParserConfiguration.EMPTY, new ByteArrayInputStream(json))
        );
        assertEquals("Input does not start with Smile format header", e.getMessage());
    }

    public void testByteArrayParserRejectsJson() {
        byte[] json = "{\"foo\":\"bar\"}".getBytes(StandardCharsets.UTF_8);
        expectThrows(
            XContentParseException.class,
            () -> SmileXContentImpl.smileXContent().createParser(XContentParserConfiguration.EMPTY, json, 0, json.length)
        );
    }
}
