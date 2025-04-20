/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.xcontent.json;

import org.elasticsearch.common.xcontent.BaseXContentTestCase;
import org.elasticsearch.xcontent.XContentGenerator;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.ByteArrayOutputStream;

public class JsonXContentTests extends BaseXContentTestCase {

    @Override
    public XContentType xcontentType() {
        return XContentType.JSON;
    }

    public void testBigInteger() throws Exception {
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        XContentGenerator generator = JsonXContent.jsonXContent.createGenerator(os);
        doTestBigInteger(generator, os);
    }

    public void testMalformedJsonFieldThrowsXContentException() throws Exception {
        String json = "{\"test\":\"/*/}";
        try (XContentParser parser = JsonXContent.jsonXContent.createParser(XContentParserConfiguration.EMPTY, json)) {
            parser.nextToken();
            parser.nextToken();
            parser.nextToken();
            assertThrows(XContentParseException.class, () -> parser.text());
        }
    }
}
