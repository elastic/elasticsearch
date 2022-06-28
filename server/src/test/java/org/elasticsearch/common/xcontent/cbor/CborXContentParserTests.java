/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.xcontent.cbor;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.cbor.CborXContent;

import java.io.IOException;

public class CborXContentParserTests extends ESTestCase {
    public void testEmptyValue() throws IOException {
        BytesReference ref = BytesReference.bytes(XContentFactory.cborBuilder().startObject().field("field", "").endObject());

        for (int i = 0; i < 2; i++) {
            // Running this part twice triggers the issue.
            // See https://github.com/elastic/elasticsearch/issues/8629
            try (XContentParser parser = createParser(CborXContent.cborXContent, ref)) {
                while (parser.nextToken() != null) {
                    parser.charBuffer();
                }
            }
        }
    }
}
