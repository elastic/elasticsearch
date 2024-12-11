/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.xcontent;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.provider.json.JsonXContentImpl;

import java.io.IOException;

public class SimdTests extends ESTestCase {
    public void test() throws IOException {
        var builder = XContentBuilder.builder(XContentType.JSON.xContent()).startObject().field("hello", "world").endObject();
        var bytes = BytesReference.toBytes(BytesReference.bytes(builder));

        var parser = JsonXContentImpl.jsonXContent().createParser(XContentParserConfiguration.EMPTY, bytes, 0, bytes.length);
    }
}
