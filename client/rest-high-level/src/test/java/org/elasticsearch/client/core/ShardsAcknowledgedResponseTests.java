/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.core;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static org.elasticsearch.test.AbstractXContentTestCase.xContentTester;

public class ShardsAcknowledgedResponseTests extends ESTestCase {

    public void testFromXContent() throws IOException {
        xContentTester(this::createParser,
            this::createTestInstance,
            ShardsAcknowledgedResponseTests::toXContent,
            ShardsAcknowledgedResponse::fromXContent)
            .supportsUnknownFields(false)
            .test();
    }

    private ShardsAcknowledgedResponse createTestInstance() {
        return new ShardsAcknowledgedResponse(randomBoolean(), randomBoolean());
    }

    public static void toXContent(ShardsAcknowledgedResponse response, XContentBuilder builder) throws IOException {
        builder.startObject();
        {
            builder.field(response.getFieldName(), response.isAcknowledged());
            builder.field(ShardsAcknowledgedResponse.SHARDS_PARSE_FIELD_NAME, response.isShardsAcknowledged());
        }
        builder.endObject();
    }

}
