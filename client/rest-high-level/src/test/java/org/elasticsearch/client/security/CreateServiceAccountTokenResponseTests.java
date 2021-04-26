/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.security;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class CreateServiceAccountTokenResponseTests extends ESTestCase {

    public void testFromXContent() throws IOException {
        final XContentType xContentType = randomFrom(XContentType.values());
        final XContentBuilder builder = XContentFactory.contentBuilder(xContentType);
        final String tokenName = randomAlphaOfLengthBetween(3, 8);
        final String value = randomAlphaOfLength(22);
        builder.startObject()
            .field("created", "true")
            .field("token", Map.of("name", tokenName, "value", value))
            .endObject();
        BytesReference xContent = BytesReference.bytes(builder);

        final CreateServiceAccountTokenResponse response =
            CreateServiceAccountTokenResponse.fromXContent(createParser(xContentType.xContent(), xContent));

        assertThat(response.getName(), equalTo(tokenName));
        assertThat(response.getValue().toString(), equalTo(value));
    }
}
