/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.security;

import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;

public class CreateEnrollmentTokenResponseTests  extends ESTestCase {

    public void testFromXContent() throws IOException {
        final SecureString enrollment_token = UUIDs.randomBase64UUIDSecureString();

        final XContentType xContentType = randomFrom(XContentType.values());
        final XContentBuilder builder = XContentFactory.contentBuilder(xContentType);
        builder.startObject()
            .field("enrollment_token", enrollment_token.toString());
        builder.endObject();
        BytesReference xContent = BytesReference.bytes(builder);

        final CreateEnrollmentTokenResponse response = CreateEnrollmentTokenResponse.fromXContent(createParser(xContentType.xContent(),
            xContent));
        assertThat(response.getEnrollmentToken(), equalTo(enrollment_token.toString()));
    }
}
