/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.security;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.EqualsHashCodeTestUtils;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;

public class KibanaEnrollmentResponseTests extends ESTestCase {

    public void testFromXContent() throws IOException {
        final String tokenName = randomAlphaOfLengthBetween(8 ,14);
        final String tokenValue = randomAlphaOfLengthBetween(58, 70);
        final String httpCa = randomAlphaOfLength(50);

        final XContentType xContentType = randomFrom(XContentType.values());
        final XContentBuilder builder = XContentFactory.contentBuilder(xContentType);
        builder.startObject()
            .startObject("token")
            .field("name", tokenName)
            .field("value", tokenValue)
            .endObject()
            .field("http_ca", httpCa)
            .endObject();
        BytesReference xContent = BytesReference.bytes(builder);

        final KibanaEnrollmentResponse response = KibanaEnrollmentResponse.fromXContent(createParser(xContentType.xContent(), xContent));
        assertThat(response.getTokenName(), equalTo(tokenName));
        assertThat(response.getTokenValue(), equalTo(tokenValue));
        assertThat(response.getHttpCa(), equalTo(httpCa));
    }

    public void testEqualsHashCode() {
        final String tokenName = randomAlphaOfLengthBetween(8 ,14);
        final SecureString tokenValue = new SecureString(randomAlphaOfLengthBetween(58, 70).toCharArray());
        final String httpCa = randomAlphaOfLength(50);
        KibanaEnrollmentResponse kibanaEnrollmentResponse = new KibanaEnrollmentResponse(tokenName, tokenValue, httpCa);

        EqualsHashCodeTestUtils.checkEqualsAndHashCode(kibanaEnrollmentResponse,
            (original) -> new KibanaEnrollmentResponse(original.getTokenName(), original.getTokenValue(), original.getHttpCa()));

        EqualsHashCodeTestUtils.checkEqualsAndHashCode(kibanaEnrollmentResponse,
            (original) -> new KibanaEnrollmentResponse(original.getTokenName(), original.getTokenValue(), original.getHttpCa()),
            KibanaEnrollmentResponseTests::mutateTestItem);
    }

    private static KibanaEnrollmentResponse mutateTestItem(KibanaEnrollmentResponse original) {
        switch (randomIntBetween(0, 3)) {
            case 0:
                return new KibanaEnrollmentResponse(
                    randomAlphaOfLengthBetween(14, 20),
                    new SecureString(randomAlphaOfLengthBetween(71, 90).toCharArray()),
                    randomAlphaOfLength(52)
                );
            case 1:
                return new KibanaEnrollmentResponse(
                    original.getTokenName(),
                    new SecureString(randomAlphaOfLengthBetween(71, 90).toCharArray()),
                    randomAlphaOfLength(52)
                );
            case 2:
                return new KibanaEnrollmentResponse(
                    randomAlphaOfLengthBetween(14, 20),
                    original.getTokenValue(),
                    randomAlphaOfLength(52)
                );
            case 3:
                return new KibanaEnrollmentResponse(
                    randomAlphaOfLengthBetween(14, 20),
                    new SecureString(randomAlphaOfLengthBetween(71, 90).toCharArray()),
                    original.getHttpCa()
                );
        }
        // we never reach here
        return null;
    }
}
