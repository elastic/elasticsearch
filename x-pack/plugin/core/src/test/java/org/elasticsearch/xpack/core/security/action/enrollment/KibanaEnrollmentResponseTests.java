/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.enrollment;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;

import java.io.IOException;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class KibanaEnrollmentResponseTests extends AbstractWireSerializingTestCase<KibanaEnrollmentResponse> {

    @Override
    protected Writeable.Reader<KibanaEnrollmentResponse> instanceReader() {
        return KibanaEnrollmentResponse::new;
    }

    @Override
    protected KibanaEnrollmentResponse createTestInstance() {
        return new KibanaEnrollmentResponse(
            randomAlphaOfLengthBetween(8, 12),
            new SecureString(randomAlphaOfLengthBetween(58, 70).toCharArray()),
            randomAlphaOfLength(50)
        );
    }

    @Override
    protected KibanaEnrollmentResponse mutateInstance(KibanaEnrollmentResponse instance) throws IOException {
        return switch (randomIntBetween(0, 3)) {
            case 0 -> new KibanaEnrollmentResponse(
                randomAlphaOfLengthBetween(14, 20),
                new SecureString(randomAlphaOfLengthBetween(71, 90).toCharArray()),
                randomAlphaOfLength(52)
            );
            case 1 -> new KibanaEnrollmentResponse(
                instance.getTokenName(),
                new SecureString(randomAlphaOfLengthBetween(71, 90).toCharArray()),
                randomAlphaOfLength(52)
            );
            case 2 -> new KibanaEnrollmentResponse(randomAlphaOfLengthBetween(14, 20), instance.getTokenValue(), randomAlphaOfLength(52));
            case 3 -> new KibanaEnrollmentResponse(
                randomAlphaOfLengthBetween(14, 20),
                new SecureString(randomAlphaOfLengthBetween(71, 90).toCharArray()),
                instance.getHttpCa()
            );
            default ->
                // we never reach here
                null;
        };
    }

    public void testToXContent() throws IOException {
        final KibanaEnrollmentResponse response = createTestInstance();
        XContentBuilder jsonBuilder = XContentFactory.jsonBuilder();
        response.toXContent(jsonBuilder, ToXContent.EMPTY_PARAMS);
        final Map<String, Object> responseMap = XContentHelper.convertToMap(
            BytesReference.bytes(jsonBuilder),
            false,
            jsonBuilder.contentType()
        ).v2();

        assertThat(
            responseMap,
            equalTo(
                Map.of(
                    "token",
                    Map.of("name", response.getTokenName(), "value", response.getTokenValue().toString()),
                    "http_ca",
                    response.getHttpCa()
                )
            )
        );
    }

}
