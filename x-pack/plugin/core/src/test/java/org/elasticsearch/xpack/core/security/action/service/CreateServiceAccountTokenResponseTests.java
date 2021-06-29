/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.service;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class CreateServiceAccountTokenResponseTests extends AbstractWireSerializingTestCase<CreateServiceAccountTokenResponse> {

    @Override
    protected Writeable.Reader<CreateServiceAccountTokenResponse> instanceReader() {
        return CreateServiceAccountTokenResponse::new;
    }

    @Override
    protected CreateServiceAccountTokenResponse createTestInstance() {
        return CreateServiceAccountTokenResponse.created(
            randomAlphaOfLengthBetween(3, 8), new SecureString(randomAlphaOfLength(20).toCharArray()));
    }

    @Override
    protected CreateServiceAccountTokenResponse mutateInstance(CreateServiceAccountTokenResponse instance) throws IOException {
        if (randomBoolean()) {
            return CreateServiceAccountTokenResponse.created(
                randomValueOtherThan(instance.getName(), () -> randomAlphaOfLengthBetween(3, 8)), instance.getValue());
        } else {
            return CreateServiceAccountTokenResponse.created(instance.getName(),
                randomValueOtherThan(instance.getValue(), () -> new SecureString(randomAlphaOfLength(22).toCharArray())));
        }
    }

    public void testToXContent() throws IOException {
        final CreateServiceAccountTokenResponse response = createTestInstance();
        XContentBuilder builder = XContentFactory.jsonBuilder();
        response.toXContent(builder, ToXContent.EMPTY_PARAMS);
        final Map<String, Object> responseMap = XContentHelper.convertToMap(
            BytesReference.bytes(builder),
            false, builder.contentType()).v2();

        assertThat(responseMap, equalTo(Map.of(
            "created", true,
            "token", Map.of("name", response.getName(), "value", response.getValue().toString())
            )));
    }
}
