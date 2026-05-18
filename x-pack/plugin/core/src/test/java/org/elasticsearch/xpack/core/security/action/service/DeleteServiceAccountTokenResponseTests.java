/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.service;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;

import java.io.IOException;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class DeleteServiceAccountTokenResponseTests extends AbstractWireSerializingTestCase<DeleteServiceAccountTokenResponse> {

    @Override
    protected Writeable.Reader<DeleteServiceAccountTokenResponse> instanceReader() {
        return DeleteServiceAccountTokenResponse::new;
    }

    @Override
    protected DeleteServiceAccountTokenResponse createTestInstance() {
        return new DeleteServiceAccountTokenResponse(randomBoolean());
    }

    @Override
    protected DeleteServiceAccountTokenResponse mutateInstance(DeleteServiceAccountTokenResponse instance) {
        return new DeleteServiceAccountTokenResponse(false == instance.found());
    }

    public void testToXContent() throws IOException {
        final DeleteServiceAccountTokenResponse response = createTestInstance();
        XContentBuilder builder = XContentFactory.jsonBuilder();
        response.toXContent(builder, ToXContent.EMPTY_PARAMS);
        final Map<String, Object> responseMap = XContentHelper.convertToMap(BytesReference.bytes(builder), false, builder.contentType())
            .v2();
        assertThat(responseMap, equalTo(Map.of("found", response.found())));
    }
}
