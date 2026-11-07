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

public class PutUserManagedServiceAccountResponseTests extends AbstractWireSerializingTestCase<PutUserManagedServiceAccountResponse> {

    @Override
    protected Writeable.Reader<PutUserManagedServiceAccountResponse> instanceReader() {
        return PutUserManagedServiceAccountResponse::new;
    }

    @Override
    protected PutUserManagedServiceAccountResponse createTestInstance() {
        return new PutUserManagedServiceAccountResponse(randomBoolean());
    }

    @Override
    protected PutUserManagedServiceAccountResponse mutateInstance(PutUserManagedServiceAccountResponse instance) {
        return new PutUserManagedServiceAccountResponse(instance.created() == false);
    }

    public void testToXContentReportsWhetherTheAccountWasCreated() throws IOException {
        for (boolean created : new boolean[] { true, false }) {
            assertThat(toMap(new PutUserManagedServiceAccountResponse(created)), equalTo(Map.of("created", created)));
        }
    }

    private static Map<String, Object> toMap(PutUserManagedServiceAccountResponse response) throws IOException {
        final XContentBuilder builder = XContentFactory.jsonBuilder();
        response.toXContent(builder, ToXContent.EMPTY_PARAMS);
        return XContentHelper.convertToMap(BytesReference.bytes(builder), false, builder.contentType()).v2();
    }
}
