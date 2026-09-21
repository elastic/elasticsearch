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

public class DeleteUserManagedServiceAccountResponseTests extends AbstractWireSerializingTestCase<DeleteUserManagedServiceAccountResponse> {

    @Override
    protected Writeable.Reader<DeleteUserManagedServiceAccountResponse> instanceReader() {
        return DeleteUserManagedServiceAccountResponse::new;
    }

    @Override
    protected DeleteUserManagedServiceAccountResponse createTestInstance() {
        return new DeleteUserManagedServiceAccountResponse(randomBoolean());
    }

    @Override
    protected DeleteUserManagedServiceAccountResponse mutateInstance(DeleteUserManagedServiceAccountResponse instance) {
        return new DeleteUserManagedServiceAccountResponse(instance.found() == false);
    }

    public void testToXContentReportsWhetherAnAccountWasRemoved() throws IOException {
        for (boolean found : new boolean[] { true, false }) {
            assertThat(toMap(new DeleteUserManagedServiceAccountResponse(found)), equalTo(Map.of("found", found)));
        }
    }

    private static Map<String, Object> toMap(DeleteUserManagedServiceAccountResponse response) throws IOException {
        final XContentBuilder builder = XContentFactory.jsonBuilder();
        response.toXContent(builder, ToXContent.EMPTY_PARAMS);
        return XContentHelper.convertToMap(BytesReference.bytes(builder), false, builder.contentType()).v2();
    }
}
