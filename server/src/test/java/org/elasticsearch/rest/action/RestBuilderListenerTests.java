/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.rest.action;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionResponse.Empty;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.FakeRestChannel;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.xcontent.XContentBuilder;

import java.util.concurrent.atomic.AtomicReference;

public class RestBuilderListenerTests extends ESTestCase {

    // bypass the check that XContent responses are never empty - we're ignoring the builder and sending a text/plain response anyway
    private static final BytesArray NONEMPTY_BODY = new BytesArray(new byte[] { '\n' });

    public void testXContentBuilderClosedInBuildResponse() throws Exception {
        AtomicReference<XContentBuilder> builderAtomicReference = new AtomicReference<>();
        RestBuilderListener<ActionResponse.Empty> builderListener = new RestBuilderListener<>(
            new FakeRestChannel(new FakeRestRequest(), randomBoolean(), 1)
        ) {
            @Override
            public RestResponse buildResponse(Empty empty, XContentBuilder builder) {
                builderAtomicReference.set(builder);
                builder.close();
                return new RestResponse(RestStatus.OK, RestResponse.TEXT_CONTENT_TYPE, NONEMPTY_BODY);
            }
        };

        builderListener.buildResponse(Empty.INSTANCE);
        assertNotNull(builderAtomicReference.get());
        assertTrue(builderAtomicReference.get().generator().isClosed());
    }

    public void testXContentBuilderNotClosedInBuildResponseAssertionsDisabled() throws Exception {
        AtomicReference<XContentBuilder> builderAtomicReference = new AtomicReference<>();
        RestBuilderListener<ActionResponse.Empty> builderListener = new RestBuilderListener<>(
            new FakeRestChannel(new FakeRestRequest(), randomBoolean(), 1)
        ) {
            @Override
            public RestResponse buildResponse(Empty empty, XContentBuilder builder) {
                builderAtomicReference.set(builder);
                return new RestResponse(RestStatus.OK, RestResponse.TEXT_CONTENT_TYPE, NONEMPTY_BODY);
            }

            @Override
            boolean assertBuilderClosed(XContentBuilder xContentBuilder) {
                // don't check the actual builder being closed so we can test auto close
                return true;
            }
        };

        builderListener.buildResponse(Empty.INSTANCE);
        assertNotNull(builderAtomicReference.get());
        assertTrue(builderAtomicReference.get().generator().isClosed());
    }

    public void testXContentBuilderNotClosedInBuildResponseAssertionsEnabled() {
        assumeTrue("tests are not being run with assertions", RestBuilderListener.class.desiredAssertionStatus());

        RestBuilderListener<ActionResponse.Empty> builderListener = new RestBuilderListener<>(
            new FakeRestChannel(new FakeRestRequest(), randomBoolean(), 1)
        ) {
            @Override
            public RestResponse buildResponse(Empty empty, XContentBuilder builder) {
                return new RestResponse(RestStatus.OK, RestResponse.TEXT_CONTENT_TYPE, NONEMPTY_BODY);
            }
        };

        AssertionError error = expectThrows(AssertionError.class, () -> builderListener.buildResponse(Empty.INSTANCE));
        assertEquals("callers should ensure the XContentBuilder is closed themselves", error.getMessage());
    }
}
