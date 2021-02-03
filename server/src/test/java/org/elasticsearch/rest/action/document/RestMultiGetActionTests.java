/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest.action.document;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestRequest.Method;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.test.rest.RestActionTestCase;
import org.junit.Before;

public class RestMultiGetActionTests extends RestActionTestCase {

    @Before
    public void setUpAction() {
        controller().registerHandler(new RestMultiGetAction(Settings.EMPTY));
    }

    public void testTypeInPath() {
        // We're not actually testing anything to do with the client, but need to set this so it doesn't fail the test for being unset.
        verifyingClient.setExecuteVerifier((arg1, arg2) -> null);

        RestRequest deprecatedRequest = new FakeRestRequest.Builder(xContentRegistry())
            .withMethod(Method.GET)
            .withPath("some_index/some_type/_mget")
            .build();
        dispatchRequest(deprecatedRequest);
        assertWarnings(RestMultiGetAction.TYPES_DEPRECATION_MESSAGE);

        RestRequest validRequest = new FakeRestRequest.Builder(xContentRegistry())
            .withMethod(Method.GET)
            .withPath("some_index/_mget")
            .build();
        dispatchRequest(validRequest);
    }

    public void testTypeInBody() throws Exception {
        XContentBuilder content = XContentFactory.jsonBuilder().startObject()
                .startArray("docs")
                    .startObject()
                        .field("_index", "some_index")
                        .field("_type", "_doc")
                        .field("_id", "2")
                    .endObject()
                    .startObject()
                        .field("_index", "test")
                        .field("_id", "2")
                    .endObject()
                .endArray()
            .endObject();

        // We're not actually testing anything to do with the client, but need to set this so it doesn't fail the test for being unset.
        verifyingClient.setExecuteVerifier((arg1, arg2) -> null);

        RestRequest request = new FakeRestRequest.Builder(xContentRegistry())
            .withPath("_mget")
            .withContent(BytesReference.bytes(content), XContentType.JSON)
            .build();
        dispatchRequest(request);
        assertWarnings(RestMultiGetAction.TYPES_DEPRECATION_MESSAGE);
    }
}
