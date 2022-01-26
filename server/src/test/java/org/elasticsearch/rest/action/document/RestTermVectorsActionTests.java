/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest.action.document;

import org.elasticsearch.action.termvectors.TermVectorsResponse;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestRequest.Method;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.test.rest.RestActionTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.junit.Before;

import java.io.IOException;

public class RestTermVectorsActionTests extends RestActionTestCase {

    @Before
    public void setUpAction() {
        controller().registerHandler(new RestTermVectorsAction());
    }

    public void testTypeInPath() {
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(Method.POST)
            .withPath("/some_index/some_type/some_id/_termvectors")
            .build();

        // We're not actually testing anything to do with the client, but need to set this so it doesn't fail the test for being unset.
        verifyingClient.setExecuteVerifier((arg1, arg2) -> new TermVectorsResponse("index", "_doc", "id"));

        dispatchRequest(request);
        assertWarnings(RestTermVectorsAction.TYPES_DEPRECATION_MESSAGE);
    }

    public void testTypeInBody() throws IOException {
        XContentBuilder content = XContentFactory.jsonBuilder().startObject().field("_type", "some_type").field("_id", 1).endObject();

        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(Method.GET)
            .withPath("/some_index/_termvectors/some_id")
            .withContent(BytesReference.bytes(content), XContentType.JSON)
            .build();

        // We're not actually testing anything to do with the client, but need to set this so it doesn't fail the test for being unset.
        verifyingClient.setExecuteVerifier((arg1, arg2) -> new TermVectorsResponse("index", "_doc", "id"));

        dispatchRequest(request);
        assertWarnings(RestTermVectorsAction.TYPES_DEPRECATION_MESSAGE);
    }
}
