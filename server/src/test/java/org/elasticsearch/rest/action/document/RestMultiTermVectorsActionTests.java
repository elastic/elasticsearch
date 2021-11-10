/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest.action.document;

import org.elasticsearch.action.termvectors.MultiTermVectorsItemResponse;
import org.elasticsearch.action.termvectors.MultiTermVectorsResponse;
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
import java.util.HashMap;
import java.util.Map;

public class RestMultiTermVectorsActionTests extends RestActionTestCase {

    @Before
    public void setUpAction() {
        controller().registerHandler(new RestMultiTermVectorsAction());
    }

    public void testTypeInPath() {
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(Method.POST)
            .withPath("/some_index/some_type/_mtermvectors")
            .build();

        // We're not actually testing anything to do with the client, but need to set this so it doesn't fail the test for being unset.
        verifyingClient.setExecuteVerifier((action, r) -> new MultiTermVectorsResponse(new MultiTermVectorsItemResponse[0]));

        dispatchRequest(request);
        assertWarnings(RestMultiTermVectorsAction.TYPES_DEPRECATION_MESSAGE);
    }

    public void testTypeParameter() {
        Map<String, String> params = new HashMap<>();
        params.put("type", "some_type");

        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(Method.GET)
            .withPath("/some_index/_mtermvectors")
            .withParams(params)
            .build();

        // We're not actually testing anything to do with the client, but need to set this so it doesn't fail the test for being unset.
        verifyingClient.setExecuteVerifier((action, r) -> new MultiTermVectorsResponse(new MultiTermVectorsItemResponse[0]));

        dispatchRequest(request);
        assertWarnings(RestMultiTermVectorsAction.TYPES_DEPRECATION_MESSAGE);
    }

    public void testTypeInBody() throws IOException {
        XContentBuilder content = XContentFactory.jsonBuilder()
            .startObject()
            .startArray("docs")
            .startObject()
            .field("_type", "some_type")
            .field("_id", 1)
            .endObject()
            .endArray()
            .endObject();

        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(Method.GET)
            .withPath("/some_index/_mtermvectors")
            .withContent(BytesReference.bytes(content), XContentType.JSON)
            .build();

        // We're not actually testing anything to do with the client, but need to set this so it doesn't fail the test for being unset.
        verifyingClient.setExecuteVerifier((action, r) -> new MultiTermVectorsResponse(new MultiTermVectorsItemResponse[0]));

        dispatchRequest(request);
        assertWarnings(RestTermVectorsAction.TYPES_DEPRECATION_MESSAGE);
    }
}
