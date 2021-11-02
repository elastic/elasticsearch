/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.script.mustache;

import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.test.rest.RestActionTestCase;
import org.elasticsearch.xcontent.XContentType;
import org.junit.Before;

import java.nio.charset.StandardCharsets;

public class RestMultiSearchTemplateActionTests extends RestActionTestCase {

    @Before
    public void setUpAction() {
        controller().registerHandler(new RestMultiSearchTemplateAction(Settings.EMPTY));
    }

    public void testTypeInPath() {
        String content = "{ \"index\": \"some_index\" } \n" + "{\"source\": {\"query\" : {\"match_all\" :{}}}} \n";
        BytesArray bytesContent = new BytesArray(content.getBytes(StandardCharsets.UTF_8));

        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.GET)
            .withPath("/some_index/some_type/_msearch/template")
            .withContent(bytesContent, XContentType.JSON)
            .build();
        // We're not actually testing anything to do with the client, but need to set this so it doesn't fail the test for being unset.
        verifyingClient.setExecuteVerifier((arg1, arg2) -> new MultiSearchResponse(new MultiSearchResponse.Item[0], 10));

        dispatchRequest(request);
        assertWarnings(RestMultiSearchTemplateAction.TYPES_DEPRECATION_MESSAGE);
    }

    public void testTypeInBody() {
        String content = "{ \"index\": \"some_index\", \"type\": \"some_type\" } \n" + "{\"source\": {\"query\" : {\"match_all\" :{}}}} \n";
        BytesArray bytesContent = new BytesArray(content.getBytes(StandardCharsets.UTF_8));

        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withPath("/some_index/_msearch/template")
            .withContent(bytesContent, XContentType.JSON)
            .build();
        // We're not actually testing anything to do with the client, but need to set this so it doesn't fail the test for being unset.
        verifyingClient.setExecuteVerifier((arg1, arg2) -> new MultiSearchResponse(new MultiSearchResponse.Item[0], 10));

        dispatchRequest(request);
        assertWarnings(RestMultiSearchTemplateAction.TYPES_DEPRECATION_MESSAGE);
    }
}
