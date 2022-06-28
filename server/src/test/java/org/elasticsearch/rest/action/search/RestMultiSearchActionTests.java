/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest.action.search;

import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.test.rest.RestActionTestCase;
import org.elasticsearch.xcontent.XContentType;
import org.junit.Before;

import java.nio.charset.StandardCharsets;

public class RestMultiSearchActionTests extends RestActionTestCase {

    private static final MultiSearchResponse DUMMY_RESPONSE = new MultiSearchResponse(new MultiSearchResponse.Item[0], 0L);

    @Before
    public void setUpAction() {
        controller().registerHandler(new RestMultiSearchAction(Settings.EMPTY));
    }

    public void testTypeInPath() {
        String content = "{ \"index\": \"some_index\" } \n {} \n";
        BytesArray bytesContent = new BytesArray(content.getBytes(StandardCharsets.UTF_8));

        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.GET)
            .withPath("/some_index/some_type/_msearch")
            .withContent(bytesContent, XContentType.JSON)
            .build();

        // We're not actually testing anything to do with the client, but need to set this so it doesn't fail the test for being unset.
        verifyingClient.setExecuteLocallyVerifier((arg1, arg2) -> DUMMY_RESPONSE);

        dispatchRequest(request);
        assertWarnings(RestMultiSearchAction.TYPES_DEPRECATION_MESSAGE);
    }

    public void testTypeInBody() {
        String content = "{ \"index\": \"some_index\", \"type\": \"some_type\" } \n {} \n";
        BytesArray bytesContent = new BytesArray(content.getBytes(StandardCharsets.UTF_8));

        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.POST)
            .withPath("/some_index/_msearch")
            .withContent(bytesContent, XContentType.JSON)
            .build();

        // We're not actually testing anything to do with the client, but need to set this so it doesn't fail the test for being unset.
        verifyingClient.setExecuteLocallyVerifier((arg1, arg2) -> DUMMY_RESPONSE);

        dispatchRequest(request);
        assertWarnings(RestMultiSearchAction.TYPES_DEPRECATION_MESSAGE);
    }
}
