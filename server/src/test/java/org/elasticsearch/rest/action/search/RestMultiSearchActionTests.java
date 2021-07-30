/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest.action.search;

import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.core.RestApiVersion;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.test.rest.RestActionTestCase;
import org.junit.Before;
import org.mockito.Mockito;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RestMultiSearchActionTests extends RestActionTestCase {
    final List<String> contentTypeHeader = Collections.singletonList(compatibleMediaType(XContentType.VND_JSON, RestApiVersion.V_7));

    private RestMultiSearchAction action;

    @Before
    public void setUpAction() {
        action = new RestMultiSearchAction(Settings.EMPTY);
        controller().registerHandler(action);
        verifyingClient.setExecuteVerifier((actionType, request) -> Mockito.mock(MultiSearchResponse.class));
        verifyingClient.setExecuteLocallyVerifier((actionType, request) -> Mockito.mock(MultiSearchResponse.class));
    }

    public void testTypeInPath() {
        String content = "{ \"index\": \"some_index\" } \n {} \n";
        BytesArray bytesContent = new BytesArray(content.getBytes(StandardCharsets.UTF_8));

        RestRequest request = new FakeRestRequest.Builder(xContentRegistry())
            .withHeaders(Map.of("Content-Type", contentTypeHeader, "Accept", contentTypeHeader))
            .withMethod(RestRequest.Method.GET)
            .withPath("/some_index/some_type/_msearch")
            .withContent(bytesContent, null)
            .build();

        dispatchRequest(request);
        assertWarnings(RestMultiSearchAction.TYPES_DEPRECATION_MESSAGE);
    }

    public void testTypeInBody() {
        String content = "{ \"index\": \"some_index\", \"type\": \"some_type\" } \n {} \n";
        BytesArray bytesContent = new BytesArray(content.getBytes(StandardCharsets.UTF_8));

        RestRequest request = new FakeRestRequest.Builder(xContentRegistry())
            .withHeaders(Map.of("Content-Type", contentTypeHeader, "Accept", contentTypeHeader))
            .withMethod(RestRequest.Method.POST)
            .withPath("/some_index/_msearch")
            .withContent(bytesContent, null)
            .build();

        dispatchRequest(request);
        assertWarnings(RestMultiSearchAction.TYPES_DEPRECATION_MESSAGE);
    }

    private Map<String, List<String>> headersWith(String accept, List<String> value) {
        Map<String, List<String>> headers = new HashMap<>();
        headers.put(accept, value);
        return headers;
    }
}
