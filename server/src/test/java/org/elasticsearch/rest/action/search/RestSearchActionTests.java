/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.rest.action.search;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.core.RestApiVersion;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.test.rest.FakeRestChannel;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.test.rest.RestActionTestCase;
import org.junit.Before;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RestSearchActionTests extends RestActionTestCase {
    final List<String> contentTypeHeader = Collections.singletonList(randomCompatibleMediaType(RestApiVersion.V_7));

    private RestSearchAction action;

    @Before
    public void setUpAction() {
        action = new RestSearchAction();
        controller().registerHandler(action);
        verifyingClient.setExecuteVerifier((actionType, request) -> Mockito.mock(SearchResponse.class));
        verifyingClient.setExecuteLocallyVerifier((actionType, request) -> Mockito.mock(SearchResponse.class));
    }

    public void testTypeInPath() {
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withHeaders(
            Map.of("Content-Type", contentTypeHeader, "Accept", contentTypeHeader)
        ).withMethod(RestRequest.Method.GET).withPath("/some_index/some_type/_search").build();

        dispatchRequest(request);
        assertCriticalWarnings(RestSearchAction.TYPES_DEPRECATION_MESSAGE);
    }

    public void testTypeParameter() {
        Map<String, String> params = new HashMap<>();
        params.put("type", "some_type");

        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withHeaders(
            Map.of("Content-Type", contentTypeHeader, "Accept", contentTypeHeader)
        ).withMethod(RestRequest.Method.GET).withPath("/some_index/_search").withParams(params).build();

        dispatchRequest(request);
        assertCriticalWarnings(RestSearchAction.TYPES_DEPRECATION_MESSAGE);
    }

    /**
     * The "enable_fields_emulation" flag on search requests is a no-op but should not raise an error
     */
    public void testEnableFieldsEmulationNoErrors() throws Exception {
        Map<String, String> params = new HashMap<>();
        params.put("enable_fields_emulation", "true");

        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withHeaders(
            Map.of("Content-Type", contentTypeHeader, "Accept", contentTypeHeader)
        ).withMethod(RestRequest.Method.GET).withPath("/some_index/_search").withParams(params).build();

        action.handleRequest(request, new FakeRestChannel(request, false, 1), verifyingClient);
    }

    /**
     * Using an illegal search type on the request should throw an error
     */
    public void testIllegalSearchType() {
        Map<String, String> params = new HashMap<>();
        params.put("search_type", "some_search_type");

        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withHeaders(
            Map.of("Content-Type", contentTypeHeader, "Accept", contentTypeHeader)
        ).withMethod(RestRequest.Method.GET).withPath("/some_index/_search").withParams(params).build();

        Exception ex = expectThrows(IllegalArgumentException.class, () -> action.prepareRequest(request, verifyingClient));
        assertEquals("No search type for [some_search_type]", ex.getMessage());
    }
}
