/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script.mustache;

import org.elasticsearch.core.RestApiVersion;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.search.RestSearchAction;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.test.rest.RestActionTestCase;
import org.junit.Before;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RestSearchTemplateActionTests extends RestActionTestCase {
    final List<String> contentTypeHeader = Collections.singletonList(randomCompatibleMediaType(RestApiVersion.V_7));

    @Before
    public void setUpAction() {
        controller().registerHandler(new RestSearchTemplateAction());
        verifyingClient.setExecuteVerifier((actionType, request) -> Mockito.mock(SearchTemplateResponse.class));
        verifyingClient.setExecuteLocallyVerifier((actionType, request) -> Mockito.mock(SearchTemplateResponse.class));
    }

    public void testTypeInPath() {
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withHeaders(
            Map.of("Content-Type", contentTypeHeader, "Accept", contentTypeHeader)
        ).withMethod(RestRequest.Method.GET).withPath("/some_index/some_type/_search/template").build();

        dispatchRequest(request);
        assertCriticalWarnings(RestSearchAction.TYPES_DEPRECATION_MESSAGE);
    }

    public void testTypeParameter() {
        Map<String, String> params = new HashMap<>();
        params.put("type", "some_type");

        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withHeaders(
            Map.of("Content-Type", contentTypeHeader, "Accept", contentTypeHeader)
        ).withMethod(RestRequest.Method.GET).withPath("/some_index/_search/template").withParams(params).build();

        dispatchRequest(request);
        assertCriticalWarnings(RestSearchAction.TYPES_DEPRECATION_MESSAGE);
    }
}
