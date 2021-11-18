/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest.action.search;

import org.elasticsearch.action.explain.ExplainResponse;
import org.elasticsearch.core.RestApiVersion;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.test.rest.RestActionTestCase;
import org.elasticsearch.xcontent.XContentType;
import org.junit.Before;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class RestExplainActionTests extends RestActionTestCase {
    final List<String> contentTypeHeader = Collections.singletonList(compatibleMediaType(XContentType.VND_JSON, RestApiVersion.V_7));

    @Before
    public void setUpAction() {
        RestExplainAction action = new RestExplainAction();
        controller().registerHandler(action);
        verifyingClient.setExecuteVerifier((actionType, request) -> Mockito.mock(ExplainResponse.class));
        verifyingClient.setExecuteLocallyVerifier((actionType, request) -> Mockito.mock(ExplainResponse.class));
    }

    public void testTypeInPath() {
        RestRequest deprecatedRequest = new FakeRestRequest.Builder(xContentRegistry()).withHeaders(Map.of("Accept", contentTypeHeader))
            .withMethod(RestRequest.Method.GET)
            .withPath("/some_index/some_type/some_id/_explain")
            .build();
        dispatchRequest(deprecatedRequest);
        assertCriticalWarnings(RestExplainAction.TYPES_DEPRECATION_MESSAGE);

        RestRequest validRequest = new FakeRestRequest.Builder(xContentRegistry()).withHeaders(Map.of("Accept", contentTypeHeader))
            .withMethod(RestRequest.Method.GET)
            .withPath("/some_index/_explain/some_id")
            .build();
        dispatchRequest(validRequest);
    }

}
