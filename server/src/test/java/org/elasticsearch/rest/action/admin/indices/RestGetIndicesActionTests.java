/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest.action.admin.indices;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.core.RestApiVersion;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.FakeRestRequest;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.rest.BaseRestHandler.INCLUDE_TYPE_NAME_PARAMETER;
import static org.mockito.Mockito.mock;

public class RestGetIndicesActionTests extends ESTestCase {
    final List<String> contentTypeHeader = Collections.singletonList(randomCompatibleMediaType(RestApiVersion.V_7));

    /**
     * Test that setting the "include_type_name" parameter raises a warning for the GET request
     */
    public void testIncludeTypeNamesWarning() throws IOException {
        Map<String, String> params = new HashMap<>();
        params.put(INCLUDE_TYPE_NAME_PARAMETER, randomFrom("true", "false"));
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry())
            .withHeaders(Map.of("Content-Type", contentTypeHeader, "Accept", contentTypeHeader))
            .withMethod(RestRequest.Method.GET)
            .withPath("/some_index")
            .withParams(params)
            .build();

        RestGetIndicesAction handler = new RestGetIndicesAction();
        handler.prepareRequest(request, mock(NodeClient.class));
        assertWarnings(RestGetIndicesAction.TYPES_DEPRECATION_MESSAGE);

        // the same request without the parameter should pass without warning
        request = new FakeRestRequest.Builder(xContentRegistry())
            .withHeaders(Map.of("Content-Type", contentTypeHeader, "Accept", contentTypeHeader))
            .withMethod(RestRequest.Method.GET)
            .withPath("/some_index")
            .build();
        handler.prepareRequest(request, mock(NodeClient.class));
    }

    /**
     * Test that setting the "include_type_name" parameter doesn't raises a warning if the HEAD method is used (indices.exists)
     */
    public void testIncludeTypeNamesWarningExists() throws IOException {
        Map<String, String> params = new HashMap<>();
        params.put(INCLUDE_TYPE_NAME_PARAMETER, randomFrom("true", "false"));
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry())
            .withHeaders(Map.of("Content-Type", contentTypeHeader, "Accept", contentTypeHeader))
            .withMethod(RestRequest.Method.HEAD)
            .withPath("/some_index")
            .withParams(params)
            .build();

        RestGetIndicesAction handler = new RestGetIndicesAction();
        handler.prepareRequest(request, mock(NodeClient.class));
    }
}
