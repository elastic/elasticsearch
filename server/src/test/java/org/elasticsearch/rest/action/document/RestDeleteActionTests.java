/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest.action.document;

import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.core.RestApiVersion;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.test.rest.RestActionTestCase;
import org.junit.Before;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class RestDeleteActionTests extends RestActionTestCase {

    final List<String> contentTypeHeader = Collections.singletonList(randomCompatibleMediaType(RestApiVersion.V_7));

    @Before
    public void setUpAction() {
        controller().registerHandler(new RestDeleteAction());
        verifyingClient.setExecuteVerifier((actionType, request) -> Mockito.mock(DeleteResponse.class));
        verifyingClient.setExecuteLocallyVerifier((actionType, request) -> Mockito.mock(DeleteResponse.class));
    }

    public void testTypeInPath() {
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry())
            .withHeaders(Map.of("Accept", contentTypeHeader))
            .withMethod(RestRequest.Method.DELETE)
            .withPath("/some_index/some_type/some_id")
            .build();
        dispatchRequest(request);
        assertWarnings(RestDeleteAction.TYPES_DEPRECATION_MESSAGE);

        RestRequest validRequest = new FakeRestRequest.Builder(xContentRegistry())
            .withHeaders(Map.of("Accept", contentTypeHeader))
            .withMethod(RestRequest.Method.DELETE)
            .withPath("/some_index/_doc/some_id")
            .build();
        dispatchRequest(validRequest);
    }
}
