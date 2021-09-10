/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest.action.document;

import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.core.RestApiVersion;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.test.rest.RestActionTestCase;
import org.junit.Before;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.instanceOf;

public class RestGetActionTests extends RestActionTestCase {
    final List<String> contentTypeHeader = Collections.singletonList(randomCompatibleMediaType(RestApiVersion.V_7));

    @Before
    public void setUpAction() {
        controller().registerHandler(new RestGetAction());
        verifyingClient.setExecuteVerifier((actionType, request) -> {
            assertThat(request, instanceOf(GetRequest.class));
            return Mockito.mock(GetResponse.class);
        });
    }

    public void testTypeInPath() {
        testTypeInPath(RestRequest.Method.GET);
        testTypeInPath(RestRequest.Method.HEAD);
    }

    private void testTypeInPath(RestRequest.Method method) {
        FakeRestRequest.Builder deprecatedRequest = new FakeRestRequest.Builder(xContentRegistry()).withHeaders(
            Map.of("Content-Type", contentTypeHeader, "Accept", contentTypeHeader)
        ).withPath("/some_index/some_type/some_id");
        dispatchRequest(deprecatedRequest.withMethod(method).build());
        assertWarnings(RestGetAction.TYPES_DEPRECATION_MESSAGE);
    }
}
