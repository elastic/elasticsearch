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
import org.elasticsearch.common.RestApiVersion;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.test.rest.RestActionTestCase;
import org.junit.Before;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.instanceOf;

public class RestGetActionV7Tests extends RestActionTestCase {
    final List<String> contentTypeHeader =
        Collections.singletonList("application/vnd.elasticsearch+json;compatible-with="+ RestApiVersion.V_7.major);

    @Before
    public void setUpAction() {
        controller().registerHandler(new RestGetActionV7());
        verifyingClient.setExecuteVerifier((actionType, request) -> {
            assertThat(request, instanceOf(GetRequest.class));
            return Mockito.mock(GetResponse.class);
        });
    }

    public void testTypeInPathWithGet() {
        FakeRestRequest.Builder deprecatedRequest = new FakeRestRequest.Builder(xContentRegistry()).withHeaders(
            Map.of("Content-Type", contentTypeHeader, "Accept", contentTypeHeader)
        ).withPath("/some_index/some_type/some_id");
        dispatchRequest(deprecatedRequest.withMethod(RestRequest.Method.GET).build());
        assertWarnings(RestGetActionV7.TYPES_DEPRECATION_MESSAGE);
    }

    public void testTypeInPathWithHead() {
        FakeRestRequest.Builder deprecatedRequest = new FakeRestRequest.Builder(xContentRegistry()).withHeaders(
            Map.of("Content-Type", contentTypeHeader, "Accept", contentTypeHeader)
        ).withPath("/some_index/some_type/some_id");
        dispatchRequest(deprecatedRequest.withMethod(RestRequest.Method.HEAD).build());
        assertWarnings(RestGetActionV7.TYPES_DEPRECATION_MESSAGE);
    }
}
