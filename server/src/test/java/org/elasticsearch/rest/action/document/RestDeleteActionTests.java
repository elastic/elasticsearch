/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.rest.action.document;

import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.test.rest.RestActionTestCase;
import org.junit.Before;
import org.mockito.Mockito;

import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.Mockito.mock;

public class RestDeleteActionTests extends RestActionTestCase {
    private RestDeleteAction action;

    @Before
    public void setUpAction() {
        action = new RestDeleteAction();
        controller().registerHandler(action);
        verifyingClient.setExecuteVerifier((actionType, request) -> Mockito.mock(DeleteResponse.class));
    }

    public void testSliceParamMappedToRouting() throws Exception {
        final String sliceValue = randomAlphaOfLengthBetween(1, 8);
        verifyingClient.setExecuteVerifier((actionType, request) -> {
            assertThat(request, instanceOf(DeleteRequest.class));
            DeleteRequest deleteRequest = (DeleteRequest) request;
            assertThat(deleteRequest.routing(), equalTo(sliceValue));
            assertThat(deleteRequest.isRoutingFromSlice(), equalTo(true));
            return Mockito.mock(DeleteResponse.class);
        });
        RestRequest deleteRequest = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.DELETE)
            .withPath("/test/_doc/1")
            .withParams(Map.of("index", "test", "id", "1", "slice", sliceValue))
            .build();
        dispatchRequest(deleteRequest);
    }

    public void testSliceAndRoutingParamsAreMutuallyExclusive() {
        RestRequest deleteRequest = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.DELETE)
            .withPath("/test/_doc/1")
            .withParams(Map.of("index", "test", "id", "1", "slice", "s1", "routing", "r1"))
            .build();
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> action.prepareRequest(deleteRequest, mock(NodeClient.class))
        );
        assertThat(e.getMessage(), containsString("[routing] is not allowed together with [slice]"));
    }

    public void testSliceParamRejectedWhenInvalid() {
        RestRequest deleteRequest = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.DELETE)
            .withPath("/test/_doc/1")
            .withParams(Map.of("index", "test", "id", "1", "slice", "_all"))
            .build();
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> action.prepareRequest(deleteRequest, mock(NodeClient.class))
        );
        assertThat(e.getMessage(), containsString("invalid [slice] value"));
    }

    public void testSliceParamRejectedWhenCommaDelimited() {
        RestRequest deleteRequest = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.DELETE)
            .withPath("/test/_doc/1")
            .withParams(Map.of("index", "test", "id", "1", "slice", "s1,s2"))
            .build();
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> action.prepareRequest(deleteRequest, mock(NodeClient.class))
        );
        assertThat(e.getMessage(), containsString("invalid [slice] value"));
    }
}
