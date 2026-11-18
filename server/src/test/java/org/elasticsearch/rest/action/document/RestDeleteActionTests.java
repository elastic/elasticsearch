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
import org.elasticsearch.index.SliceIndexing;
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
        assumeTrue("slice indexing feature flag must be enabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());
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
            .withParams(Map.of("index", "test", "id", "1", "_slice", sliceValue))
            .build();
        dispatchRequest(deleteRequest);
    }

    public void testSliceAndRoutingParamsAreMutuallyExclusive() {
        assumeTrue("slice indexing feature flag must be enabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());
        RestRequest deleteRequest = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.DELETE)
            .withPath("/test/_doc/1")
            .withParams(Map.of("index", "test", "id", "1", "_slice", "s1", "routing", "r1"))
            .build();
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> action.prepareRequest(deleteRequest, mock(NodeClient.class))
        );
        assertThat(e.getMessage(), containsString("[routing] is not allowed together with [_slice]"));
    }

    public void testSliceParamRejectedWhenFeatureDisabled() {
        assumeFalse("slice indexing feature flag must be disabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());
        RestRequest deleteRequest = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.DELETE)
            .withPath("/test/_doc/1")
            .withParams(Map.of("index", "test", "id", "1", "_slice", "s1"))
            .build();
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> action.prepareRequest(deleteRequest, mock(NodeClient.class))
        );
        assertThat(e.getMessage(), containsString("request does not support [_slice]"));
    }

    public void testSliceParamRejectedWhenInvalid() {
        assumeTrue("slice indexing feature flag must be enabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());
        RestRequest deleteRequest = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.DELETE)
            .withPath("/test/_doc/1")
            .withParams(Map.of("index", "test", "id", "1", "_slice", "_all"))
            .build();
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> action.prepareRequest(deleteRequest, mock(NodeClient.class))
        );
        assertThat(e.getMessage(), containsString("invalid [_slice] value"));
    }

    public void testSliceParamRejectedWhenCommaDelimited() {
        assumeTrue("slice indexing feature flag must be enabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());
        RestRequest deleteRequest = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.DELETE)
            .withPath("/test/_doc/1")
            .withParams(Map.of("index", "test", "id", "1", "_slice", "s1,s2"))
            .build();
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> action.prepareRequest(deleteRequest, mock(NodeClient.class))
        );
        assertThat(e.getMessage(), containsString("invalid [_slice] value"));
    }
}
