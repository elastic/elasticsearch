/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.rest.action.search;

import org.elasticsearch.action.explain.ExplainRequest;
import org.elasticsearch.action.explain.ExplainResponse;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.index.SliceIndexing;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.test.rest.RestActionTestCase;
import org.junit.Before;

import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.Mockito.mock;

public class RestExplainActionTests extends RestActionTestCase {
    private RestExplainAction action;

    @Before
    public void setUpAction() {
        action = new RestExplainAction();
        controller().registerHandler(action);
        verifyingClient.setExecuteVerifier((actionType, request) -> new ExplainResponse("test", "1", false));
    }

    public void testSliceParamMappedToRouting() throws Exception {
        assumeTrue("slice indexing feature flag must be enabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());
        final String sliceValue = randomAlphaOfLengthBetween(1, 8);
        verifyingClient.setExecuteVerifier((actionType, request) -> {
            assertThat(request, instanceOf(ExplainRequest.class));
            ExplainRequest explainRequest = (ExplainRequest) request;
            assertThat(explainRequest.routing(), equalTo(sliceValue));
            assertThat(explainRequest.isRoutingFromSlice(), equalTo(true));
            return new ExplainResponse("test", "1", false);
        });
        RestRequest explainRequest = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.GET)
            .withPath("/test/_explain/1")
            .withParams(Map.of("index", "test", "id", "1", "_slice", sliceValue, "q", "field:value"))
            .build();
        dispatchRequest(explainRequest);
    }

    public void testSliceAndRoutingParamsAreMutuallyExclusive() {
        assumeTrue("slice indexing feature flag must be enabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());
        RestRequest explainRequest = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.GET)
            .withPath("/test/_explain/1")
            .withParams(Map.of("index", "test", "id", "1", "_slice", "s1", "routing", "r1"))
            .build();
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> action.prepareRequest(explainRequest, mock(NodeClient.class))
        );
        assertThat(e.getMessage(), containsString("[routing] is not allowed together with [_slice]"));
    }

    public void testSliceParamRejectedWhenFeatureDisabled() {
        assumeFalse("slice indexing feature flag must be disabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());
        RestRequest explainRequest = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.GET)
            .withPath("/test/_explain/1")
            .withParams(Map.of("index", "test", "id", "1", "_slice", "s1"))
            .build();
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> action.prepareRequest(explainRequest, mock(NodeClient.class))
        );
        assertThat(e.getMessage(), containsString("request does not support [_slice]"));
    }

    public void testSliceParamRejectedWhenInvalid() {
        assumeTrue("slice indexing feature flag must be enabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());
        RestRequest explainRequest = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.GET)
            .withPath("/test/_explain/1")
            .withParams(Map.of("index", "test", "id", "1", "_slice", "_all"))
            .build();
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> action.prepareRequest(explainRequest, mock(NodeClient.class))
        );
        assertThat(e.getMessage(), containsString("[_slice] must be a single value for explain requests"));
    }

    public void testSliceParamRejectedWhenCommaDelimited() {
        assumeTrue("slice indexing feature flag must be enabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());
        RestRequest explainRequest = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.GET)
            .withPath("/test/_explain/1")
            .withParams(Map.of("index", "test", "id", "1", "_slice", "s1,s2"))
            .build();
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> action.prepareRequest(explainRequest, mock(NodeClient.class))
        );
        assertThat(e.getMessage(), containsString("[_slice] must be a single value for explain requests"));
    }
}
