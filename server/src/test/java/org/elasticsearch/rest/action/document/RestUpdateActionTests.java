/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.rest.action.document;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.index.SliceIndexing;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.test.rest.RestActionTestCase;
import org.elasticsearch.xcontent.XContentType;
import org.junit.Before;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.Mockito.mock;

public final class RestUpdateActionTests extends RestActionTestCase {
    private RestUpdateAction action;

    @Before
    public void setUpAction() {
        action = new RestUpdateAction();
        controller().registerHandler(action);
        verifyingClient.setExecuteVerifier((actionType, request) -> Mockito.mock(UpdateResponse.class));
        verifyingClient.setExecuteLocallyVerifier((actionType, request) -> Mockito.mock(UpdateResponse.class));
    }

    public void testUpdateDocVersion() {
        Map<String, String> params = new HashMap<>();
        if (randomBoolean()) {
            params.put("version", Long.toString(randomNonNegativeLong()));
            params.put("version_type", randomFrom(VersionType.values()).name());
        } else if (randomBoolean()) {
            params.put("version", Long.toString(randomNonNegativeLong()));
        } else {
            params.put("version_type", randomFrom(VersionType.values()).name());
        }
        String content = """
            {
                "doc" : {
                    "name" : "new_name"
                }
            }""";
        FakeRestRequest updateRequest = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.POST)
            .withPath("test/_update/1")
            .withParams(params)
            .withContent(new BytesArray(content), XContentType.JSON)
            .build();
        ActionRequestValidationException e = expectThrows(
            ActionRequestValidationException.class,
            () -> action.prepareRequest(updateRequest, mock(NodeClient.class))
        );
        assertThat(
            e.getMessage(),
            containsString(
                "internal versioning can not be used for optimistic concurrency control. "
                    + "Please use `if_seq_no` and `if_primary_term` instead"
            )
        );
    }

    public void testSliceParamMappedToRouting() throws Exception {
        assumeTrue("slice indexing feature flag must be enabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());
        final String sliceValue = randomAlphaOfLengthBetween(1, 8);
        verifyingClient.setExecuteVerifier((actionType, request) -> {
            assertThat(request, instanceOf(UpdateRequest.class));
            UpdateRequest updateRequest = (UpdateRequest) request;
            assertThat(updateRequest.routing(), equalTo(sliceValue));
            assertThat(updateRequest.isRoutingFromSlice(), equalTo(true));
            assertThat(updateRequest.doc().routing(), equalTo(sliceValue));
            assertThat(updateRequest.doc().isRoutingFromSlice(), equalTo(true));
            assertThat(updateRequest.upsertRequest().routing(), equalTo(sliceValue));
            assertThat(updateRequest.upsertRequest().isRoutingFromSlice(), equalTo(true));
            return Mockito.mock(UpdateResponse.class);
        });
        String content = """
            {
                "doc" : {
                    "name" : "new_name"
                },
                "upsert": {
                    "name": "created_name"
                }
            }""";
        FakeRestRequest updateRequest = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.POST)
            .withPath("/test/_update/1")
            .withParams(Map.of("_slice", sliceValue))
            .withContent(new BytesArray(content), XContentType.JSON)
            .build();
        dispatchRequest(updateRequest);
    }

    public void testSliceAndRoutingParamsAreMutuallyExclusive() {
        assumeTrue("slice indexing feature flag must be enabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());
        String content = """
            {
                "doc" : {
                    "name" : "new_name"
                }
            }""";
        FakeRestRequest updateRequest = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.POST)
            .withPath("test/_update/1")
            .withParams(Map.of("_slice", "s1", "routing", "r1"))
            .withContent(new BytesArray(content), XContentType.JSON)
            .build();
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> action.prepareRequest(updateRequest, mock(NodeClient.class))
        );
        assertThat(e.getMessage(), containsString("[routing] is not allowed together with [_slice]"));
    }

    public void testSliceParamRejectedWhenFeatureDisabled() {
        assumeFalse("slice indexing feature flag must be disabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());
        String content = """
            {
                "doc" : {
                    "name" : "new_name"
                }
            }""";
        FakeRestRequest updateRequest = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.POST)
            .withPath("test/_update/1")
            .withParams(Map.of("_slice", "s1"))
            .withContent(new BytesArray(content), XContentType.JSON)
            .build();
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> action.prepareRequest(updateRequest, mock(NodeClient.class))
        );
        assertThat(e.getMessage(), containsString("request does not support [_slice]"));
    }

    public void testSliceParamRejectedWhenInvalid() {
        assumeTrue("slice indexing feature flag must be enabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());
        String content = """
            {
                "doc" : {
                    "name" : "new_name"
                }
            }""";
        FakeRestRequest updateRequest = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.POST)
            .withPath("test/_update/1")
            .withParams(Map.of("_slice", "_all"))
            .withContent(new BytesArray(content), XContentType.JSON)
            .build();
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> action.prepareRequest(updateRequest, mock(NodeClient.class))
        );
        assertThat(e.getMessage(), containsString("invalid [_slice] value"));
    }

    public void testSliceParamRejectedWhenCommaDelimited() {
        assumeTrue("slice indexing feature flag must be enabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());
        String content = """
            {
                "doc" : {
                    "name" : "new_name"
                }
            }""";
        FakeRestRequest updateRequest = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.POST)
            .withPath("test/_update/1")
            .withParams(Map.of("_slice", "s1,s2"))
            .withContent(new BytesArray(content), XContentType.JSON)
            .build();
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> action.prepareRequest(updateRequest, mock(NodeClient.class))
        );
        assertThat(e.getMessage(), containsString("invalid [_slice] value"));
    }
}
