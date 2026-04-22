/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.rest.action.document;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.index.SliceIndexing;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponseUtils;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.document.RestIndexAction.AutoIdHandler;
import org.elasticsearch.rest.action.document.RestIndexAction.CreateHandler;
import org.elasticsearch.test.rest.FakeRestChannel;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.test.rest.RestActionTestCase;
import org.elasticsearch.xcontent.XContentType;
import org.junit.Before;

import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public final class RestIndexActionTests extends RestActionTestCase {
    @Before
    public void setUpAction() {
        controller().registerHandler(new RestIndexAction(null, null));
        controller().registerHandler(new CreateHandler(null, null));
        controller().registerHandler(new AutoIdHandler(null, null));
    }

    public void testCreateOpTypeValidation() {
        RestIndexAction.CreateHandler create = new CreateHandler(null, null);

        String opType = randomFrom("CREATE", null);
        CreateHandler.validateOpType(opType);

        String illegalOpType = randomFrom("index", "unknown", "");
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> CreateHandler.validateOpType(illegalOpType));
        assertThat(e.getMessage(), equalTo("opType must be 'create', found: [" + illegalOpType + "]"));
    }

    public void testAutoIdDefaultsToOptypeCreate() {
        checkAutoIdOpType(DocWriteRequest.OpType.CREATE);
    }

    private void checkAutoIdOpType(DocWriteRequest.OpType expectedOpType) {
        SetOnce<Boolean> executeCalled = new SetOnce<>();
        verifyingClient.setExecuteVerifier((actionType, request) -> {
            assertThat(request, instanceOf(IndexRequest.class));
            assertThat(((IndexRequest) request).opType(), equalTo(expectedOpType));
            executeCalled.set(true);
            return new IndexResponse(new ShardId("test", "test", 0), "id", 0, 0, 0, true);
        });
        RestRequest autoIdRequest = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.POST)
            .withPath("/some_index/_doc")
            .withContent(new BytesArray("{}"), XContentType.JSON)
            .build();
        dispatchRequest(autoIdRequest);
        assertThat(executeCalled.get(), equalTo(true));
    }

    public void testSliceParamParsedWhenFeatureEnabled() {
        assumeTrue("slice indexing feature flag must be enabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());
        final String sliceValue = randomAlphaOfLengthBetween(1, 8);
        SetOnce<Boolean> executeCalled = new SetOnce<>();
        verifyingClient.setExecuteVerifier((actionType, request) -> {
            assertThat(request, instanceOf(IndexRequest.class));
            assertThat(((IndexRequest) request).routing(), equalTo(sliceValue));
            assertThat(((IndexRequest) request).isRoutingFromSlice(), equalTo(true));
            executeCalled.set(true);
            return new IndexResponse(new ShardId("test", "test", 0), "id", 0, 0, 0, true);
        });
        RestRequest indexRequest = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.POST)
            .withPath("/some_index/_doc/1")
            .withParams(Map.of("index", "some_index", "id", "1", "_slice", sliceValue))
            .withContent(new BytesArray("{}"), XContentType.JSON)
            .build();
        dispatchRequest(indexRequest);
        assertThat(executeCalled.get(), equalTo(true));
    }

    public void testSliceAndRoutingParamsAreMutuallyExclusive() {
        assumeTrue("slice indexing feature flag must be enabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());
        RestRequest indexRequest = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.POST)
            .withPath("/some_index/_doc/1")
            .withParams(Map.of("index", "some_index", "id", "1", "_slice", "s1", "routing", "r1"))
            .withContent(new BytesArray("{}"), XContentType.JSON)
            .build();
        FakeRestChannel channel = dispatchRequestWithChannel(indexRequest);
        try (var response = channel.capturedResponse()) {
            assertThat(response.status(), equalTo(RestStatus.BAD_REQUEST));
            assertThat(RestResponseUtils.getBodyContent(response).utf8ToString(), containsString("illegal_argument_exception"));
            assertThat(
                RestResponseUtils.getBodyContent(response).utf8ToString(),
                containsString("[routing] is not allowed together with [_slice]")
            );
        }
    }

    public void testSliceParamRejectedWhenFeatureDisabled() {
        assumeFalse("slice indexing feature flag must be disabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());
        RestRequest indexRequest = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.POST)
            .withPath("/some_index/_doc/1")
            .withParams(Map.of("index", "some_index", "id", "1", "_slice", "s1"))
            .withContent(new BytesArray("{}"), XContentType.JSON)
            .build();
        FakeRestChannel channel = dispatchRequestWithChannel(indexRequest);
        try (var response = channel.capturedResponse()) {
            assertThat(response.status(), equalTo(RestStatus.BAD_REQUEST));
            assertThat(RestResponseUtils.getBodyContent(response).utf8ToString(), containsString("illegal_argument_exception"));
            assertThat(RestResponseUtils.getBodyContent(response).utf8ToString(), containsString("request does not support [_slice]"));
        }
    }

    public void testSliceParamRejectedWhenInvalid() {
        assumeTrue("slice indexing feature flag must be enabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());
        RestRequest indexRequest = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.POST)
            .withPath("/some_index/_doc/1")
            .withParams(Map.of("index", "some_index", "id", "1", "_slice", "_all"))
            .withContent(new BytesArray("{}"), XContentType.JSON)
            .build();
        FakeRestChannel channel = dispatchRequestWithChannel(indexRequest);
        try (var response = channel.capturedResponse()) {
            assertThat(response.status(), equalTo(RestStatus.BAD_REQUEST));
            assertThat(RestResponseUtils.getBodyContent(response).utf8ToString(), containsString("invalid [_slice] value"));
        }
    }

    private FakeRestChannel dispatchRequestWithChannel(RestRequest request) {
        FakeRestChannel channel = new FakeRestChannel(request, true);
        var threadContext = verifyingClient.threadPool().getThreadContext();
        try (var ignore = threadContext.stashContext()) {
            controller().dispatchRequest(request, channel, threadContext);
        }
        return channel;
    }

}
