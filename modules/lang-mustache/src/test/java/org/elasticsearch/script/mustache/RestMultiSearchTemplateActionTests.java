/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script.mustache;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.RestApiVersion;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.search.CCSVersionCheckHelper;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.test.rest.RestActionTestCase;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentType;
import org.junit.Before;
import org.mockito.Mockito;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class RestMultiSearchTemplateActionTests extends RestActionTestCase {
    final List<String> contentTypeHeader = Collections.singletonList(compatibleMediaType(XContentType.VND_JSON, RestApiVersion.V_7));
    private RestMultiSearchTemplateAction action;
    private static NamedXContentRegistry xContentRegistry;

    @Before
    public void setUpAction() {
        action = new RestMultiSearchTemplateAction(Settings.EMPTY);
        controller().registerHandler(action);
        // todo how to workaround this? we get AssertionError without this
        verifyingClient.setExecuteVerifier((actionType, request) -> Mockito.mock(MultiSearchTemplateResponse.class));
        verifyingClient.setExecuteLocallyVerifier((actionType, request) -> Mockito.mock(MultiSearchTemplateResponse.class));
    }

    public void testTypeInPath() {
        String content = """
            { "index": "some_index" }
            {"source": {"query" : {"match_all" :{}}}}
            """;
        BytesArray bytesContent = new BytesArray(content.getBytes(StandardCharsets.UTF_8));

        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withHeaders(
            Map.of("Content-Type", contentTypeHeader, "Accept", contentTypeHeader)
        ).withMethod(RestRequest.Method.GET).withPath("/some_index/some_type/_msearch/template").withContent(bytesContent, null).build();

        dispatchRequest(request);
        assertCriticalWarnings(RestMultiSearchTemplateAction.TYPES_DEPRECATION_MESSAGE);
    }

    public void testTypeInBody() {
        String content = """
            { "index": "some_index", "type": "some_type" }\s
            {"source": {"query" : {"match_all" :{}}}}\s
            """;
        BytesArray bytesContent = new BytesArray(content.getBytes(StandardCharsets.UTF_8));

        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withHeaders(
            Map.of("Content-Type", contentTypeHeader, "Accept", contentTypeHeader)
        ).withPath("/some_index/_msearch/template").withContent(bytesContent, null).build();

        dispatchRequest(request);
        assertCriticalWarnings(RestMultiSearchTemplateAction.TYPES_DEPRECATION_MESSAGE);
    }

    public void testCCSCheckCompatibilityParameter() {
        Map<String, String> params = new HashMap<>();

        String query = """
            { "index": "some_index" }\s
            {"source": {"query" : {"match_all" :{}}}}\s
            """;

        {
            RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.GET)
                .withPath("/some_index/_msearch/template")
                .withParams(params)
                .withContent(new BytesArray(query), XContentType.JSON)
                .build();

            SetOnce<Boolean> executeCalled = new SetOnce<>();
            verifyingClient.setExecuteVerifier((actionType, multiSearchTemplateRequest) -> {
                assertThat(multiSearchTemplateRequest, instanceOf(MultiSearchTemplateRequest.class));
                assertFalse(((MultiSearchTemplateRequest) multiSearchTemplateRequest).requests().get(0).getCcsCompatibilityCheck());
                executeCalled.set(true);
                return null;
            });
            dispatchRequest(request);
            assertThat(executeCalled.get(), equalTo(true));
        }

        {
            params.put(CCSVersionCheckHelper.CCS_VERSION_CHECK_FLAG, "true");
            RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.GET)
                .withPath("/some_index/_msearch/template")
                .withParams(params)
                .withContent(new BytesArray(query), XContentType.JSON)
                .build();

            SetOnce<Boolean> executeCalled = new SetOnce<>();
            verifyingClient.setExecuteVerifier((actionType, multiSearchTemplateRequest) -> {
                assertThat(multiSearchTemplateRequest, instanceOf(MultiSearchTemplateRequest.class));
                assertTrue(((MultiSearchTemplateRequest) multiSearchTemplateRequest).requests().get(0).getCcsCompatibilityCheck());
                executeCalled.set(true);
                return null;
            });
            dispatchRequest(request);
            assertThat(executeCalled.get(), equalTo(true));
        }
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        return xContentRegistry;
    }
}
