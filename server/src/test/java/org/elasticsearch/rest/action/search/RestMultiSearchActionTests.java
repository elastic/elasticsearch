/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest.action.search;

import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.RestApiVersion;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.test.rest.RestActionTestCase;
import org.elasticsearch.usage.UsageService;
import org.elasticsearch.xcontent.XContentType;
import org.junit.Before;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.mock;

public final class RestMultiSearchActionTests extends RestActionTestCase {
    final List<String> contentTypeHeader = Collections.singletonList(compatibleMediaType(XContentType.VND_JSON, RestApiVersion.V_7));

    @Before
    public void setUpAction() {
        RestMultiSearchAction action = new RestMultiSearchAction(Settings.EMPTY, new UsageService().getSearchUsageHolder(), nf -> false);
        controller().registerHandler(action);
        verifyingClient.setExecuteVerifier((actionType, request) -> mock(MultiSearchResponse.class));
        verifyingClient.setExecuteLocallyVerifier((actionType, request) -> mock(MultiSearchResponse.class));
    }

    public void testTypeInPath() {
        String content = "{ \"index\": \"some_index\" } \n {} \n";
        BytesArray bytesContent = new BytesArray(content.getBytes(StandardCharsets.UTF_8));

        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withHeaders(
            Map.of("Content-Type", contentTypeHeader, "Accept", contentTypeHeader)
        ).withMethod(RestRequest.Method.GET).withPath("/some_index/some_type/_msearch").withContent(bytesContent, null).build();

        dispatchRequest(request);
        assertCriticalWarnings(RestMultiSearchAction.TYPES_DEPRECATION_MESSAGE);
    }

    public void testTypeInBody() {
        String content = "{ \"index\": \"some_index\", \"type\": \"some_type\" } \n {} \n";
        BytesArray bytesContent = new BytesArray(content.getBytes(StandardCharsets.UTF_8));

        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withHeaders(
            Map.of("Content-Type", contentTypeHeader, "Accept", contentTypeHeader)
        ).withMethod(RestRequest.Method.POST).withPath("/some_index/_msearch").withContent(bytesContent, null).build();

        dispatchRequest(request);
        assertCriticalWarnings(RestMultiSearchAction.TYPES_DEPRECATION_MESSAGE);
    }

}
