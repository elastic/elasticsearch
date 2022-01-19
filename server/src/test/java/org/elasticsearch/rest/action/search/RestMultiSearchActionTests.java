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
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentType;
import org.junit.Before;
import org.junit.BeforeClass;
import org.mockito.Mockito;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RestMultiSearchActionTests extends RestActionTestCase {
    final List<String> contentTypeHeader = Collections.singletonList(compatibleMediaType(XContentType.VND_JSON, RestApiVersion.V_7));

    private RestMultiSearchAction action;
    private static NamedXContentRegistry xContentRegistry;

    /**
     * setup for the whole base test class
     */
    @BeforeClass
    public static void init() {
        xContentRegistry = new NamedXContentRegistry(RestSearchActionTests.initCCSFlagTestQuerybuilders());
    }

    @Before
    public void setUpAction() {
        action = new RestMultiSearchAction(Settings.EMPTY);
        controller().registerHandler(action);
        verifyingClient.setExecuteVerifier((actionType, request) -> Mockito.mock(MultiSearchResponse.class));
        verifyingClient.setExecuteLocallyVerifier((actionType, request) -> Mockito.mock(MultiSearchResponse.class));
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

    private Map<String, List<String>> headersWith(String accept, List<String> value) {
        Map<String, List<String>> headers = new HashMap<>();
        headers.put(accept, value);
        return headers;
    }

    public void testCCSCheckCompatibilityFlag() throws IOException {
        Map<String, String> params = new HashMap<>();
        params.put(CCSVersionCheckHelper.CCS_VERSION_CHECK_FLAG, "true");

        String query = """
            {"index": "some_index"}
            { "query" : { "fail_before_current_version" : { }}}
            """;

        {
            RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.POST)
                .withPath("/some_index/_msearch")
                .withParams(params)
                .withContent(new BytesArray(query), XContentType.JSON)
                .build();

            Exception ex = expectThrows(IllegalArgumentException.class, () -> action.prepareRequest(request, verifyingClient));
            assertEquals(
                "parts of request [POST /some_index/_msearch] are not compatible with version 8.0.0 and the 'check_ccs_compatibility' "
                + "is enabled.",
                ex.getMessage()
            );
            assertEquals("This query isn't serializable to nodes on or before 8.0.0", ex.getCause().getMessage());
        }

        String newQueryBuilderInside = """
            {"index": "some_index"}
            { "query" : { "new_released_query" : { }}}
            """;

        {
            RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.POST)
                .withPath("/some_index/_msearch")
                .withParams(params)
                .withContent(new BytesArray(newQueryBuilderInside), XContentType.JSON)
                .build();

            Exception ex = expectThrows(IllegalArgumentException.class, () -> action.prepareRequest(request, verifyingClient));
            assertEquals(
                "parts of request [POST /some_index/_msearch] are not compatible with version 8.0.0 and the 'check_ccs_compatibility' "
                + "is enabled.",
                ex.getMessage()
            );
            assertEquals(
                "NamedWritable [org.elasticsearch.search.NewlyReleasedQueryBuilder] was released in "
                    + "version 8.1.0 and was not supported in version 8.0.0",
                ex.getCause().getMessage()
            );
        }

        // this shouldn't fail without the flag enabled
        params = new HashMap<>();
        if (randomBoolean()) {
            params.put("check_ccs_compatibility", "false");
        }
        {
            RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.POST)
                .withPath("/some_index/_msearch")
                .withParams(params)
                .withContent(new BytesArray(query), XContentType.JSON)
                .build();
            action.prepareRequest(request, verifyingClient);
        }
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        return xContentRegistry;
    }
}
