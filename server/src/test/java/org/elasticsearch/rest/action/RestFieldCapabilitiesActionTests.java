/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest.action;

import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.core.RestApiVersion;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.search.CCSVersionCheckHelper;
import org.elasticsearch.rest.action.search.RestSearchActionTests;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.test.rest.RestActionTestCase;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentType;
import org.junit.Before;
import org.junit.BeforeClass;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RestFieldCapabilitiesActionTests extends RestActionTestCase {
    final List<String> contentTypeHeader = Collections.singletonList(compatibleMediaType(XContentType.VND_JSON, RestApiVersion.V_7));

    private RestFieldCapabilitiesAction action;
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
        action = new RestFieldCapabilitiesAction();
        controller().registerHandler(action);
        verifyingClient.setExecuteVerifier((actionType, request) -> Mockito.mock(MultiSearchResponse.class));
        verifyingClient.setExecuteLocallyVerifier((actionType, request) -> Mockito.mock(MultiSearchResponse.class));
    }

    public void testCCSCheckCompatibilityFlag() throws IOException {
        Map<String, String> params = new HashMap<>();
        params.put(CCSVersionCheckHelper.CCS_VERSION_CHECK_FLAG, "true");
        params.put("fields", "*");

        String query = """
            { "index_filter": { "fail_before_current_version" : { }}}
            """;

        {
            RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.GET)
                .withPath("/some_index/_field_caps")
                .withParams(params)
                .withContent(new BytesArray(query), XContentType.JSON)
                .build();

            Exception ex = expectThrows(IllegalArgumentException.class, () -> action.prepareRequest(request, verifyingClient));
            assertEquals(
                "parts of request [GET /some_index/_field_caps] are not compatible with version 8.0.0 and the 'check_ccs_compatibility' "
                + "is enabled.",
                ex.getMessage()
            );
            assertEquals("This query isn't serializable to nodes on or before 8.0.0", ex.getCause().getMessage());
        }

        String newQueryBuilderInside = """
            { "index_filter": { "new_released_query" : { }}}
            """;

        {
            RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.GET)
                .withPath("/some_index/_field_caps")
                .withParams(params)
                .withContent(new BytesArray(newQueryBuilderInside), XContentType.JSON)
                .build();

            Exception ex = expectThrows(IllegalArgumentException.class, () -> action.prepareRequest(request, verifyingClient));
            assertEquals(
                "parts of request [GET /some_index/_field_caps] are not compatible with version 8.0.0 and the 'check_ccs_compatibility' "
                + "is enabled.",
                ex.getMessage()
            );
            assertEquals(
                "NamedWritable [org.elasticsearch.rest.action.search.NewlyReleasedQueryBuilder] was released in "
                    + "version 8.1.0 and was not supported in version 8.0.0",
                ex.getCause().getMessage()
            );
        }

        // this shouldn't fail without the flag enabled
        params = new HashMap<>();
        if (randomBoolean()) {
            params.put("check_ccs_compatibility", "false");
            params.put("fields", "*");
        }
        {
            RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.GET)
                .withPath("/some_index/_field_caps")
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
