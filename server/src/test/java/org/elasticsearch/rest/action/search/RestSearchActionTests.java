/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.rest.action.search;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.RestApiVersion;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.test.rest.FakeRestChannel;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.test.rest.RestActionTestCase;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.NamedXContentRegistry.Entry;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentType;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyList;

public class RestSearchActionTests extends RestActionTestCase {
    final List<String> contentTypeHeader = Collections.singletonList(randomCompatibleMediaType(RestApiVersion.V_7));

    private RestSearchAction action;
    private static NamedXContentRegistry xContentRegistry;

    /**
     * setup for the whole base test class
     */
    @BeforeClass
    public static void init() {
        xContentRegistry = new NamedXContentRegistry(initCCSFlagTestQuerybuilders());
    }

    public static List<Entry> initCCSFlagTestQuerybuilders() {
        SearchModule searchModule = new SearchModule(Settings.EMPTY, emptyList());
        List<org.elasticsearch.xcontent.NamedXContentRegistry.Entry> namedXContents = searchModule.getNamedXContents();
        namedXContents.add(
            new NamedXContentRegistry.Entry(
                QueryBuilder.class,
                new ParseField(FailBeforeVersionQueryBuilder.NAME),
                FailBeforeVersionQueryBuilder::fromXContent,
                RestApiVersion.onOrAfter(RestApiVersion.current())
            )
        );
        namedXContents.add(
            new NamedXContentRegistry.Entry(
                QueryBuilder.class,
                new ParseField(NewlyReleasedQueryBuilder.NAME),
                NewlyReleasedQueryBuilder::fromXContent,
                RestApiVersion.onOrAfter(RestApiVersion.current())
            )
        );
        return namedXContents;
    }

    @AfterClass
    public static void afterClass() throws Exception {
        xContentRegistry = null;
    }

    @Before
    public void setUpAction() {
        action = new RestSearchAction();
        controller().registerHandler(action);
        verifyingClient.setExecuteVerifier((actionType, request) -> Mockito.mock(SearchResponse.class));
        verifyingClient.setExecuteLocallyVerifier((actionType, request) -> Mockito.mock(SearchResponse.class));
    }

    public void testTypeInPath() {
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withHeaders(
            Map.of("Content-Type", contentTypeHeader, "Accept", contentTypeHeader)
        ).withMethod(RestRequest.Method.GET).withPath("/some_index/some_type/_search").build();

        dispatchRequest(request);
        assertCriticalWarnings(RestSearchAction.TYPES_DEPRECATION_MESSAGE);
    }

    public void testTypeParameter() {
        Map<String, String> params = new HashMap<>();
        params.put("type", "some_type");

        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withHeaders(
            Map.of("Content-Type", contentTypeHeader, "Accept", contentTypeHeader)
        ).withMethod(RestRequest.Method.GET).withPath("/some_index/_search").withParams(params).build();

        dispatchRequest(request);
        assertCriticalWarnings(RestSearchAction.TYPES_DEPRECATION_MESSAGE);
    }

    /**
     * The "enable_fields_emulation" flag on search requests is a no-op but should not raise an error
     */
    public void testEnableFieldsEmulationNoErrors() throws Exception {
        Map<String, String> params = new HashMap<>();
        params.put("enable_fields_emulation", "true");

        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withHeaders(
            Map.of("Content-Type", contentTypeHeader, "Accept", contentTypeHeader)
        ).withMethod(RestRequest.Method.GET).withPath("/some_index/_search").withParams(params).build();

        action.handleRequest(request, new FakeRestChannel(request, false, 1), verifyingClient);
    }

    /**
     * Using an illegal search type on the request should throw an error
     */
    public void testIllegalSearchType() {
        Map<String, String> params = Collections.singletonMap("search_type", "some_search_type");

        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withHeaders(
            Map.of("Content-Type", contentTypeHeader, "Accept", contentTypeHeader)
        ).withMethod(RestRequest.Method.GET).withPath("/some_index/_search").withParams(params).build();

        Exception ex = expectThrows(IllegalArgumentException.class, () -> action.prepareRequest(request, verifyingClient));
        assertEquals("No search type for [some_search_type]", ex.getMessage());
    }

    public void testCCSCheckCompatibilityFlag() throws IOException {
        Map<String, String> params = new HashMap<>();
        params.put(CCSVersionCheckHelper.CCS_VERSION_CHECK_FLAG, "true");

        String query = """
            { "query" : { "fail_before_current_version" : { }}}
            """;

        {
            RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.POST)
                .withPath("/some_index/_search")
                .withParams(params)
                .withContent(new BytesArray(query), XContentType.JSON)
                .build();

            Exception ex = expectThrows(IllegalArgumentException.class, () -> action.prepareRequest(request, verifyingClient));
            assertEquals(
                "parts of request [POST /some_index/_search] are not compatible with version 8.0.0 and the 'check_ccs_compatibility' "
                + "is enabled.",
                ex.getMessage()
            );
            assertEquals("This query isn't serializable to nodes on or before 8.0.0", ex.getCause().getMessage());
        }

        String newQueryBuilderInside = """
            { "query" : { "new_released_query" : { }}}
            """;

        {
            RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.POST)
                .withPath("/some_index/_search")
                .withParams(params)
                .withContent(new BytesArray(newQueryBuilderInside), XContentType.JSON)
                .build();

            Exception ex = expectThrows(IllegalArgumentException.class, () -> action.prepareRequest(request, verifyingClient));
            assertEquals(
                "parts of request [POST /some_index/_search] are not compatible with version 8.0.0 and the 'check_ccs_compatibility' "
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
        }
        {
            RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.POST)
                .withPath("/some_index/_search")
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
