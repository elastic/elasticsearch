/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.reindex;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.core.RestApiVersion;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.search.RestSearchAction;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.test.rest.RestActionTestCase;
import org.elasticsearch.xcontent.XContentType;
import org.junit.Before;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class RestUpdateByQueryActionTests extends RestActionTestCase {

    private final RestUpdateByQueryAction action = new RestUpdateByQueryAction();

    @Before
    public void setUpAction() {
        controller().registerHandler(action);
        verifyingClient.setExecuteVerifier((actionType, request) -> Mockito.mock(BulkByScrollResponse.class));
        verifyingClient.setExecuteLocallyVerifier((actionType, request) -> Mockito.mock(BulkByScrollResponse.class));
    }

    public void testTypeInPath() {
        var contentTypeHeader = List.of(compatibleMediaType(XContentType.VND_JSON, RestApiVersion.V_7));
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withHeaders(
            Map.of("Content-Type", contentTypeHeader, "Accept", contentTypeHeader)
        ).withMethod(RestRequest.Method.POST).withPath("/some_index/some_type/_update_by_query").build();

        // checks the type in the URL is propagated correctly to the request object
        // only works after the request is dispatched, so its params are filled from url.
        dispatchRequest(request);

        // RestUpdateByQueryAction itself doesn't check for a deprecated type usage
        // checking here for a deprecation from its internal search request
        assertCriticalWarnings(RestSearchAction.TYPES_DEPRECATION_MESSAGE);
    }

    public void testSetsScrollByDefault() throws IOException {
        var httpRequest = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.POST)
            .withPath("/my-index/_update_by_query")
            .withContent(new BytesArray("{}"), XContentType.JSON)
            .build();

        var transportRequest = action.buildRequest(httpRequest, writableRegistry());

        assertThat(transportRequest.getSearchRequest().scroll(), notNullValue());
    }

    public void testNoScrollWhenMaxDocsIsLessThenScrollSize() throws IOException {
        var httpRequest = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.POST)
            .withPath("/my-index/_update_by_query")
            .withParams(Map.of("scroll_size", "10"))
            .withContent(new BytesArray("""
                {
                  "max_docs": 1
                }
                """), XContentType.JSON)
            .build();

        var transportRequest = action.buildRequest(httpRequest, writableRegistry());

        assertThat(transportRequest.getSearchRequest().scroll(), nullValue());
    }

    public void testSetsScrollWhenMaxDocsIsLessThenScrollSizeAndProceedOnConflict() throws IOException {
        var httpRequest = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.POST)
            .withPath("/my-index/_update_by_query")
            .withParams(Map.of("scroll_size", "10", "conflicts", "proceed"))
            .withContent(new BytesArray("""
                {
                  "max_docs": 1
                }
                """), XContentType.JSON)
            .build();

        var transportRequest = action.buildRequest(httpRequest, writableRegistry());

        assertThat(transportRequest.getSearchRequest().scroll(), notNullValue());
    }
}
