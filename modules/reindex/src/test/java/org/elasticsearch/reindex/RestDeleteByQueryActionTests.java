/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.reindex;

import org.elasticsearch.index.reindex.AbstractBulkByScrollRequest;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.search.RestSearchAction;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.test.rest.RestActionTestCase;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.junit.Before;

import java.io.IOException;

import static java.util.Collections.emptyList;

public class RestDeleteByQueryActionTests extends RestActionTestCase {
    private RestDeleteByQueryAction action;

    @Before
    public void setUpAction() {
        action = new RestDeleteByQueryAction();
        controller().registerHandler(action);
    }

    public void testTypeInPath() throws IOException {
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.POST)
            .withPath("/some_index/some_type/_delete_by_query")
            .build();

        // We're not actually testing anything to do with the client, but need to set this so it doesn't fail the test for being unset.
        verifyingClient.setExecuteLocallyVerifier((arg1, arg2) -> null);

        dispatchRequest(request);

        // checks the type in the URL is propagated correctly to the request object
        // only works after the request is dispatched, so its params are filled from url.
        DeleteByQueryRequest dbqRequest = action.buildRequest(request, DEFAULT_NAMED_WRITABLE_REGISTRY);
        assertArrayEquals(new String[] { "some_type" }, dbqRequest.getDocTypes());

        // RestDeleteByQueryAction itself doesn't check for a deprecated type usage
        // checking here for a deprecation from its internal search request
        assertWarnings(RestSearchAction.TYPES_DEPRECATION_MESSAGE);
    }

    public void testParseEmpty() throws IOException {
        final FakeRestRequest restRequest = new FakeRestRequest.Builder(new NamedXContentRegistry(emptyList())).build();
        DeleteByQueryRequest request = action.buildRequest(restRequest, DEFAULT_NAMED_WRITABLE_REGISTRY);
        assertEquals(AbstractBulkByScrollRequest.SIZE_ALL_MATCHES, request.getSize());
        assertEquals(AbstractBulkByScrollRequest.DEFAULT_SCROLL_SIZE, request.getSearchRequest().source().size());
    }
}
