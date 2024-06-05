/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.rest.action.search;

import org.elasticsearch.core.RestApiVersion;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.test.rest.RestActionTestCase;
import org.junit.Before;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class RestKnnSearchActionTests extends RestActionTestCase {
    private List<String> contentTypeHeader;
    private RestKnnSearchAction action;

    @Before
    public void setUpAction() {
        action = new RestKnnSearchAction();
        controller().registerHandler(action);
        contentTypeHeader = Collections.singletonList(randomCompatibleMediaType(RestApiVersion.V_8));
    }

    public void testDeprecation() {
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withHeaders(
            Map.of("Content-Type", contentTypeHeader, "Accept", contentTypeHeader)
        ).withMethod(RestRequest.Method.GET).withPath("/some_index/_knn_search").build();

        dispatchRequest(request);
        assertCriticalWarnings(RestKnnSearchAction.DEPRECATION_MESSAGE);
    }
}
