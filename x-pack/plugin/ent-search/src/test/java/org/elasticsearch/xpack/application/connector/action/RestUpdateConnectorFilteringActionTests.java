/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.connector.action;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.FakeRestRequest;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasToString;
import static org.mockito.Mockito.mock;

public class RestUpdateConnectorFilteringActionTests extends ESTestCase {

    private RestUpdateConnectorFilteringAction action;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        action = new RestUpdateConnectorFilteringAction();
    }

    public void testPrepareRequest_emptyPayload_badRequestError() {
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.PUT)
            .withPath("/_connector/123/_filtering")
            .build();

        final ElasticsearchParseException e = expectThrows(
            ElasticsearchParseException.class,
            () -> action.prepareRequest(request, mock(NodeClient.class))
        );
        assertThat(e, hasToString(containsString("request body is required")));
    }
}
