/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.rest.action;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.xcontent.NamedXContentRegistry;

import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;

public class RestStartTransformActionTests extends ESTestCase {

    private static final String ID = "id";
    private static final String START_AFTER = "start_after";

    public void testStartAfterValid() throws Exception {
        testStartAfterValid(null);
        testStartAfterValid("12345678");
        testStartAfterValid("2022-10-25");
        testStartAfterValid("now-1d");
    }

    private void testStartAfterValid(String startAfter) {
        RestStartTransformAction handler = new RestStartTransformAction();
        FakeRestRequest.Builder requestBuilder = new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY);
        if (startAfter == null) {
            requestBuilder.withParams(Map.of(ID, "my-id"));
        } else {
            requestBuilder.withParams(Map.of(ID, "my-id", START_AFTER, startAfter));
        }
        FakeRestRequest request = requestBuilder.build();
        handler.prepareRequest(request, mock(NodeClient.class));
    }

    public void testStartAfterInvalid() {
        testStartAfterInvalid("");
        testStartAfterInvalid("not-a-valid-timestamp");
        testStartAfterInvalid("2023-17-42");
    }

    private void testStartAfterInvalid(String startAfter) {
        final RestStartTransformAction handler = new RestStartTransformAction();
        final FakeRestRequest request = new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY).withParams(
            Map.of(ID, "my-id", START_AFTER, startAfter)
        ).build();
        ElasticsearchParseException e = expectThrows(
            ElasticsearchParseException.class,
            () -> handler.prepareRequest(request, mock(NodeClient.class))
        );
        assertThat(e.getMessage(), equalTo("Failed to parse date for [start_after]"));
    }
}
