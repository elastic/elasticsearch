/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.rest.action;

import org.elasticsearch.exception.ElasticsearchParseException;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.xcontent.NamedXContentRegistry;

import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;

public class RestStartTransformActionTests extends ESTestCase {

    private static final String ID = "id";
    private static final String FROM = "from";

    public void testFromValid() throws Exception {
        testFromValid(null);
        testFromValid("12345678");
        testFromValid("2022-10-25");
        testFromValid("now-1d");
    }

    private void testFromValid(String from) {
        RestStartTransformAction handler = new RestStartTransformAction();
        FakeRestRequest.Builder requestBuilder = new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY);
        if (from == null) {
            requestBuilder.withParams(Map.of(ID, "my-id"));
        } else {
            requestBuilder.withParams(Map.of(ID, "my-id", FROM, from));
        }
        FakeRestRequest request = requestBuilder.build();
        handler.prepareRequest(request, mock(NodeClient.class));
    }

    public void testFromInvalid() {
        testFromInvalid("");
        testFromInvalid("not-a-valid-timestamp");
        testFromInvalid("2023-17-42");
    }

    private void testFromInvalid(String from) {
        final RestStartTransformAction handler = new RestStartTransformAction();
        final FakeRestRequest request = new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY).withParams(Map.of(ID, "my-id", FROM, from))
            .build();
        ElasticsearchParseException e = expectThrows(
            ElasticsearchParseException.class,
            () -> handler.prepareRequest(request, mock(NodeClient.class))
        );
        assertThat(e.getMessage(), equalTo("Failed to parse date for [from]"));
    }
}
