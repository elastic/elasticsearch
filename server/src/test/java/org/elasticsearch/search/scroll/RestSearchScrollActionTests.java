/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.scroll;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.search.RestSearchScrollAction;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpNodeClient;
import org.elasticsearch.test.rest.FakeRestChannel;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.xcontent.XContentType;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class RestSearchScrollActionTests extends ESTestCase {

    public void testParseSearchScrollRequestWithInvalidJsonThrowsException() throws Exception {
        RestSearchScrollAction action = new RestSearchScrollAction();
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withContent(
            new BytesArray("{invalid_json}"),
            XContentType.JSON
        ).build();
        Exception e = expectThrows(IllegalArgumentException.class, () -> action.prepareRequest(request, null));
        assertThat(e.getMessage(), equalTo("Failed to parse request body"));
    }

    public void testBodyParamsOverrideQueryStringParams() throws Exception {
        SetOnce<Boolean> scrollCalled = new SetOnce<>();
        try (var threadPool = createThreadPool()) {
            final var nodeClient = new NoOpNodeClient(threadPool) {
                @Override
                public void searchScroll(SearchScrollRequest request, ActionListener<SearchResponse> listener) {
                    scrollCalled.set(true);
                    assertThat(request.scrollId(), equalTo("BODY"));
                    assertThat(request.scroll().keepAlive().getStringRep(), equalTo("1m"));
                }
            };
            RestSearchScrollAction action = new RestSearchScrollAction();
            Map<String, String> params = new HashMap<>();
            params.put("scroll_id", "QUERY_STRING");
            params.put("scroll", "1000m");
            RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withParams(params)
                .withContent(new BytesArray("{\"scroll_id\":\"BODY\", \"scroll\":\"1m\"}"), XContentType.JSON)
                .build();
            FakeRestChannel channel = new FakeRestChannel(request, randomBoolean(), 0);
            action.handleRequest(request, channel, nodeClient);

            assertThat(scrollCalled.get(), equalTo(true));
        }
    }
}
