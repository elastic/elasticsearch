/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.reindex;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.index.reindex.AbstractBulkByScrollRequest;
import org.elasticsearch.index.reindex.ReindexRequest;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.test.rest.RestActionTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.junit.Before;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static java.util.Collections.singletonMap;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertToXContentEquivalent;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.notNullValue;

public class RestReindexActionTests extends RestActionTestCase {

    private RestReindexAction action;

    @Before
    public void setUpAction() {
        action = new RestReindexAction(nf -> false);
        controller().registerHandler(action);
    }

    public void testPipelineQueryParameterIsError() throws IOException {
        FakeRestRequest.Builder request = new FakeRestRequest.Builder(xContentRegistry());
        try (XContentBuilder body = JsonXContent.contentBuilder().prettyPrint()) {
            body.startObject();
            {
                body.startObject("source");
                {
                    body.field("index", "source");
                }
                body.endObject();
                body.startObject("dest");
                {
                    body.field("index", "dest");
                }
                body.endObject();
            }
            body.endObject();
            request.withContent(BytesReference.bytes(body), body.contentType());
        }
        request.withParams(singletonMap("pipeline", "doesn't matter"));
        Exception e = expectThrows(IllegalArgumentException.class, () -> action.buildRequest(request.build()));

        assertEquals("_reindex doesn't support [pipeline] as a query parameter. Specify it in the [dest] object instead.", e.getMessage());
    }

    public void testSetScrollTimeout() throws IOException {
        {
            FakeRestRequest.Builder requestBuilder = new FakeRestRequest.Builder(xContentRegistry());
            requestBuilder.withContent(new BytesArray("{}"), XContentType.JSON);
            ReindexRequest request = action.buildRequest(requestBuilder.build());
            assertEquals(AbstractBulkByScrollRequest.DEFAULT_SCROLL_TIMEOUT, request.getScrollTime());
        }
        {
            FakeRestRequest.Builder requestBuilder = new FakeRestRequest.Builder(xContentRegistry());
            requestBuilder.withParams(singletonMap("scroll", "10m"));
            requestBuilder.withContent(new BytesArray("{}"), XContentType.JSON);
            ReindexRequest request = action.buildRequest(requestBuilder.build());
            assertEquals("10m", request.getScrollTime().toString());
        }
    }

    public void testFilterSource() throws IOException {
        final FakeRestRequest.Builder requestBuilder = new FakeRestRequest.Builder(xContentRegistry());
        final var body = """
            {
              "source" : {
                "index": "photos",
                "remote" : {
                  "host": "https://bugle.example.net:2400/",
                  "username": "peter.parker",
                  "password": "mj4ever!",
                  "headers": {
                    "X-Hero-Name": "spiderman"
                  }
                }
              },
              "dest": {
                "index": "webshots"
              }
            }
            """;
        requestBuilder.withContent(new BytesArray(body), XContentType.JSON);

        final FakeRestRequest restRequest = requestBuilder.build();
        ReindexRequest request = action.buildRequest(restRequest);

        // Check that the request parsed correctly
        assertThat(request.getRemoteInfo().getScheme(), equalTo("https"));
        assertThat(request.getRemoteInfo().getHost(), equalTo("bugle.example.net"));
        assertThat(request.getRemoteInfo().getPort(), equalTo(2400));
        assertThat(request.getRemoteInfo().getUsername(), equalTo("peter.parker"));
        assertThat(request.getRemoteInfo().getPassword(), equalTo("mj4ever!"));
        assertThat(request.getRemoteInfo().getHeaders(), hasEntry("X-Hero-Name", "spiderman"));
        assertThat(request.getRemoteInfo().getHeaders(), aMapWithSize(1));

        final RestRequest filtered = action.getFilteredRequest(restRequest);
        assertToXContentEquivalent(new BytesArray("""
            {
              "source" : {
                "index": "photos",
                "remote" : {
                  "host": "https://bugle.example.net:2400/",
                  "username": "peter.parker",
                  "password": "::es-redacted::",
                  "headers": {
                    "X-Hero-Name": "::es-redacted::"
                  }
                }
              },
              "dest": {
                "index": "webshots"
              }
            }
            """), filtered.content(), XContentType.JSON);
    }

    public void testUnfilteredSource() throws IOException {
        final FakeRestRequest.Builder requestBuilder = new FakeRestRequest.Builder(xContentRegistry());
        final var empty1 = "";
        final var empty2 = "{}";
        final var nonRemote = """
                {
                  "source" : { "index": "your-index" },
                  "dest" : { "index": "my-index" }
                }
            """;
        final var noCredentials = """
            {
              "source" : {
                "index": "remote-index",
                "remote" : {
                  "host": "https://es.example.net:12345/",
                  "headers": {}
                }
              },
              "dest": {
                "index": "my-index"
              }
            }
            """;
        for (String body : List.of(empty1, empty2, nonRemote, noCredentials)) {
            final BytesArray bodyAsBytes = new BytesArray(body);
            requestBuilder.withContent(bodyAsBytes, XContentType.JSON);
            final FakeRestRequest restRequest = requestBuilder.build();
            final RestRequest filtered = action.getFilteredRequest(restRequest);
            assertToXContentEquivalent(bodyAsBytes, filtered.content(), XContentType.JSON);
        }
    }

    public void testFilteringBadlyStructureSourceIsSafe() throws IOException {
        final FakeRestRequest.Builder requestBuilder = new FakeRestRequest.Builder(xContentRegistry());
        final var remoteAsString = """
            {
              "source" : {
                "index": "remote-index",
                "remote" : "https://es.example.net:12345/"
              },
              "dest": {
                "index": "my-index"
              }
            }
            """;
        final var passwordAsNumber = """
            {
              "source" : {
                "index": "remote-index",
                "remote" : {
                  "host": "https://es.example.net:12345/",
                  "username": "skroob",
                  "password": 12345
                }
              },
              "dest": {
                "index": "my-index"
              }
            }
            """;
        final var headersAsList = """
            {
              "source" : {
                "index": "remote-index",
                "remote" : {
                  "host": "https://es.example.net:12345/",
                  "headers": [ "bogus" ]
                }
              },
              "dest": {
                "index": "my-index"
              }
            }
            """;
        for (String body : List.of(remoteAsString, passwordAsNumber, headersAsList)) {
            final BytesArray bodyAsBytes = new BytesArray(body);
            requestBuilder.withContent(bodyAsBytes, XContentType.JSON);
            final FakeRestRequest restRequest = requestBuilder.build();

            final RestRequest filtered = action.getFilteredRequest(restRequest);
            assertThat(filtered, notNullValue());

            // We will redacted some parts of these bodies, so just check that they end up as valid JSON with the right top level fields
            final Map<String, Object> filteredMap = XContentHelper.convertToMap(filtered.content(), false, XContentType.JSON).v2();
            assertThat(filteredMap, notNullValue());
            assertThat(filteredMap, hasKey("source"));
            assertThat(filteredMap, hasKey("dest"));
        }
    }
}
