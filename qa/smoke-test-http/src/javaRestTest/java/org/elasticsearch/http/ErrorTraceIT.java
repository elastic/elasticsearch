/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.http;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.client.Request;
import org.elasticsearch.test.rest.ObjectPath;
import org.elasticsearch.transport.TransportMessageListener;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.Optional;
import java.util.function.Consumer;

import static org.elasticsearch.index.query.QueryBuilders.simpleQueryStringQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertFailures;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;

public class ErrorTraceIT extends HttpSmokeTestCase {

    private void createIndexWithDivergentFieldType(Consumer<SearchResponse> consumer) {
        createIndex("test1", "test2");
        indexRandom(
            true,
            prepareIndex("test1").setId("1").setSource("field", "foo"),
            prepareIndex("test2").setId("10").setSource("field", 5)
        );
        refresh();

        assertResponse(prepareSearch().setQuery(simpleQueryStringQuery("foo").field("field")), consumer);
    }

    public void testStandardFailingQueryErrorTraceDefault() throws Exception {
        internalCluster().getDataNodeInstances(TransportService.class).forEach(ts -> {
            ts.addMessageListener(new TransportMessageListener() {
                @Override
                public void onResponseSent(long requestId, String action, Exception error) {
                    TransportMessageListener.super.onResponseSent(requestId, action, error);
                    if (action.startsWith("indices:data/read/search")) {
                        Optional<Throwable> throwable = ExceptionsHelper.unwrapCausesAndSuppressed(
                            error,
                            t -> t.getStackTrace().length > 0
                        );
                        assertTrue(throwable.isEmpty());
                    }
                }
            });
        });
        createIndexWithDivergentFieldType(response -> {
            assertFailures(response);
            for (ShardSearchFailure failedShard : response.getShardFailures()) {
                assertThat(failedShard.getStackTrace().length, is(not(equalTo(0))));
            }
        });
    }

    public void testBase() throws IOException {

    }

    public void testWithRestRequest() throws IOException {
        internalCluster().getDataNodeInstances(TransportService.class).forEach(ts -> {
            ts.addMessageListener(new TransportMessageListener() {
                @Override
                public void onResponseSent(long requestId, String action, Exception error) {
                    TransportMessageListener.super.onResponseSent(requestId, action, error);
                    if (action.startsWith("indices:data/read/search")) {
                        Optional<Throwable> throwable = ExceptionsHelper.unwrapCausesAndSuppressed(
                            error,
                            t -> t.getStackTrace().length > 0
                        );
                        assertTrue(throwable.isEmpty());
                    }
                }
            });
        });
        createIndex("test1", "test2");
        indexRandom(
            true,
            prepareIndex("test1").setId("1").setSource("field", "foo"),
            prepareIndex("test2").setId("10").setSource("field", 5)
        );
        refresh();

        Request searchRequest = new Request("POST", "/_search");
        searchRequest.setJsonEntity("""
            {
                "query": {
                    "simple_query_string" : {
                        "query": "foo",
                        "fields": ["field"]
                    }
                }
            }
            """);
        searchRequest.addParameter("error_trace", "true");
        ObjectPath response = ObjectPath.createFromResponse(getRestClient().performRequest(searchRequest));
    }
}
