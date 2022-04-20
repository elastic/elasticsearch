/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.xpack.apm;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.test.rest.ESRestTestCase;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.hasSize;

public class ApmIT extends ESRestTestCase {

    public void testName() throws Exception {
        final Request nodesRequest = new Request("GET", "/_nodes/stats");
        final Response nodesResponse = client().performRequest(nodesRequest);
        assertOK(nodesResponse);

        assertBusy(() -> {
            final Request tracesSearchRequest = new Request("GET", "/traces-apm-default/_search");
            tracesSearchRequest.setJsonEntity("""
            {
              "query": {
                "match": {
                  "transaction.name": "GET /_nodes/stats"
                }
              }
            }""");
            final Response tracesSearchResponse = performRequestTolerantly(tracesSearchRequest);
            assertOK(tracesSearchResponse);

            final List<Map<String, Object>> documents = getDocuments(nodesResponse);
            assertThat(documents, hasSize(1));
        });
    }

    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> getDocuments(Response response) throws IOException {
        final Map<String, Object> stringObjectMap = ESRestTestCase.entityAsMap(response);
        return (List<Map<String, Object>>) XContentMapValues.extractValue(
            "hits.hits._source",
            stringObjectMap
        );
    }

    private Response performRequestTolerantly(Request request) {
        try {
            return client().performRequest(request);
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }

    private String getProperty(String key) {
        String value = System.getProperty(key);
        if (value == null) {
            throw new IllegalStateException(
                "Could not find system properties from test.fixtures. "
                    + "This test expects to run with the elasticsearch.test.fixtures Gradle plugin"
            );
        }
        return value;
    }

    @Override
    protected Settings restClientSettings() {
        String token = basicAuthHeaderValue("admin", new SecureString("changeme".toCharArray()));
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

    @Override
    protected String getTestRestCluster() {
        return "localhost:" + getProperty("test.fixtures.elasticsearch.tcp.9200");
    }
}
