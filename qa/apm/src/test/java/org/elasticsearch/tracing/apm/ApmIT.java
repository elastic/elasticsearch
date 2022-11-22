/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.tracing.apm;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.core.CheckedRunnable;
import org.elasticsearch.test.rest.ESRestTestCase;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.not;

/**
 * Tests around Elasticsearch's tracing support using APM.
 */
public class ApmIT extends ESRestTestCase {

    private static final String DATA_STREAM = "traces-apm-default";

    /**
     * Check that if we send HTTP traffic to Elasticsearch, then traces are captured in APM server. The traces are generated in
     * a separate Docker container, which continually fetches `/_nodes/stats`. We check for the following:
     * <ul>
     *     <li>A transaction for the REST API call
     *     <li>A span for the task started by the REST call
     *     <li>A child span started by the above span
     * </ul>
     * <p>This proves that the hierarchy of spans is being correctly captured.
     */
    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/89340")
    public void testCapturesTracesForHttpTraffic() throws Exception {
        checkTracesDataStream();

        assertTracesExist();
    }

    private void checkTracesDataStream() throws Exception {
        assertBusy(() -> {
            final Response response = performRequestTolerantly(new Request("GET", "/_data_stream/" + DATA_STREAM));
            assertOK(response);
        }, 1, TimeUnit.MINUTES);
    }

    private void assertTracesExist() throws Exception {
        // First look for a transaction for the REST calls that we make via the `tracegenerator` Docker container

        final AtomicReference<String> transactionId = new AtomicReference<>();
        assertBusy(() -> {
            final Request tracesSearchRequest = new Request("GET", "/" + DATA_STREAM + "/_search");
            tracesSearchRequest.setJsonEntity("""
                {
                  "query": {
                     "match": { "transaction.name": "GET /_nodes/stats" }
                  }
                }""");
            final Response tracesSearchResponse = performRequestTolerantly(tracesSearchRequest);
            assertOK(tracesSearchResponse);

            final List<Map<String, Object>> documents = getDocuments(tracesSearchResponse);
            assertThat(documents, not(empty()));

            final Map<String, Object> tx = documents.get(0);

            check(tx, "http.request.method", "GET");
            check(tx, "http.response.status_code", 200);
            check(tx, "labels.es_cluster_name", "docker-cluster");
            check(tx, "labels.http_request_headers_authorization", "[REDACTED]");
            check(tx, "span.kind", "SERVER");
            check(tx, "transaction.result", "HTTP 2xx");
            check(tx, "url.path", "/_nodes/stats");

            final String txId = pluck(tx, "transaction.id");
            transactionId.set(txId);
        }, 1, TimeUnit.MINUTES);

        // Then look for the task that the REST call starts

        final AtomicReference<String> monitorNodeStatsSpanId = new AtomicReference<>();
        assertBusy(() -> {
            final List<Map<String, Object>> documents = searchByParentId(transactionId.get());
            assertThat(documents, not(empty()));

            final Map<String, Object> spansByName = documents.stream().collect(Collectors.toMap(d -> pluck(d, "span.name"), d -> d));

            assertThat(spansByName, hasKey("cluster:monitor/nodes/stats"));

            @SuppressWarnings("unchecked")
            final Map<String, Object> span = (Map<String, Object>) spansByName.get("cluster:monitor/nodes/stats");
            check(span, "span.kind", "INTERNAL");

            final String spanId = pluck(span, "span.id");
            monitorNodeStatsSpanId.set(spanId);
        }, 1, TimeUnit.MINUTES);

        // Finally look for the child task that the task above started

        assertBusy(() -> {
            final List<Map<String, Object>> documents = searchByParentId(monitorNodeStatsSpanId.get());
            assertThat(documents, not(empty()));

            final Map<String, Object> spansByName = documents.stream().collect(Collectors.toMap(d -> pluck(d, "span.name"), d -> d));

            assertThat(spansByName, hasKey("cluster:monitor/nodes/stats[n]"));
        }, 1, TimeUnit.MINUTES);
    }

    @SuppressWarnings("unchecked")
    private <T> T pluck(Map<String, Object> map, String path) {
        String[] parts = path.split("\\.");

        Object result = map;

        for (String part : parts) {
            result = ((Map<String, ?>) result).get(part);
        }

        return (T) result;
    }

    private List<Map<String, Object>> searchByParentId(String parentId) throws IOException {
        final Request searchRequest = new Request("GET", "/" + DATA_STREAM + "/_search");
        searchRequest.setJsonEntity("""
            {
              "query": {
                 "match": { "parent.id": "%s" }
              }
            }""".formatted(parentId));
        final Response response = performRequestTolerantly(searchRequest);
        assertOK(response);

        return getDocuments(response);
    }

    /**
     * We don't need to clean up the cluster, particularly as we have Kibana and APM server using ES as well as our test, so declare
     * that we need to preserve the cluster in order to prevent the usual cleanup logic from running (and inevitably failing).
     */
    @Override
    protected boolean preserveClusterUponCompletion() {
        return true;
    }

    /**
     * Turns exceptions into assertion failures so that {@link #assertBusy(CheckedRunnable)} can still retry.
     */
    private Response performRequestTolerantly(Request request) {
        try {
            return client().performRequest(request);
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }

    /**
     * Customizes the client settings to use the same username / password that is configured in Docke.r
     */
    @Override
    protected Settings restClientSettings() {
        String token = basicAuthHeaderValue("admin", new SecureString("changeme".toCharArray()));
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

    /**
     * Constructs the correct cluster address by looking up the dynamic port that Elasticsearch is exposed on.
     */
    @Override
    protected String getTestRestCluster() {
        return "localhost:" + getProperty("test.fixtures.elasticsearch.tcp.9200");
    }

    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> getDocuments(Response response) throws IOException {
        final Map<String, Object> stringObjectMap = ESRestTestCase.entityAsMap(response);
        return (List<Map<String, Object>>) XContentMapValues.extractValue("hits.hits._source", stringObjectMap);
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

    private <T> void check(Map<String, Object> doc, String path, T expected) {
        assertThat(pluck(doc, path), equalTo(expected));
    }
}
