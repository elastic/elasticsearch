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
import org.elasticsearch.core.CheckedRunnable;
import org.elasticsearch.test.rest.ESRestTestCase;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.not;

/**
 * Tests around Elasticsearch's tracing support using APM.
 */
public class ApmIT extends ESRestTestCase {

    /**
     * Check that if we send HTTP traffic to Elasticsearch, then traces are captured in APM server. The traces are generated in
     * a separate Docker container, which continually fetches `/_nodes/stats`.
     */
    public void testCapturesTracesForHttpTraffic() throws Exception {
        checkTracesDataStream();

        assertTracesExist();
    }

    private void checkTracesDataStream() throws Exception {
        assertBusy(() -> {
            final Response response = performRequestTolerantly(new Request("GET", "/_data_stream/traces-apm-default"));
            assertOK(response);
        }, 1, TimeUnit.MINUTES);
    }

    private void assertTracesExist() throws Exception {
        assertBusy(() -> {
            final Request tracesSearchRequest = new Request("GET", "/traces-apm-default/_search");
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
        }, 2, TimeUnit.MINUTES);
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
}
