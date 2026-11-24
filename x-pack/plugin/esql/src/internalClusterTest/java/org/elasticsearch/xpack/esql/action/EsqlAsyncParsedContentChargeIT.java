/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.breaker.ChildMemoryCircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.indices.breaker.CircuitBreakerMetrics;
import org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.TestTelemetryPlugin;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.async.DeleteAsyncResultRequest;
import org.elasticsearch.xpack.core.async.TransportDeleteAsyncResultAction;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.action.EsqlAsyncTestUtils.deleteAsyncId;
import static org.elasticsearch.xpack.esql.action.EsqlAsyncTestUtils.getAsyncResponse;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

/**
 * Tests that an async query holds the charge of its parsed request after the submit responds, until the query ends.
 */
public class EsqlAsyncParsedContentChargeIT extends AbstractPausableIntegTestCase {

    @Override
    protected boolean addMockHttpTransport() {
        return false; // enable http
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(EsqlAsyncActionIT.LocalStateEsqlAsync.class);
        plugins.add(TestTelemetryPlugin.class);
        return plugins;
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            // The test cluster can make a node's request breaker a noop one, which charges nothing.
            .put(HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_TYPE_SETTING.getKey(), "memory")
            .build();
    }

    public void testChargeIsHeldUntilTheQuerySucceeds() throws Exception {
        assertChargeIsHeldUntilTheQueryEnds(false);
    }

    public void testChargeIsHeldUntilTheDeletedQueryStops() throws Exception {
        assertChargeIsHeldUntilTheQueryEnds(true);
    }

    private void assertChargeIsHeldUntilTheQueryEnds(boolean delete) throws Exception {
        scriptPermits.drainPermits();
        try {
            Map<String, Object> body = submitThroughRest("1ms", "FROM test | STATS SUM(pause_me)");
            assertEquals(Boolean.TRUE, body.get("is_running"));
            String id = (String) body.get("id");
            // The response is sent, but the query waits in pause_me.
            assertThat(parsedRequestHeldBytes(), greaterThan(0L));

            if (delete) {
                // Cancels the query, which stops once pause_me lets it go on. The delete responds once it stops.
                ActionFuture<AcknowledgedResponse> deleted = client().execute(
                    TransportDeleteAsyncResultAction.TYPE,
                    new DeleteAsyncResultRequest(id)
                );
                assertThat(parsedRequestHeldBytes(), greaterThan(0L));
                scriptPermits.release(numberOfDocs());
                assertTrue(deleted.actionGet(TimeValue.timeValueSeconds(60)).isAcknowledged());
            } else {
                scriptPermits.release(numberOfDocs());
                assertBusy(() -> {
                    try (EsqlQueryResponse response = getAsyncResponse(client(), id)) {
                        assertFalse(response.isRunning());
                    }
                });
            }
            assertBusy(() -> assertThat(parsedRequestHeldBytes(), equalTo(0L)));
            if (delete == false) {
                assertTrue(deleteAsyncId(client(), id).isAcknowledged());
            }
        } finally {
            scriptPermits.drainPermits();
        }
    }

    public void testChargeIsReleasedWhenTheQueryEndsBeforeTheResponse() throws Exception {
        int measurementsBefore = parsedRequestMeasurements().size();
        // Long enough for the query to end before the submit responds.
        Map<String, Object> body = submitThroughRest("30s", "FROM test | STATS COUNT(*)");
        assertEquals(Boolean.FALSE, body.get("is_running"));
        assertThat(parsedRequestMeasurements().size(), greaterThan(measurementsBefore));
        // The query can drop its hold just after the response is sent.
        assertBusy(() -> assertThat(parsedRequestHeldBytes(), equalTo(0L)));
    }

    private Map<String, Object> submitThroughRest(String waitForCompletionTimeout, String query) throws IOException {
        Request request = new Request("POST", "/_query/async");
        request.setJsonEntity(
            "{\"query\":\"" + query + "\",\"wait_for_completion_timeout\":\"" + waitForCompletionTimeout + "\",\"keep_on_completion\":true}"
        );
        Response response = getRestClient().performRequest(request);
        try (InputStream content = response.getEntity().getContent()) {
            return XContentHelper.convertToMap(JsonXContent.jsonXContent, content, false);
        }
    }

    /** The bytes the request breakers of all nodes hold for parsed requests, from the held memory metric. */
    private long parsedRequestHeldBytes() {
        return parsedRequestMeasurements().stream().mapToLong(Measurement::getLong).sum();
    }

    private List<Measurement> parsedRequestMeasurements() {
        List<Measurement> measurements = new ArrayList<>();
        for (String node : internalCluster().getNodeNames()) {
            TestTelemetryPlugin telemetry = internalCluster().getInstance(PluginsService.class, node)
                .filterPlugins(TestTelemetryPlugin.class)
                .findFirst()
                .orElseThrow();
            for (Measurement measurement : telemetry.getLongUpDownCounterMeasurement(CircuitBreakerMetrics.ES_BREAKER_MEMORY_HELD)) {
                Map<String, Object> attributes = measurement.attributes();
                if (CircuitBreaker.REQUEST.equals(attributes.get(ChildMemoryCircuitBreaker.BREAKER_METRIC_TYPE_ATTRIBUTE))
                    && ChildMemoryCircuitBreaker.CATEGORY_PARSED_REQUEST.equals(
                        attributes.get(ChildMemoryCircuitBreaker.CIRCUIT_BREAKER_CATEGORY_ATTRIBUTE)
                    )) {
                    measurements.add(measurement);
                }
            }
        }
        return measurements;
    }
}
