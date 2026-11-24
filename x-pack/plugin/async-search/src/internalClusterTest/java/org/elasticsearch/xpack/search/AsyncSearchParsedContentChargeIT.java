/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.search;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.breaker.ChildMemoryCircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.LimitedBreaker;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.ParsedContentReservation;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.indices.breaker.CircuitBreakerMetrics;
import org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.TestTelemetryPlugin;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.search.action.AsyncSearchResponse;
import org.elasticsearch.xpack.core.search.action.SubmitAsyncSearchAction;
import org.elasticsearch.xpack.core.search.action.SubmitAsyncSearchRequest;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

/**
 * Tests that an async search holds the charge of its parsed request after the submit responds, until the search ends.
 */
public class AsyncSearchParsedContentChargeIT extends AsyncSearchIntegTestCase {

    private static final String INDEX = "test";

    @Override
    protected boolean addMockHttpTransport() {
        return false; // enable http
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
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

    private enum CompletionState {
        SUCCEEDED,
        FAILED,
        DELETED
    }

    public void testChargeIsHeldUntilTheSearchSucceeds() throws Exception {
        assertChargeIsHeldUntilTheSearchEnds(CompletionState.SUCCEEDED);
    }

    public void testChargeIsHeldUntilTheSearchFails() throws Exception {
        assertChargeIsHeldUntilTheSearchEnds(CompletionState.FAILED);
    }

    public void testChargeIsHeldUntilTheDeletedSearchStops() throws Exception {
        assertChargeIsHeldUntilTheSearchEnds(CompletionState.DELETED);
    }

    private void assertChargeIsHeldUntilTheSearchEnds(CompletionState state) throws Exception {
        createIndex(INDEX, indexSettings(1, 0).build());
        indexRandom(true, prepareIndex(INDEX).setSource("f", "v"));

        String node = internalCluster().getRandomNodeName();
        ThreadContext threadContext = internalCluster().getInstance(ThreadPool.class, node).getThreadContext();
        LimitedBreaker breaker = new LimitedBreaker(CircuitBreaker.REQUEST, ByteSizeValue.ofMb(100));
        ParsedContentReservation reservation = new ParsedContentReservation(breaker, true);
        try (XContentParser parser = reservation.track(createParser(JsonXContent.jsonXContent, "{\"query\":{\"match_all\":{}}}"))) {
            while (parser.nextToken() != null) {
                // Charges each token as the REST layer's parse does.
            }
        }
        long charged = reservation.reservedBytes();
        assertThat(charged, greaterThan(0L));

        // Failing the only shard fails the search.
        int shardFailures = state == CompletionState.FAILED ? 1 : 0;
        try (BlockingQueryBuilder.QueryLatch latch = BlockingQueryBuilder.acquireQueryLatch(shardFailures)) {
            SubmitAsyncSearchRequest request = new SubmitAsyncSearchRequest(
                new SearchSourceBuilder().query(new BlockingQueryBuilder(randomLong())),
                INDEX
            ).setWaitForCompletionTimeout(TimeValue.timeValueMillis(1));
            final AsyncSearchResponse response;
            try (ThreadContext.StoredContext ignored = threadContext.stashContext()) {
                // As the REST layer publishes the reservation of the request it handles.
                threadContext.putTransient(ParsedContentReservation.TRANSIENT_NAME, reservation);
                response = client(node).execute(SubmitAsyncSearchAction.INSTANCE, request).get();
            }
            final String id;
            try {
                assertTrue(response.isRunning());
                id = response.getId();
            } finally {
                response.decRef();
            }
            // As the REST layer closes the reservation once it sends the response.
            reservation.close();
            assertThat(breaker.getUsed(), equalTo(charged));

            if (state == CompletionState.DELETED) {
                // Cancels the search, which stops once its shard is unblocked.
                deleteAsyncSearch(id);
                assertThat(breaker.getUsed(), equalTo(charged));
            }
            latch.close();
            assertBusy(() -> assertThat(breaker.getUsed(), equalTo(0L)));
            ensureTaskCompletion(id);
            if (state != CompletionState.DELETED) {
                deleteAsyncSearch(id);
            }
        }
    }

    /**
     * Submits through the REST layer, so the reservation is the one the REST layer creates and publishes to the submit action.
     */
    public void testChargeOfRestRequestIsHeldUntilTheSearchEnds() throws Exception {
        createIndex(INDEX, indexSettings(1, 0).build());
        indexRandom(true, prepareIndex(INDEX).setSource("f", "v"));

        try (BlockingQueryBuilder.QueryLatch latch = BlockingQueryBuilder.acquireQueryLatch(0)) {
            Map<String, Object> body = submitThroughRest("1ms", "{\"query\":{\"block\":{}}}");
            assertEquals(Boolean.TRUE, body.get("is_running"));
            String id = (String) body.get("id");
            // The response is sent, but the search still runs.
            assertThat(parsedRequestHeldBytes(), greaterThan(0L));

            latch.close();
            assertBusy(() -> assertThat(parsedRequestHeldBytes(), equalTo(0L)));
            ensureTaskCompletion(id);
            deleteAsyncSearch(id);
        }
    }

    public void testChargeOfRestRequestIsReleasedWhenTheSearchEndsBeforeTheResponse() throws Exception {
        createIndex(INDEX, indexSettings(1, 0).build());
        indexRandom(true, prepareIndex(INDEX).setSource("f", "v"));

        int measurementsBefore = parsedRequestMeasurements().size();
        // Long enough for the search to end before the submit responds.
        Map<String, Object> body = submitThroughRest("30s", "{\"query\":{\"match_all\":{}}}");
        assertEquals(Boolean.FALSE, body.get("is_running"));
        assertThat(parsedRequestMeasurements().size(), greaterThan(measurementsBefore));
        // The search can drop its hold just after the response is sent.
        assertBusy(() -> assertThat(parsedRequestHeldBytes(), equalTo(0L)));
    }

    private Map<String, Object> submitThroughRest(String waitForCompletionTimeout, String source) throws IOException {
        Request request = new Request("POST", "/" + INDEX + "/_async_search");
        request.addParameter("wait_for_completion_timeout", waitForCompletionTimeout);
        request.setJsonEntity(source);
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
