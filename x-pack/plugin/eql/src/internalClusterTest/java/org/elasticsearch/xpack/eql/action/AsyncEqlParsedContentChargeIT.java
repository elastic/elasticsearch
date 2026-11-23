/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.eql.action;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.telemetry.TestTelemetryPlugin;
import org.elasticsearch.transport.netty4.Netty4Plugin;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.async.DeleteAsyncResultRequest;
import org.elasticsearch.xpack.core.async.GetAsyncResultRequest;
import org.elasticsearch.xpack.core.async.TransportDeleteAsyncResultAction;
import org.elasticsearch.xpack.eql.plugin.EqlAsyncGetResultAction;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.BreakerTestUtil.parsedRequestHeldBytes;
import static org.elasticsearch.test.BreakerTestUtil.parsedRequestMeasurements;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

/**
 * Tests that an async EQL search holds the charge of its parsed request after the submit responds, until the query ends.
 */
public class AsyncEqlParsedContentChargeIT extends AbstractEqlBlockingIntegTestCase {

    @Override
    protected boolean addMockHttpTransport() {
        return false; // enable http
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(NetworkModule.HTTP_TYPE_KEY, Netty4Plugin.NETTY_HTTP_TRANSPORT_NAME)
            // The test cluster can make a node's request breaker a noop one, which charges nothing.
            .put(HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_TYPE_SETTING.getKey(), "memory")
            .build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(getTestTransportPlugin());
        plugins.add(TestTelemetryPlugin.class);
        return plugins;
    }

    public void testChargeIsHeldUntilTheQuerySucceeds() throws Exception {
        assertChargeIsHeldUntilTheQueryEnds(false);
    }

    public void testChargeIsHeldUntilTheDeletedQueryStops() throws Exception {
        assertChargeIsHeldUntilTheQueryEnds(true);
    }

    private void assertChargeIsHeldUntilTheQueryEnds(boolean delete) throws Exception {
        createTestIndex();
        // The query waits until field caps are unblocked.
        List<SearchBlockPlugin> plugins = initBlockFactory(false, true);
        try {
            Map<String, Object> body = submitThroughRest("1ms");
            assertEquals(Boolean.TRUE, body.get("is_running"));
            String id = (String) body.get("id");
            awaitForBlockedFieldCaps(plugins);
            // The response is sent, but the query still runs.
            assertThat(parsedRequestHeldBytes(internalCluster()), greaterThan(0L));

            if (delete) {
                // Cancels the query, which stops once field caps are unblocked. The delete responds once it stops.
                ActionFuture<AcknowledgedResponse> deleted = client().execute(
                    TransportDeleteAsyncResultAction.TYPE,
                    new DeleteAsyncResultRequest(id)
                );
                assertThat(parsedRequestHeldBytes(internalCluster()), greaterThan(0L));
                disableBlocks(plugins);
                assertTrue(deleted.actionGet(TimeValue.timeValueSeconds(30)).isAcknowledged());
            } else {
                disableBlocks(plugins);
                GetAsyncResultRequest get = new GetAsyncResultRequest(id).setWaitForCompletionTimeout(TimeValue.timeValueSeconds(30));
                EqlSearchResponse response = client().execute(EqlAsyncGetResultAction.INSTANCE, get).get();
                try {
                    assertFalse(response.isRunning());
                } finally {
                    response.decRef();
                }
            }
            assertBusy(() -> assertThat(parsedRequestHeldBytes(internalCluster()), equalTo(0L)));
            if (delete == false) {
                assertTrue(
                    client().execute(TransportDeleteAsyncResultAction.TYPE, new DeleteAsyncResultRequest(id)).get().isAcknowledged()
                );
            }
        } finally {
            disableBlocks(plugins);
        }
    }

    public void testChargeIsReleasedWhenTheQueryEndsBeforeTheResponse() throws Exception {
        createTestIndex();
        int measurementsBefore = parsedRequestMeasurements(internalCluster()).size();
        // Long enough for the query to end before the submit responds.
        Map<String, Object> body = submitThroughRest("30s");
        assertEquals(Boolean.FALSE, body.get("is_running"));
        assertThat(parsedRequestMeasurements(internalCluster()).size(), greaterThan(measurementsBefore));
        // The query can drop its hold just after the response is sent.
        assertBusy(() -> assertThat(parsedRequestHeldBytes(internalCluster()), equalTo(0L)));
        assertTrue(
            client().execute(TransportDeleteAsyncResultAction.TYPE, new DeleteAsyncResultRequest((String) body.get("id")))
                .get()
                .isAcknowledged()
        );
    }

    private void createTestIndex() {
        assertAcked(indicesAdmin().prepareCreate("test").setMapping("event_type", "type=keyword", "@timestamp", "type=date"));
        indexRandom(true, prepareIndex("test").setSource("event_type", "my_event", "@timestamp", "2020-04-09T12:35:48Z"));
    }

    private Map<String, Object> submitThroughRest(String waitForCompletionTimeout) throws IOException {
        Request request = new Request("POST", "/test/_eql/search");
        request.setJsonEntity(
            "{\"query\":\"my_event where true\",\"event_category_field\":\"event_type\",\"wait_for_completion_timeout\":\""
                + waitForCompletionTimeout
                + "\",\"keep_on_completion\":true}"
        );
        Response response = getRestClient().performRequest(request);
        try (InputStream content = response.getEntity().getContent()) {
            return XContentHelper.convertToMap(JsonXContent.jsonXContent, content, false);
        }
    }
}
