/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.reindex;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.breaker.ChildMemoryCircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.indices.breaker.CircuitBreakerMetrics;
import org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.reindex.ReindexPlugin;
import org.elasticsearch.reindex.RethrottleRequestBuilder;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskInfo;
import org.elasticsearch.tasks.TaskResult;
import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.TestTelemetryPlugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

/**
 * Tests that a reindex, update by query or delete by query started with {@code wait_for_completion=false} holds the charge of its parsed
 * request after the response with its task id, until the task ends.
 */
public class ReindexParsedContentChargeIT extends ESIntegTestCase {

    private static final String SOURCE = "source";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(ReindexPlugin.class, TestTelemetryPlugin.class);
    }

    @Override
    protected boolean addMockHttpTransport() {
        return false; // enable http
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            // The test cluster can make a node's request breaker a noop one, which charges nothing.
            .put(HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_TYPE_SETTING.getKey(), "memory")
            .build();
    }

    private enum TestApi {
        REINDEX,
        UPDATE_BY_QUERY,
        DELETE_BY_QUERY
    }

    public void testChargeIsHeldUntilTheTaskSucceeds() throws Exception {
        assertChargeIsHeldUntilTheTaskEnds(false);
    }

    public void testChargeIsHeldUntilTheTaskIsCancelled() throws Exception {
        assertChargeIsHeldUntilTheTaskEnds(true);
    }

    private void assertChargeIsHeldUntilTheTaskEnds(boolean cancel) throws Exception {
        int slices = between(1, 3);
        TestApi api = randomFrom(TestApi.values());
        logger.info("starting [{}] with [{}] slices", api, slices);
        indexDocs(slices * 10);

        Map<String, Object> body = performAndParse(startTask(api, slices));
        TaskId taskId = new TaskId((String) body.get("task"));
        // The response is sent, but the task still runs.
        assertThat(parsedRequestHeldBytes(), greaterThan(0L));
        // And once it is throttled after its first batch.
        assertBusy(() -> assertTrue(isThrottled(taskId)));
        assertThat(parsedRequestHeldBytes(), greaterThan(0L));

        if (cancel) {
            clusterAdmin().prepareCancelTasks().setTargetTaskId(taskId).get();
        } else {
            if (slices > 1) {
                waitForAllSlicesToStart(taskId, slices);
            }
            removeThrottle(taskId);
        }
        TaskResult result = clusterAdmin().prepareGetTask(taskId).setWaitForCompletion(true).get().getTask();
        assertTrue(result.isCompleted());
        if (cancel == false) {
            assertNull(result.getError());
        }
        assertBusy(() -> assertThat(parsedRequestHeldBytes(), equalTo(0L)));
    }

    public void testChargeIsReleasedWhenTheTaskCannotBeRegistered() throws Exception {
        indexDocs(10);

        Request request = startTask(randomFrom(TestApi.values()), 1);
        // Passes the http limit, but not the task headers limit, which counts two bytes per char.
        request.setOptions(RequestOptions.DEFAULT.toBuilder().addHeader(Task.X_OPAQUE_ID_HTTP_HEADER, randomAlphaOfLength(10_000)));
        int measurementsBefore = parsedRequestMeasurements().size();
        ResponseException e = expectThrows(ResponseException.class, () -> getRestClient().performRequest(request));
        assertThat(e.getResponse().getStatusLine().getStatusCode(), equalTo(400));
        assertThat(e.getMessage(), containsString("Request exceeded the maximum size of task headers"));
        assertThat(parsedRequestMeasurements().size(), greaterThan(measurementsBefore));
        assertBusy(() -> assertThat(parsedRequestHeldBytes(), equalTo(0L)));
    }

    public void testChargeIsReleasedWhenTheTaskFails() throws Exception {
        // The source index is missing, so the task fails.
        int measurementsBefore = parsedRequestMeasurements().size();
        Map<String, Object> body = performAndParse(startTask(randomFrom(TestApi.values()), 1));
        TaskId taskId = new TaskId((String) body.get("task"));
        TaskResult result = clusterAdmin().prepareGetTask(taskId).setWaitForCompletion(true).get().getTask();
        assertTrue(result.isCompleted());
        assertNotNull(result.getError());
        assertThat(parsedRequestMeasurements().size(), greaterThan(measurementsBefore));
        assertBusy(() -> assertThat(parsedRequestHeldBytes(), equalTo(0L)));
    }

    public void testChargeIsReleasedWhenTheRequestIsInvalid() throws Exception {
        Request request = new Request("POST", "/_reindex");
        request.addParameter("wait_for_completion", "false");
        request.setJsonEntity("{\"source\":{\"index\":\"" + SOURCE + "\",\"_source\":false},\"dest\":{\"index\":\"dest\"}}");
        int measurementsBefore = parsedRequestMeasurements().size();
        ResponseException e = expectThrows(ResponseException.class, () -> getRestClient().performRequest(request));
        assertThat(e.getResponse().getStatusLine().getStatusCode(), equalTo(400));
        assertThat(e.getMessage(), containsString("_source:false is not supported in this context"));
        assertThat(parsedRequestMeasurements().size(), greaterThan(measurementsBefore));
        assertBusy(() -> assertThat(parsedRequestHeldBytes(), equalTo(0L)));
    }

    private void indexDocs(int count) {
        createIndex(SOURCE);
        List<IndexRequestBuilder> docs = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            docs.add(prepareIndex(SOURCE).setId(Integer.toString(i)).setSource("foo", "bar"));
        }
        indexRandom(true, docs);
    }

    /**
     * A request that starts a task that is throttled after its first batch until {@link #removeThrottle}, so it runs until the test ends
     * it.
     */
    private static Request startTask(TestApi api, int slices) {
        Request request = switch (api) {
            case REINDEX -> {
                Request r = new Request("POST", "/_reindex");
                r.setJsonEntity("{\"source\":{\"index\":\"" + SOURCE + "\",\"size\":1},\"dest\":{\"index\":\"dest\"}}");
                yield r;
            }
            case UPDATE_BY_QUERY -> {
                Request r = new Request("POST", "/" + SOURCE + "/_update_by_query");
                r.addParameter("scroll_size", "1");
                r.setJsonEntity("{\"query\":{\"match_all\":{}}}");
                yield r;
            }
            case DELETE_BY_QUERY -> {
                Request r = new Request("POST", "/" + SOURCE + "/_delete_by_query");
                r.addParameter("scroll_size", "1");
                r.setJsonEntity("{\"query\":{\"match_all\":{}}}");
                yield r;
            }
        };
        request.addParameter("wait_for_completion", "false");
        request.addParameter("requests_per_second", "0.000001");
        request.addParameter("slices", Integer.toString(slices));
        return request;
    }

    /**
     * Whether the task, or one of its slices, waits for the throttle before its next batch. A leader's own status does not show its
     * running slices.
     */
    private boolean isThrottled(TaskId taskId) {
        ListTasksResponse tasks = clusterAdmin().prepareListTasks().setDetailed(true).get();
        tasks.rethrowFailures("tasks");
        return tasks.getTasks()
            .stream()
            .filter(t -> t.taskId().equals(taskId) || t.parentTaskId().equals(taskId))
            .map(TaskInfo::status)
            .filter(BulkByPaginatedSearchTask.Status.class::isInstance)
            .map(s -> ((BulkByPaginatedSearchTask.Status) s).getThrottledUntil())
            .anyMatch(until -> until != null && until.nanos() > 0);
    }

    /** A leader only rethrottles the slices that are running, so a slice that starts later would keep the throttle. */
    private void waitForAllSlicesToStart(TaskId taskId, int slices) throws Exception {
        assertBusy(() -> {
            BulkByPaginatedSearchTask.Status status = (BulkByPaginatedSearchTask.Status) clusterAdmin().prepareGetTask(taskId)
                .get()
                .getTask()
                .getTask()
                .status();
            long finished = status.getSliceStatuses().stream().filter(Objects::nonNull).count();
            ListTasksResponse running = clusterAdmin().prepareListTasks().setTargetParentTaskId(taskId).get();
            running.rethrowFailures("subtasks");
            assertThat(finished + running.getTasks().size(), greaterThanOrEqualTo((long) slices));
        });
    }

    private void removeThrottle(TaskId taskId) throws Exception {
        // A sliced task cannot be rethrottled until it knows its slices.
        assertBusy(() -> {
            try {
                new RethrottleRequestBuilder(client()).setTargetTaskId(taskId)
                    .setRequestsPerSecond(Float.POSITIVE_INFINITY)
                    .get()
                    .rethrowFailures("Rethrottle");
            } catch (ElasticsearchException e) {
                Throwable unwrapped = ExceptionsHelper.unwrap(e, IllegalArgumentException.class);
                if (unwrapped == null || unwrapped.getMessage().contains("has not yet been initialized") == false) {
                    throw e;
                }
                throw new AssertionError("task [" + taskId + "] is not ready to be rethrottled", e);
            }
        });
    }

    private static Map<String, Object> performAndParse(Request request) throws IOException {
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
