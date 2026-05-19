/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security;

import org.apache.http.HttpHost;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.Constants;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.cluster.util.resource.Resource;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.test.rest.ObjectPath;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class ReindexRelocationWithSecurityIT extends ESRestTestCase {

    private static final Logger logger = LogManager.getLogger(ReindexRelocationWithSecurityIT.class);

    private static final String ADMIN_USER = "test_admin";
    private static final String REINDEX_USER = "test_reindex_user";
    private static final String RETHROTTLE_USER = "test_rethrottle_user";
    private static final String LIST_USER = "test_list_user";
    private static final String GET_USER = "test_get_user";
    private static final String PASS = "x-pack-test-password";

    private static final int MASTER_NODE = 0;
    private static final int DATA_NODE = 1;
    private static final int COORD_NODE = 2;

    private static final String SOURCE = "source";
    private static final String DEST = "dest";

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        // master-only node
        .withNode(node -> node.name("master-node").setting("node.roles", "[master]"))
        // data-only node — also where the reindex relocates to and where .tasks ends up
        .withNode(node -> node.name("data-node").setting("node.roles", "[data,ingest]"))
        // coordinating-only node — where the reindex initially runs and which we will SIGTERM
        .withNode(
            node -> node.name("coord-node")
                .setting("node.roles", "[]")
                // Bound the reindex relocation shutdown hook so prepareForShutdown() doesn't block indefinitely waiting on the throttled
                // reindex; the external rethrottle below is what actually lets the task advance to the next batch boundary in time.
                .setting("node.maximum_reindexing_grace_period", "30s")
        )
        .setting("xpack.security.enabled", "true")
        .setting("xpack.ml.enabled", "false")
        .setting("xpack.watcher.enabled", "false")
        .setting("xpack.license.self_generated.type", "trial")
        .setting("xpack.security.autoconfiguration.enabled", "false")
        .rolesFile(Resource.fromClasspath("roles.yml"))
        .user(ADMIN_USER, PASS, "superuser", false)
        .user(REINDEX_USER, PASS, "minimal", false)
        .user(RETHROTTLE_USER, PASS, "reindex_rethrottle_only", false)
        .user(LIST_USER, PASS, "reindex_list_only", false)
        .user(GET_USER, PASS, "reindex_get_only", false)
        .build();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @Override
    protected Settings restAdminSettings() {
        return restClientSettingsForUser(ADMIN_USER);
    }

    @Override
    protected Settings restClientSettings() {
        return restClientSettingsForUser(REINDEX_USER);
    }

    private static Settings restClientSettingsForUser(String user) {
        String token = basicAuthHeaderValue(user, new SecureString(PASS.toCharArray()));
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

    public void testReindexRelocatesWhenCoordinatorShutsDown() throws Exception {
        // Skip if reindex resilience features aren't enabled in this build (e.g. release distro without the feature flag);
        // the dedicated GET /_reindex/{task_id} endpoint is registered iff REINDEX_RESILIENCE_ENABLED is true.
        assumeTrue(
            "reindex resilience endpoints must be available",
            clusterHasCapability("GET", "/_reindex/{task_id}", List.of(), List.of("reindex_management_api")).orElse(false)
        );
        // The test cluster framework's Process.destroy() maps to TerminateProcess on Windows, which kills the JVM without running
        // shutdown hooks. As a result ShutdownPrepareService.prepareForShutdown() never fires, the reindex-stop hook never marks the
        // task for relocation, and the assertions below cannot be satisfied.
        assumeFalse("graceful JVM shutdown is not deliverable on Windows by the test cluster framework", Constants.WINDOWS);

        final int numDocs = 30;
        createSourceIndexAndPopulate(numDocs);

        final String coordNodeName = cluster.getName(COORD_NODE);
        final String coordNodeId = lookupNodeId(coordNodeName);
        final String coordHttpAddress = cluster.getHttpAddress(COORD_NODE);
        final String dataNodeAddress = cluster.getHttpAddress(DATA_NODE);

        // Issue the reindex through a client that is pinned to the coordinator so the root reindex task is created on that node
        // (and is therefore the task that needs to relocate when the coordinator shuts down).
        final String taskId;
        try (RestClient coordClient = buildClient(restClientSettings(), new HttpHost[] { HttpHost.create(coordHttpAddress) })) {
            taskId = startThrottledReindex(coordClient);
        }
        waitForRootReindexTaskOn(coordNodeId, taskId);

        // Stage 1: tell the cluster the coordinator is going away. This populates NodesShutdownMetadata which gives the rest of the
        // cluster a chance to drain shards/PITs off the coordinator (it has none here, but it sets the right precondition for relocation).
        markNodeForShutdown(coordNodeId);

        // Stage 2: SIGTERM the coordinator in a background thread. The graceful shutdown sequence will run ShutdownPrepareService,
        // which fires the "reindex-stop" hook -> marks the running reindex for relocation. We rely on a parallel rethrottle (below) to
        // unblock the throttled task so the relocation hand-off actually executes within the shutdown timeout window.
        final Thread stopThread = new Thread(() -> cluster.stopNode(COORD_NODE, false), "stop-coord-node");
        stopThread.setDaemon(true);
        stopThread.start();

        try {
            // Stage 3: assert that the list API works
            assertListViaSurvivingNode(dataNodeAddress, taskId);

            // Stage 4: wait for the coordinator to be fully removed from cluster state.
            waitForCoordinatorRemovedFromCluster(dataNodeAddress, coordNodeId);

            // Stage 6: unthrottle via the data node
            unthrottleViaSurvivingNode(dataNodeAddress, taskId);

            // Stage 7: wait for the relocated task to complete successfully.
            assertReindexCompletesOnDataNode(dataNodeAddress, taskId, numDocs);
        } finally {
            stopThread.join(TimeUnit.SECONDS.toMillis(60));
        }

        // Verify the destination index actually has all docs.
        refresh(DEST);
        final Request countReq = new Request("GET", "/" + DEST + "/_count");
        final ObjectPath count = ObjectPath.createFromResponse(client().performRequest(countReq));
        assertThat(count.evaluate("count"), equalTo(numDocs));
    }

    private void createSourceIndexAndPopulate(int numDocs) throws IOException {
        // Place the source index on the data node only, so the PIT used by reindex stays available across coordinator shutdown.
        final Request create = new Request("PUT", "/" + SOURCE);
        create.setJsonEntity("""
            {
              "settings": {
                "number_of_shards": 1,
                "number_of_replicas": 0
              }
            }
            """);
        client().performRequest(create);

        final StringBuilder bulk = new StringBuilder();
        for (int i = 0; i < numDocs; i++) {
            bulk.append("{\"index\":{\"_id\":\"").append(i).append("\"}}\n");
            bulk.append("{\"n\":").append(i).append("}\n");
        }
        final Request bulkReq = new Request("POST", "/" + SOURCE + "/_bulk");
        bulkReq.addParameter("refresh", "true");
        bulkReq.setJsonEntity(bulk.toString());
        client().performRequest(bulkReq);
    }

    private String lookupNodeId(String nodeName) throws IOException {
        final ObjectPath nodes = ObjectPath.createFromResponse(adminClient().performRequest(new Request("GET", "/_nodes/" + nodeName)));
        final Map<String, Object> nodesMap = nodes.evaluate("nodes");
        assertThat("expected exactly one node matching name " + nodeName, nodesMap.size(), equalTo(1));
        return nodesMap.keySet().iterator().next();
    }

    private String startThrottledReindex(RestClient coordClient) throws IOException {
        // wait_for_completion=false has two important side effects on the server side that we rely on:
        // - setShouldStoreResult(true) is set in AbstractBaseReindexRestHandler so the task outcome (and any relocated_task_id link)
        // is persisted to .tasks and discoverable via GET /_reindex/{task_id}.
        // - setEligibleForRelocationOnShutdown(true) is set in RestReindexAction when the cluster supports the resilience feature,
        // opting this task into the relocation hand-off when ShutdownPrepareService runs.
        // Heavy throttle keeps the task running long enough for us to send the shutdown signal before it completes.
        final Request reindex = new Request("POST", "/_reindex");
        reindex.addParameter("wait_for_completion", "false");
        reindex.addParameter("requests_per_second", "0.000001");
        reindex.addParameter("refresh", "true");
        reindex.setJsonEntity("""
            {
              "source": { "index": "%s", "size": 1 },
              "dest":   { "index": "%s" }
            }
            """.formatted(SOURCE, DEST));
        final ObjectPath response = ObjectPath.createFromResponse(coordClient.performRequest(reindex));
        final String taskId = response.evaluate("task");
        assertThat("reindex task id should be returned by wait_for_completion=false", taskId, notNullValue());
        return taskId;
    }

    private void waitForRootReindexTaskOn(String nodeId, String taskId) throws Exception {
        // taskId returned by /_reindex?wait_for_completion=false is "<nodeId>:<localId>". If it doesn't start with the coordinator's
        // node ID, the reindex landed on the wrong node and the rest of the test's relocation flow won't exercise what we want.
        assertThat("reindex task " + taskId + " should be hosted on coord " + nodeId, taskId.startsWith(nodeId + ":"), equalTo(true));
        assertBusy(() -> {
            final Request listTasks = new Request("GET", "/_tasks");
            listTasks.addParameter("actions", "indices:data/write/reindex");
            listTasks.addParameter("detailed", "true");
            final ObjectPath tasks = ObjectPath.createFromResponse(adminClient().performRequest(listTasks));
            final Map<String, Object> nodes = tasks.evaluate("nodes");
            assertThat("expected reindex task to appear in cluster", nodes, notNullValue());
            assertThat(
                "reindex task should be running on " + nodeId + " but tasks were on " + nodes.keySet(),
                nodes.keySet(),
                hasItem(nodeId)
            );
        }, 30, TimeUnit.SECONDS);
    }

    private void markNodeForShutdown(String nodeId) throws IOException {
        final Request shutdown = new Request("PUT", "/_nodes/" + nodeId + "/shutdown");
        shutdown.setJsonEntity("""
            {
              "type": "remove",
              "reason": "ReindexRelocationWithSecurityIT - shutting down coordinator to trigger relocation"
            }
            """);
        adminClient().performRequest(shutdown);
    }

    private void assertListViaSurvivingNode(String dataNodeAddress, String taskId) throws IOException {
        // Use the data node address captured before coordinator shutdown so the fixture is not asked to resolve a dead node.
        try (
            RestClient dataClient = buildClient(restClientSettingsForUser(LIST_USER), new HttpHost[] { HttpHost.create(dataNodeAddress) })
        ) {
            Request list = new Request("GET", "/_reindex");
            Response response = dataClient.performRequest(list);
            ObjectPath body = ObjectPath.createFromResponse(response);
            // Surface fan-out failures up front so that an unexpectedly empty `reindex` array points at the real cause (e.g. the
            // coordinator going unreachable mid-list) rather than a bare hasSize(1) mismatch.
            assertThat(body.<List<?>>evaluate("node_failures"), anyOf(nullValue(), empty()));
            assertThat(body.<List<?>>evaluate("task_failures"), anyOf(nullValue(), empty()));
            assertThat(body.<List<?>>evaluate("reindex"), hasSize(1));
            assertThat(body.evaluate("reindex.0.id"), equalTo(taskId));
        }
    }

    private void unthrottleViaSurvivingNode(String dataNodeAddress, String taskId) throws Exception {
        logger.info("---> Unthrottling reindex task [{}]", taskId);
        // Use the data node address captured before coordinator shutdown so the fixture is not asked to resolve a dead node.
        try (
            RestClient dataClient = buildClient(
                restClientSettingsForUser(RETHROTTLE_USER),
                new HttpHost[] { HttpHost.create(dataNodeAddress) }
            )
        ) {
            assertBusy(() -> {
                final Request rethrottle = new Request("POST", "/_reindex/" + taskId + "/_rethrottle");
                // Forces the reindexing task to still take 2 seconds, giving enough time for the node to shut down
                rethrottle.addParameter("requests_per_second", String.valueOf(Float.POSITIVE_INFINITY));
                try {
                    dataClient.performRequest(rethrottle);
                    logger.info("--->  Successfully unthrottled task [{}]", taskId);
                } catch (Exception e) {
                    // Translate transient server errors to AssertionError so assertBusy will retry
                    throw new AssertionError("rethrottle failed", e);
                }
            });
        }
    }

    private void waitForCoordinatorRemovedFromCluster(String dataNodeAddress, String coordNodeId) throws Exception {
        logger.info("--->  Waiting for coordinator node [{}] to be removed from cluster state", coordNodeId);
        try (RestClient dataClient = buildClient(restAdminSettings(), new HttpHost[] { HttpHost.create(dataNodeAddress) })) {
            assertBusy(() -> {
                final Request clusterStateRequest = new Request("GET", "/_cluster/state/nodes");
                final ObjectPath clusterState = ObjectPath.createFromResponse(dataClient.performRequest(clusterStateRequest));
                final Map<String, Object> nodes = clusterState.evaluate("nodes");
                logger.info("---> Current nodes in cluster state: {}", nodes.keySet());
                assertThat("coordinator node should be removed from cluster state", nodes.keySet(), not(hasItem(coordNodeId)));
            });
        }
        logger.info("--->  Coordinator node [{}] successfully removed from cluster state", coordNodeId);
    }

    private void assertReindexCompletesOnDataNode(String dataNodeAddress, String originalTaskId, int numDocs) throws Exception {
        logger.info("Waiting for reindex task [{}] to complete on data node", originalTaskId);
        try (
            RestClient dataClient = buildClient(restClientSettingsForUser(GET_USER), new HttpHost[] { HttpHost.create(dataNodeAddress) })
        ) {
            assertBusy(() -> {
                // First, poll the status WITHOUT wait_for_completion to see current state
                final Request statusPoll = new Request("GET", "/_reindex/" + originalTaskId);
                final ObjectPath statusBody = ObjectPath.createFromResponse(dataClient.performRequest(statusPoll));
                final Boolean statusCompleted = statusBody.evaluate("completed");
                final Object statusError = statusBody.evaluate("error");
                final Object statusObj = statusBody.evaluate("status");

                // Extract progress information from status
                Integer statusCreated = null;
                Integer statusUpdated = null;
                Integer statusTotal = null;
                Float statusRequestsPerSecond = null;
                if (statusObj instanceof Map) {
                    @SuppressWarnings("unchecked")
                    Map<String, Object> status = (Map<String, Object>) statusObj;
                    statusCreated = status.get("created") instanceof Number n ? n.intValue() : null;
                    statusUpdated = status.get("updated") instanceof Number n ? n.intValue() : null;
                    statusTotal = status.get("total") instanceof Number n ? n.intValue() : null;
                    statusRequestsPerSecond = status.get("requests_per_second") instanceof Number n ? n.floatValue() : null;
                }

                logger.info(
                    "---> Task [{}] status BEFORE wait: completed={}, error={}, status.created={}, status.updated={}, status.total={}, status.requests_per_second={}",
                    originalTaskId,
                    statusCompleted,
                    statusError,
                    statusCreated,
                    statusUpdated,
                    statusTotal,
                    statusRequestsPerSecond
                );

                // Now do the actual wait_for_completion call
                final Request get = new Request("GET", "/_reindex/" + originalTaskId);
                get.addParameter("wait_for_completion", "true");
                get.addParameter("timeout", "10s");

                try {
                    final ObjectPath body = ObjectPath.createFromResponse(dataClient.performRequest(get));
                    final Boolean completed = body.evaluate("completed");
                    final Object error = body.evaluate("error");

                    logger.info("Task [{}] status AFTER wait: completed={}, error={}", originalTaskId, completed, error);

                    assertThat("original reindex task should complete (after relocation)", completed, equalTo(true));
                    // No security_exception should be surfaced anywhere in the chain.
                    assertThat("relocation hand-off should not have produced an exception", error, nullValue());
                    final Object created = body.evaluate("response.created");
                    assertThat("relocated reindex should have created destination docs", ((Number) created).intValue(), greaterThan(0));
                    final Object total = body.evaluate("response.total");
                    assertThat("relocated reindex should report the full source size", ((Number) total).intValue(), equalTo(numDocs));
                } catch (ResponseException e) {
                    logger.error("Task [{}] wait_for_completion failed: {}", originalTaskId, e.getMessage());
                    // Translate ResponseException (e.g. timeouts) to AssertionError so assertBusy will retry
                    throw new AssertionError("get reindex task failed: " + e.getMessage(), e);
                }
            }, 90, TimeUnit.SECONDS);
        }
        logger.info("---> Reindex task [{}] completed successfully", originalTaskId);
    }

    @Override
    protected boolean preserveClusterUponCompletion() {
        // The test deliberately stops one of the cluster nodes; let the @ClassRule tear it down.
        return true;
    }
}
