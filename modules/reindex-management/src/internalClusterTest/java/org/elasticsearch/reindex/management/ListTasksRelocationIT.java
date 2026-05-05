/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.reindex.management;

import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.reindex.ReindexAction;
import org.elasticsearch.node.ShutdownPrepareService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.reindex.ReindexPlugin;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskInfo;
import org.elasticsearch.tasks.TaskResult;
import org.elasticsearch.tasks.TaskResultsService;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.NodeRoles;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

/**
 * Integration test for {@code GET _tasks} (via {@link ListTasksResponse}) transparently showing relocated reindex tasks
 * with the original task ID and correct timing.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0, numClientNodes = 0)
public class ListTasksRelocationIT extends ESIntegTestCase {

    private static final String SOURCE_INDEX = "reindex_src";
    private static final String DEST_INDEX = "reindex_dst";

    private final int bulkSize = randomIntBetween(1, 4);
    private final int numOfSlices = randomIntBetween(1, 4);
    private final int requestsPerSecond = randomIntBetween(bulkSize * numOfSlices, 20);
    private final int numberOfDocumentsThatTakes60SecondsToIngest = 60 * requestsPerSecond;

    @BeforeClass
    public static void skipSetupIfReindexResilienceDisabled() {
        assumeTrue("reindex resilience is enabled", ReindexPlugin.REINDEX_RESILIENCE_ENABLED);
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(ReindexPlugin.class, ReindexManagementPlugin.class);
    }

    @Override
    protected boolean addMockHttpTransport() {
        return false;
    }

    public void testListTasksShowsOriginalIdentityAfterRelocation() throws Exception {
        final String nodeAName = internalCluster().startNode(
            NodeRoles.onlyRoles(Set.of(DiscoveryNodeRole.DATA_ROLE, DiscoveryNodeRole.MASTER_ROLE))
        );
        final String nodeBName = internalCluster().startNode(
            NodeRoles.onlyRoles(Set.of(DiscoveryNodeRole.DATA_ROLE, DiscoveryNodeRole.MASTER_ROLE))
        );
        final String nodeBId = nodeIdByName(nodeBName);
        ensureStableCluster(2);

        createIndexPinnedToNodeName(SOURCE_INDEX, nodeAName);
        createIndexPinnedToNodeName(DEST_INDEX, nodeAName);
        indexRandom(true, SOURCE_INDEX, numberOfDocumentsThatTakes60SecondsToIngest);
        ensureGreen(SOURCE_INDEX, DEST_INDEX);

        final TaskId originalTaskId = startAsyncThrottledReindexOnNode(nodeBName);
        assertThat(originalTaskId.getNodeId(), equalTo(nodeBId));

        final TaskInfo beforeTask = getSingleReindexParent();
        assertThat("taskId and originalTaskId match before relocation", beforeTask.taskId(), equalTo(beforeTask.originalTaskId()));
        assertThat("taskId matches the submitted task", beforeTask.taskId(), equalTo(originalTaskId));
        final long originalStartTimeMillis = beforeTask.startTime();

        shutdownNodeNameAndRelocate(nodeBName);

        final List<TaskInfo> allTasks = listAllReindexTasks();
        final List<TaskInfo> parents = allTasks.stream().filter(t -> t.parentTaskId().isSet() == false).toList();
        assertThat("exactly one reindex parent", parents, hasSize(1));
        final TaskInfo afterTask = parents.getFirst();

        assertThat("originalTaskId is preserved after relocation", afterTask.originalTaskId(), equalTo(originalTaskId));
        assertThat("originalStartTimeMillis is preserved", afterTask.originalStartTimeMillis(), equalTo(originalStartTimeMillis));
        assertThat("task is not cancelled", afterTask.cancelled(), is(false));
        assertThat("relocated parent taskId differs from originalTaskId", afterTask.taskId(), not(equalTo(afterTask.originalTaskId())));

        if (numOfSlices > 1) {
            final List<TaskInfo> children = allTasks.stream().filter(t -> t.parentTaskId().equals(afterTask.taskId())).toList();
            assertThat("slice count matches numOfSlices", children, hasSize(numOfSlices));
            for (TaskInfo child : children) {
                assertThat("child originalTaskId is its own taskId", child.originalTaskId(), equalTo(child.taskId()));
                assertThat(
                    "child originalStartTimeMillis is its own startTime",
                    child.originalStartTimeMillis(),
                    equalTo(child.startTime())
                );
                assertThat("child parentTaskId matches relocated parent", child.parentTaskId(), equalTo(afterTask.taskId()));
            }
        }

        final TaskId relocatedTaskId = getRelocatedTaskIdFromTasksIndex(originalTaskId);
        unthrottleReindex(relocatedTaskId);

        assertBusy(() -> {
            final List<TaskInfo> runningReindexes = listAllReindexTasks();
            assertThat("no running reindex tasks remain", runningReindexes, hasSize(0));
        }, 30, TimeUnit.SECONDS);
    }

    private List<TaskInfo> listAllReindexTasks() {
        final ListTasksResponse response = clusterAdmin().prepareListTasks().setActions(ReindexAction.NAME).setDetailed(true).get();
        assertThat(response.getTaskFailures(), hasSize(0));
        assertThat(response.getNodeFailures(), hasSize(0));
        return response.getTasks();
    }

    private List<TaskInfo> listReindexParents() {
        final ListTasksResponse response = clusterAdmin().prepareListTasks().setActions(ReindexAction.NAME).setDetailed(true).get();
        assertThat(response.getTaskFailures(), hasSize(0));
        assertThat(response.getNodeFailures(), hasSize(0));
        return response.getTasks().stream().filter(t -> t.parentTaskId().isSet() == false).toList();
    }

    private TaskInfo getSingleReindexParent() {
        final List<TaskInfo> parents = listReindexParents();
        assertThat("exactly one reindex parent", parents, hasSize(1));
        return parents.getFirst();
    }

    private TaskId startAsyncThrottledReindexOnNode(final String nodeName) throws Exception {
        try (RestClient restClient = createRestClient(nodeName)) {
            final Request request = new Request("POST", "/_reindex");
            request.addParameter("wait_for_completion", "false");
            request.addParameter("slices", Integer.toString(numOfSlices));
            request.addParameter("requests_per_second", Integer.toString(requestsPerSecond));
            request.setJsonEntity(Strings.format("""
                {
                  "source": {
                    "index": "%s",
                    "size": %d
                  },
                  "dest": {
                    "index": "%s"
                  }
                }
                """, SOURCE_INDEX, bulkSize, DEST_INDEX));

            final Response response = restClient.performRequest(request);
            final String task = (String) ESRestTestCase.entityAsMap(response).get("task");
            assertNotNull("reindex did not return a task id", task);
            return new TaskId(task);
        }
    }

    private void unthrottleReindex(final TaskId taskId) throws IOException {
        final Request request = new Request("POST", "/_reindex/" + taskId + "/_rethrottle");
        request.addParameter("requests_per_second", Integer.toString(-1));
        getRestClient().performRequest(request);
    }

    private void createIndexPinnedToNodeName(final String index, final String nodeName) {
        prepareCreate(index).setSettings(
            Settings.builder()
                .put("index.number_of_shards", randomIntBetween(1, 3))
                .put("index.number_of_replicas", 0)
                .put("index.routing.allocation.require._name", nodeName)
        ).get();
        ensureGreen(TimeValue.timeValueSeconds(10), index);
    }

    private String nodeIdByName(final String nodeName) {
        return clusterService().state()
            .nodes()
            .stream()
            .filter(node -> node.getName().equals(nodeName))
            .map(DiscoveryNode::getId)
            .findFirst()
            .orElseThrow(() -> new AssertionError("node with name [" + nodeName + "] not found"));
    }

    private TaskId getRelocatedTaskIdFromTasksIndex(TaskId originalTaskId) {
        ensureYellowAndNoInitializingShards(TaskResultsService.TASK_INDEX);
        assertNoFailures(indicesAdmin().prepareRefresh(TaskResultsService.TASK_INDEX).get());
        final GetResponse getResponse = client().prepareGet(TaskResultsService.TASK_INDEX, originalTaskId.toString()).get();
        assertThat("task exists in .tasks index", getResponse.isExists(), is(true));

        final TaskResult result;
        try (
            XContentParser parser = XContentType.JSON.xContent()
                .createParser(XContentParserConfiguration.EMPTY, getResponse.getSourceAsString())
        ) {
            result = TaskResult.PARSER.apply(parser, null);
        } catch (IOException e) {
            throw new AssertionError("failed to parse task result from .tasks index", e);
        }
        assertThat("original task should be completed", result.isCompleted(), is(true));
        final Map<String, Object> errorMap = result.getErrorAsMap();
        assertThat(errorMap.get("type"), equalTo("task_relocated_exception"));
        return new TaskId((String) errorMap.get("relocated_task_id"));
    }

    private void shutdownNodeNameAndRelocate(final String nodeName) throws Exception {
        assertFalse(".tasks index should not exist before shutdown", indexExists(TaskResultsService.TASK_INDEX));

        internalCluster().getInstance(ShutdownPrepareService.class, nodeName).prepareForShutdown();

        assertTrue(indexExists(TaskResultsService.TASK_INDEX));
        ensureGreen(TaskResultsService.TASK_INDEX);

        internalCluster().stopNode(nodeName);
    }
}
