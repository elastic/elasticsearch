/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.reindex;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksResponse;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.reindex.GetReindexRequest;
import org.elasticsearch.reindex.GetReindexResponse;
import org.elasticsearch.reindex.ReindexPlugin;
import org.elasticsearch.reindex.TransportGetReindexAction;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskInfo;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.Arrays;
import java.util.Collection;
import java.util.stream.IntStream;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

/**
 * Integration tests for GET /_reindex/{id} API.
 * These tests only run when the reindex_resilience feature flag is enabled.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 1)
public class GetReindexIT extends ESIntegTestCase {

    private static final String SOURCE_INDEX = "source_index";
    private static final String DEST_INDEX = "dest_index";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(ReindexPlugin.class);
    }

    public void testGetRunningReindexTask() throws Exception {
        assumeTrue("Feature flag must be enabled", ReindexPlugin.REINDEX_RESILIENCE_ENABLED);

        createIndex(SOURCE_INDEX);
        int numDocs = 50;
        indexRandom(
            true,
            IntStream.range(0, numDocs).mapToObj(i -> prepareIndex(SOURCE_INDEX).setId(String.valueOf(i)).setSource("n", i)).toList()
        );

        // Start a reindex asynchronously
        ReindexRequestBuilder reindexBuilder = new ReindexRequestBuilder(client());
        reindexBuilder.source(SOURCE_INDEX).destination(DEST_INDEX).execute();

        // Find the task ID by listing tasks
        ListTasksResponse listResponse = clusterAdmin().prepareListTasks().setActions(ReindexAction.NAME).setDetailed(true).get();
        assertThat(listResponse.getTasks().isEmpty(), equalTo(false));
        TaskInfo taskInfo = listResponse.getTasks().get(0);
        TaskId taskId = taskInfo.taskId();
        assertThat(taskId, notNullValue());

        // Get the reindex task while it's running or just completed
        GetReindexRequest getRequest = new GetReindexRequest();
        getRequest.setTaskId(taskId);
        GetReindexResponse getResponse = client().execute(TransportGetReindexAction.TYPE, getRequest).actionGet();

        assertThat(getResponse, notNullValue());
        // Task might be completed by the time we query it, so we just verify the response is valid
    }

    public void testGetCompletedReindexTask() throws Exception {
        assumeTrue("Feature flag must be enabled", ReindexPlugin.REINDEX_RESILIENCE_ENABLED);

        createIndex(SOURCE_INDEX);
        int numDocs = 50;
        indexRandom(
            true,
            IntStream.range(0, numDocs).mapToObj(i -> prepareIndex(SOURCE_INDEX).setId(String.valueOf(i)).setSource("n", i)).toList()
        );

        // Execute a reindex synchronously
        ReindexRequestBuilder reindexBuilder = new ReindexRequestBuilder(client());
        reindexBuilder.source(SOURCE_INDEX).destination(DEST_INDEX).get();

        // Verify reindex completed
        assertHitCount(prepareSearch(DEST_INDEX).setSize(0).setTrackTotalHits(true), numDocs);

        // Find the completed task by listing tasks
        ListTasksResponse listResponse = clusterAdmin().prepareListTasks().setActions(ReindexAction.NAME).setDetailed(true).get();
        if (listResponse.getTasks().isEmpty() == false) {
            TaskInfo taskInfo = listResponse.getTasks().get(0);
            TaskId taskId = taskInfo.taskId();

            GetReindexRequest getRequest = new GetReindexRequest();
            getRequest.setTaskId(taskId);
            GetReindexResponse getResponse = client().execute(TransportGetReindexAction.TYPE, getRequest).actionGet();

            assertThat(getResponse, notNullValue());
            assertThat(getResponse.getTask(), notNullValue());
            assertThat(getResponse.getTask().isCompleted(), equalTo(true));
        }
    }

    public void testGetNonExistentReindex() {
        assumeTrue("Feature flag must be enabled", ReindexPlugin.REINDEX_RESILIENCE_ENABLED);

        TaskId nonExistentTaskId = new TaskId("nonexistent", 99999);
        GetReindexRequest getRequest = new GetReindexRequest();
        getRequest.setTaskId(nonExistentTaskId);

        ResourceNotFoundException exception = expectThrows(
            ResourceNotFoundException.class,
            () -> client().execute(TransportGetReindexAction.TYPE, getRequest).actionGet()
        );

        assertThat(exception.getMessage(), notNullValue());
        assertThat(exception.getMessage(), org.hamcrest.Matchers.containsString("Reindex"));
        assertThat(exception.getMessage(), org.hamcrest.Matchers.containsString("not found"));
    }

    public void testGetNonReindexTask() throws Exception {
        assumeTrue("Feature flag must be enabled", ReindexPlugin.REINDEX_RESILIENCE_ENABLED);

        // Create a non-reindex task by listing tasks (which creates a task)
        ListTasksResponse listResponse = clusterAdmin().prepareListTasks().setDetailed(true).execute().actionGet();

        if (listResponse.getTasks().isEmpty() == false) {
            TaskInfo taskInfo = listResponse.getTasks().get(0);
            TaskId taskId = taskInfo.taskId();

            // Try to get it as a reindex task - should fail
            GetReindexRequest getRequest = new GetReindexRequest();
            getRequest.setTaskId(taskId);

            ResourceNotFoundException exception = expectThrows(
                ResourceNotFoundException.class,
                () -> client().execute(TransportGetReindexAction.TYPE, getRequest).actionGet()
            );

            assertThat(exception.getMessage(), notNullValue());
            assertThat(exception.getMessage(), org.hamcrest.Matchers.containsString("Reindex"));
            assertThat(exception.getMessage(), org.hamcrest.Matchers.containsString("not found"));
        }
    }

    public void testGetReindexWithWaitForCompletion() throws Exception {
        assumeTrue("Feature flag must be enabled", ReindexPlugin.REINDEX_RESILIENCE_ENABLED);

        createIndex(SOURCE_INDEX);
        int numDocs = 30;
        indexRandom(
            true,
            IntStream.range(0, numDocs).mapToObj(i -> prepareIndex(SOURCE_INDEX).setId(String.valueOf(i)).setSource("n", i)).toList()
        );

        // Start a reindex
        ReindexRequestBuilder reindexBuilder = new ReindexRequestBuilder(client());
        reindexBuilder.source(SOURCE_INDEX).destination(DEST_INDEX).execute();

        // Find the task ID by listing tasks
        ListTasksResponse listResponse = clusterAdmin().prepareListTasks().setActions(ReindexAction.NAME).setDetailed(true).get();
        assertThat(listResponse.getTasks().isEmpty(), equalTo(false));
        TaskInfo taskInfo = listResponse.getTasks().get(0);
        TaskId taskId = taskInfo.taskId();

        // Get with wait_for_completion
        GetReindexRequest getRequest = new GetReindexRequest();
        getRequest.setTaskId(taskId);
        getRequest.setWaitForCompletion(true);
        GetReindexResponse getResponse = client().execute(TransportGetReindexAction.TYPE, getRequest).actionGet();

        assertThat(getResponse, notNullValue());
        assertThat(getResponse.getTask(), notNullValue());
        assertThat(getResponse.getTask().isCompleted(), equalTo(true));
    }
}
