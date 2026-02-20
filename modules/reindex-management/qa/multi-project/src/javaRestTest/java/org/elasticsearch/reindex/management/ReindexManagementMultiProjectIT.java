/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v 3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.reindex.management;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.index.reindex.ReindexAction;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.test.rest.ObjectPath;
import org.junit.After;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.instanceOf;

/** Tests that endpoints in reindex-management module are project-aware and behave as expected in multi-project environments. */
public class ReindexManagementMultiProjectIT extends ESRestTestCase {

    private static final String SOURCE_INDEX = "reindex_src";
    private static final String DEST_INDEX = "reindex_dst";
    private static final int BULK_SIZE = 1;
    private static final int REQUESTS_PER_SECOND = 1;
    private static final int NUM_OF_SLICES = 2;
    private static final int NUMBER_OF_DOCUMENTS_THAT_TAKES_30_SECS_TO_INGEST = 30 * REQUESTS_PER_SECOND * BULK_SIZE;

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .nodes(2)   // 2 to test transport serializes projectId over wire.
        .module("reindex")
        .module("reindex-management")
        .module("test-multi-project")
        .setting("test.multi_project.enabled", "true")
        .setting("xpack.security.enabled", "false")
        .setting("xpack.ml.enabled", "false")
        .build();

    @Override
    protected boolean shouldConfigureProjects() {
        return false;
    }

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @After
    public void cleanup() throws Exception {
        if (preserveClusterUponCompletion() == false) {
            // Cancel all running reindex tasks before deleting projects to avoid
            // errors when tasks try to access project metadata after project deletion
            cancelAllRunningReindexTasks();
            cleanUpProjects();
        }
    }

    /**
     * Test <code>POST /_reindex/{task_id}/_cancel</code> API is project-aware, by doing the following:
     * 1. Create two projects: one will contain a reindex task
     * 2. Try to cancel the reindex from the other project and expect a 404
     * 3. Cancel the reindex from the correct project and expect success
     */
    public void testCancellingReindexOnlyWorksForCorrectProject() throws Exception {
        final String projectWithReindex = randomUniqueProjectId().id();
        final String projectWithoutReindex = randomUniqueProjectId().id();

        createProject(projectWithReindex);
        createProject(projectWithoutReindex);
        createPopulatedIndexInProject(SOURCE_INDEX, projectWithReindex);

        final TaskId taskId = startAsyncThrottledReindexInProject(projectWithReindex);

        assertTrue(runningTaskExistsInProject(taskId, projectWithReindex));

        final var cancellingFromOtherProjectException = expectThrows(
            ResponseException.class,
            () -> cancelReindexInProjectAndWaitForCompletion(taskId, projectWithoutReindex)
        );
        assertThat(cancellingFromOtherProjectException.getResponse().getStatusLine().getStatusCode(), equalTo(404));
        final String reason = ObjectPath.createFromResponse(cancellingFromOtherProjectException.getResponse()).evaluate("error.reason");
        assertThat(reason, equalTo(Strings.format("reindex task [%s] either not found or completed", taskId)));

        assertTrue(runningTaskExistsInProject(taskId, projectWithReindex));

        final Map<String, Object> response = cancelReindexInProjectAndWaitForCompletion(taskId, projectWithReindex);
        assertThat("reindex is cancelled", response, allOf(hasEntry("cancelled", true), hasEntry("completed", true)));

        assertFalse(runningTaskExistsInProject(taskId, projectWithReindex));
    }

    public void testGettingReindexOnlyWorksForCorrectProject() throws Exception {
        final String projectWithReindex = randomUniqueProjectId().id();
        final String projectWithoutReindex = randomUniqueProjectId().id();

        createProject(projectWithReindex);
        createProject(projectWithoutReindex);
        createPopulatedIndexInProject(SOURCE_INDEX, projectWithReindex);

        final TaskId taskId = startAsyncThrottledReindexInProject(projectWithReindex);

        assertTrue(runningTaskExistsInProject(taskId, projectWithReindex));
        final Map<String, Object> response = getReindexInProject(taskId, projectWithReindex);
        assertThat("reindex task is found", response, hasKey("id"));
        assertThat("task id matches", response.get("id"), equalTo(taskId.toString()));
        assertThat("task is not completed", response.get("completed"), equalTo(false));

        final var gettingFromOtherProjectException = expectThrows(
            ResponseException.class,
            () -> getReindexInProject(taskId, projectWithoutReindex)
        );
        assertThat(gettingFromOtherProjectException.getResponse().getStatusLine().getStatusCode(), equalTo(404));
        final String reason = ObjectPath.createFromResponse(gettingFromOtherProjectException.getResponse()).evaluate("error.reason");
        assertThat(reason, equalTo(Strings.format("task [%s] isn't running and hasn't stored its results", taskId)));

        // assert the task is still running
        assertTrue(runningTaskExistsInProject(taskId, projectWithReindex));
    }

    public void testListingReindexOnlyWorksForCorrectProject() throws Exception {
        final String project1 = randomUniqueProjectId().id();
        final String project2 = randomUniqueProjectId().id();

        createProject(project1);
        createProject(project2);
        createPopulatedIndexInProject(SOURCE_INDEX, project1);
        createPopulatedIndexInProject(SOURCE_INDEX, project2);

        final TaskId taskId1 = startAsyncThrottledReindexInProject(project1);
        final TaskId taskId2 = startAsyncThrottledReindexInProject(project2);

        assertTrue(runningTaskExistsInProject(taskId1, project1));
        assertTrue(runningTaskExistsInProject(taskId2, project2));

        // List from project1 - should only return taskId1
        final Map<String, Object> listResponse1 = listReindexInProject(project1);
        assertThat("list response has reindex array", listResponse1, hasKey("reindex"));
        assertThat("reindex is a list", listResponse1.get("reindex"), instanceOf(List.class));
        @SuppressWarnings("unchecked")
        final List<Map<String, Object>> reindexList1 = (List<Map<String, Object>>) listResponse1.get("reindex");
        assertThat("project1 list contains exactly one task", reindexList1.size(), equalTo(1));
        assertThat("project1 list contains taskId1", reindexList1.get(0).get("id"), equalTo(taskId1.toString()));

        // List from project2 - should only return taskId2
        final Map<String, Object> listResponse2 = listReindexInProject(project2);
        assertThat("list response has reindex array", listResponse2, hasKey("reindex"));
        assertThat("reindex is a list", listResponse2.get("reindex"), instanceOf(List.class));
        @SuppressWarnings("unchecked")
        final List<Map<String, Object>> reindexList2 = (List<Map<String, Object>>) listResponse2.get("reindex");
        assertThat("project2 list contains exactly one task", reindexList2.size(), equalTo(1));
        assertThat("project2 list contains taskId2", reindexList2.get(0).get("id"), equalTo(taskId2.toString()));
    }

    private static TaskId startAsyncThrottledReindexInProject(final String projectId) throws IOException {
        final Request request = new Request("POST", "/_reindex");
        setRequestProjectId(request, projectId);
        request.addParameter("wait_for_completion", "false");
        request.addParameter("slices", Integer.toString(NUM_OF_SLICES));
        request.addParameter("requests_per_second", Integer.toString(REQUESTS_PER_SECOND));
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
            """, SOURCE_INDEX, BULK_SIZE, DEST_INDEX));

        final Response response = assertOK(client().performRequest(request));
        final String task = (String) entityAsMap(response).get("task");
        assertNotNull("reindex did not return a task id", task);
        return new TaskId(task);
    }

    private static Map<String, Object> cancelReindexInProjectAndWaitForCompletion(final TaskId taskId, final String projectId)
        throws IOException {
        final Request request = new Request("POST", "/_reindex/" + taskId + "/_cancel");
        request.addParameter("wait_for_completion", "true");
        setRequestProjectId(request, projectId);
        final Response response = assertOK(client().performRequest(request));
        return entityAsMap(response);
    }

    private static boolean runningTaskExistsInProject(final TaskId taskId, final String projectId) throws IOException {
        final Request request = new Request("GET", "/_tasks/" + taskId);
        setRequestProjectId(request, projectId);
        try {
            final Response response = assertOK(client().performRequest(request));
            final Map<String, Object> body = entityAsMap(response);
            return Boolean.FALSE.equals(body.get("completed"));
        } catch (ResponseException e) {
            if (e.getResponse().getStatusLine().getStatusCode() == 404) {
                return false;
            }
            throw e;
        }
    }

    private static void createPopulatedIndexInProject(final String indexName, final String projectId) throws IOException {
        createIndex(request -> {
            setRequestProjectId(request, projectId);
            return assertOK(client().performRequest(request));
        }, indexName, null, null, null);

        final Request bulkRequest = new Request("POST", "/_bulk");
        setRequestProjectId(bulkRequest, projectId);
        bulkRequest.addParameter("refresh", "true");

        final StringBuilder bulkBody = new StringBuilder();
        for (int i = 0; i < NUMBER_OF_DOCUMENTS_THAT_TAKES_30_SECS_TO_INGEST; i++) {
            bulkBody.append(Strings.format("""
                {"index":{"_index":"%s"}}
                {"value": %d}
                """, indexName, i));
        }
        bulkRequest.setJsonEntity(bulkBody.toString());

        final Response bulkResponse = assertOK(client().performRequest(bulkRequest));
        final Map<String, Object> bulkResult = entityAsMap(bulkResponse);
        assertThat("bulk index didn't receive errors", bulkResult.get("errors"), equalTo(false));
    }

    private static Map<String, Object> getReindexInProject(final TaskId taskId, final String projectId) throws IOException {
        final Request request = new Request("GET", "/_reindex/" + taskId);
        setRequestProjectId(request, projectId);
        final Response response = assertOK(client().performRequest(request));
        return entityAsMap(response);
    }

    private static Map<String, Object> listReindexInProject(final String projectId) throws IOException {
        final Request request = new Request("GET", "/_reindex");
        setRequestProjectId(request, projectId);
        final Response response = assertOK(client().performRequest(request));
        return entityAsMap(response);
    }

    private static void setRequestProjectId(final Request request, final String projectId) {
        final RequestOptions.Builder options = request.getOptions().toBuilder();
        options.removeHeader(Task.X_ELASTIC_PROJECT_ID_HTTP_HEADER);
        options.addHeader(Task.X_ELASTIC_PROJECT_ID_HTTP_HEADER, projectId);
        request.setOptions(options);
    }

    private void cancelAllRunningReindexTasks() throws IOException {
        final Request cancelRequest = new Request("POST", "/_tasks/_cancel");
        cancelRequest.addParameter("actions", ReindexAction.NAME);
        cancelRequest.addParameter("wait_for_completion", "true");
        assertOK(adminClient().performRequest(cancelRequest));
    }

}
