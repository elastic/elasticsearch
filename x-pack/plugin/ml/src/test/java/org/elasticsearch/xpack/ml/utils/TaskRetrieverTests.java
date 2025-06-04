/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.utils;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksRequestBuilder;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksResponse;
import org.elasticsearch.action.admin.cluster.node.tasks.list.TransportListTasksAction;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.internal.AdminClient;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.ClusterAdminClient;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.exception.ElasticsearchStatusException;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskInfo;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.xpack.ml.utils.TaskRetriever.getDownloadTaskInfo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TaskRetrieverTests extends ESTestCase {
    private static final TimeValue TIMEOUT = new TimeValue(30, TimeUnit.SECONDS);
    private ThreadPool threadPool;

    @Before
    public void setUpThreadPool() {
        threadPool = new TestThreadPool(getTestName());
    }

    @After
    public void tearDownThreadPool() {
        terminate(threadPool);
    }

    public void testGetExistingTaskInfoCallsOnFailureForAnError() {
        var client = mockListTasksClient(threadPool);

        doAnswer(invocationOnMock -> {
            @SuppressWarnings("unchecked")
            ActionListener<ListTasksResponse> actionListener = (ActionListener<ListTasksResponse>) invocationOnMock.getArguments()[2];
            actionListener.onFailure(new Exception("error"));

            return Void.TYPE;
        }).when(client).execute(same(TransportListTasksAction.TYPE), any(), any());

        var listener = new PlainActionFuture<TaskInfo>();

        getDownloadTaskInfo(client, "inferenceEntityId", false, TIMEOUT, () -> "", listener);

        var exception = expectThrows(ElasticsearchException.class, () -> listener.actionGet(TIMEOUT));
        assertThat(exception.status(), is(RestStatus.INTERNAL_SERVER_ERROR));
        assertThat(exception.getMessage(), is("Unable to retrieve task information for model id [inferenceEntityId]"));
    }

    public void testGetExistingTaskInfoCallsListenerWithNullWhenNoTasksExist() {
        var client = mockClientWithTasksResponse(Collections.emptyList(), threadPool);
        var listener = new PlainActionFuture<TaskInfo>();

        getDownloadTaskInfo(client, "inferenceEntityId", false, TIMEOUT, () -> "", listener);

        assertThat(listener.actionGet(TIMEOUT), nullValue());
    }

    public void testGetExistingTaskInfoCallsListenerWithTaskInfoWhenTaskExists() {
        List<TaskInfo> listTaskInfo = getTaskInfoListOfOne();
        var client = mockClientWithTasksResponse(listTaskInfo, threadPool);
        var listener = new PlainActionFuture<TaskInfo>();

        getDownloadTaskInfo(client, "inferenceEntityId", false, TIMEOUT, () -> "", listener);

        assertThat(listener.actionGet(TIMEOUT), is(listTaskInfo.get(0)));
    }

    public void testGetExistingTaskInfoCallsListenerWithFirstTaskInfoWhenMultipleTasksExist() {
        List<TaskInfo> listTaskInfo = getTaskInfoList(2);
        var client = mockClientWithTasksResponse(listTaskInfo, threadPool);
        var listener = new PlainActionFuture<TaskInfo>();

        getDownloadTaskInfo(client, "inferenceEntityId", false, TIMEOUT, () -> "", listener);

        assertThat(listener.actionGet(TIMEOUT), is(listTaskInfo.get(0)));
    }

    public void testGetTimeoutOnWaitForCompletion() {
        var client = mockListTasksClient(threadPool);

        doAnswer(invocationOnMock -> {
            @SuppressWarnings("unchecked")
            ActionListener<ListTasksResponse> actionListener = (ActionListener<ListTasksResponse>) invocationOnMock.getArguments()[2];
            actionListener.onResponse(
                new ListTasksResponse(
                    List.of(),
                    List.of(),
                    List.of(new ElasticsearchStatusException("node timeout", RestStatus.REQUEST_TIMEOUT))
                )
            );

            return Void.TYPE;
        }).when(client).execute(same(TransportListTasksAction.TYPE), any(), any());

        var listener = new PlainActionFuture<TaskInfo>();

        getDownloadTaskInfo(client, "inferenceEntityId", true, TIMEOUT, () -> "Testing timeout", listener);

        var exception = expectThrows(ElasticsearchException.class, () -> listener.actionGet(TIMEOUT));
        assertThat(exception.status(), is(RestStatus.REQUEST_TIMEOUT));
        assertThat(exception.getMessage(), is("Testing timeout"));
    }

    /**
     * A helper method for setting up a mock cluster client to return the passed in list of tasks.
     *
     * @param taskInfo a list of {@link TaskInfo} objects representing the tasks to return
     *                 when {@code Client.execute(ListTasksAction.INSTANCE, ...)} is called
     * @param threadPool a test thread pool to associate with the client
     * @return the mocked {@link Client}
     */
    public static Client mockClientWithTasksResponse(List<TaskInfo> taskInfo, ThreadPool threadPool) {
        var client = mockListTasksClient(threadPool);

        var listTasksResponse = mock(ListTasksResponse.class);
        when(listTasksResponse.getTasks()).thenReturn(taskInfo);

        doAnswer(invocationOnMock -> {
            @SuppressWarnings("unchecked")
            ActionListener<ListTasksResponse> actionListener = (ActionListener<ListTasksResponse>) invocationOnMock.getArguments()[2];
            actionListener.onResponse(listTasksResponse);

            return Void.TYPE;
        }).when(client).execute(same(TransportListTasksAction.TYPE), any(), any());

        return client;
    }

    /**
     * A helper method for setting up the mock cluster client so that it will return a valid {@link ListTasksRequestBuilder}.
     *
     * @param threadPool a test thread pool to associate with the client
     * @return a mocked Client
     */
    public static Client mockListTasksClient(ThreadPool threadPool) {
        var client = mockClusterClient(threadPool);
        mockListTasksClient(client);

        return client;
    }

    /**
     * A helper method for setting up the mock cluster client so that it will return a valid {@link ListTasksRequestBuilder}.
     *
     * @param client a Client that already has the admin and cluster clients mocked
     * @return a mocked Client
     */
    public static Client mockListTasksClient(Client client) {
        var cluster = client.admin().cluster();

        when(cluster.prepareListTasks()).thenReturn(new ListTasksRequestBuilder(client));

        return client;
    }

    /**
     * A helper method for setting up the mock cluster client.
     *
     * @param threadPool a test thread pool to associate with the client
     * @return a mocked Client
     */
    public static Client mockClusterClient(ThreadPool threadPool) {
        var client = mock(Client.class);
        var cluster = mock(ClusterAdminClient.class);
        var admin = mock(AdminClient.class);

        when(client.threadPool()).thenReturn(threadPool);
        when(client.admin()).thenReturn(admin);
        when(admin.cluster()).thenReturn(cluster);

        return client;
    }

    /**
     * A helper method for returning a list of one test {@link TaskInfo} object
     *
     * @return a list with a single TaskInfo object within it
     */
    public static List<TaskInfo> getTaskInfoListOfOne() {
        return getTaskInfoList(1);
    }

    /**
     * A helper method for returning a list of the specified number of test {@link TaskInfo} objects
     *
     * @param size the number of TaskInfo objects to create
     * @return a list of TaskInfo objects
     */
    public static List<TaskInfo> getTaskInfoList(int size) {
        final int ID_BASE = 100;
        final int PARENT_ID_BASE = 200;
        var list = new ArrayList<TaskInfo>(size);

        for (int i = 0; i < size; i++) {
            list.add(
                new TaskInfo(
                    new TaskId("test", ID_BASE + i),
                    "test",
                    "test",
                    "test",
                    "test",
                    null,
                    0,
                    0,
                    true,
                    false,
                    new TaskId("test", PARENT_ID_BASE + i),
                    Collections.emptyMap()
                )
            );
        }

        return list;
    }
}
