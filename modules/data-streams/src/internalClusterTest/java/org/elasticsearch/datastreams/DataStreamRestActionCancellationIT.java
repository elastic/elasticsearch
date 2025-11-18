/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.datastreams;

import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.elasticsearch.action.datastreams.GetDataStreamAction;
import org.elasticsearch.action.datastreams.lifecycle.GetDataStreamLifecycleAction;
import org.elasticsearch.action.support.CancellableActionTestPlugin;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.RefCountingListener;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.datastreams.options.action.GetDataStreamOptionsAction;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.root.MainRestPlugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.rest.ObjectPath;
import org.elasticsearch.transport.netty4.Netty4Plugin;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.action.support.ActionTestUtils.wrapAsRestResponseListener;
import static org.elasticsearch.test.TaskAssertions.assertAllTasksHaveFinished;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.oneOf;

public class DataStreamRestActionCancellationIT extends ESIntegTestCase {

    @Override
    protected boolean addMockHttpTransport() {
        return false; // enable http
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(NetworkModule.TRANSPORT_TYPE_KEY, Netty4Plugin.NETTY_TRANSPORT_NAME)
            .put(NetworkModule.HTTP_TYPE_KEY, Netty4Plugin.NETTY_HTTP_TRANSPORT_NAME)
            .build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(getTestTransportPlugin(), MainRestPlugin.class, CancellableActionTestPlugin.class, DataStreamsPlugin.class);
    }

    public void testGetDataStreamCancellation() {
        runRestActionCancellationTest(new Request(HttpGet.METHOD_NAME, "/_data_stream"), GetDataStreamAction.NAME);
        runRestActionCancellationTest(new Request(HttpGet.METHOD_NAME, "/_data_stream?verbose"), GetDataStreamAction.NAME);
    }

    public void testGetDataStreamLifecycleCancellation() {
        runRestActionCancellationTest(
            new Request(HttpGet.METHOD_NAME, "/_data_stream/test/_lifecycle"),
            GetDataStreamLifecycleAction.INSTANCE.name()
        );
    }

    public void testGetDataStreamOptionsCancellation() {
        runRestActionCancellationTest(
            new Request(HttpGet.METHOD_NAME, "/_data_stream/test/_options"),
            GetDataStreamOptionsAction.INSTANCE.name()
        );
    }

    private void runRestActionCancellationTest(Request request, String actionName) {
        final var node = usually() ? internalCluster().getRandomNodeName() : internalCluster().startCoordinatingOnlyNode(Settings.EMPTY);

        try (
            var restClient = createRestClient(node);
            var capturingAction = CancellableActionTestPlugin.capturingActionOnNode(actionName, node)
        ) {
            final var responseFuture = new PlainActionFuture<Response>();
            final var restInvocation = restClient.performRequestAsync(request, wrapAsRestResponseListener(responseFuture));

            if (randomBoolean()) {
                // cancel by aborting the REST request
                capturingAction.captureAndCancel(restInvocation::cancel);
                expectThrows(ExecutionException.class, CancellationException.class, () -> responseFuture.get(10, TimeUnit.SECONDS));
            } else {
                // cancel via the task management API
                final var cancelFuture = new PlainActionFuture<Void>();
                capturingAction.captureAndCancel(
                    () -> SubscribableListener

                        .<ObjectPath>newForked(
                            l -> restClient.performRequestAsync(
                                getListTasksRequest(node, actionName),
                                wrapAsRestResponseListener(l.map(ObjectPath::createFromResponse))
                            )
                        )

                        .<Void>andThen((l, listTasksResponse) -> {
                            final var taskCount = listTasksResponse.evaluateArraySize("tasks");
                            assertThat(taskCount, greaterThan(0));
                            try (var listeners = new RefCountingListener(l)) {
                                for (int i = 0; i < taskCount; i++) {
                                    final var taskPrefix = "tasks." + i + ".";
                                    assertTrue(listTasksResponse.evaluate(taskPrefix + "cancellable"));
                                    assertFalse(listTasksResponse.evaluate(taskPrefix + "cancelled"));
                                    restClient.performRequestAsync(
                                        getCancelTaskRequest(
                                            listTasksResponse.evaluate(taskPrefix + "node"),
                                            listTasksResponse.evaluate(taskPrefix + "id")
                                        ),
                                        wrapAsRestResponseListener(listeners.acquire(DataStreamRestActionCancellationIT::assertOK))
                                    );
                                }
                            }
                        })

                        .addListener(cancelFuture)
                );
                cancelFuture.get(10, TimeUnit.SECONDS);
                expectThrows(Exception.class, () -> responseFuture.get(10, TimeUnit.SECONDS));
            }

            assertAllTasksHaveFinished(actionName);
        } catch (Exception e) {
            fail(e);
        }
    }

    private static Request getListTasksRequest(String taskNode, String actionName) {
        final var listTasksRequest = new Request(HttpGet.METHOD_NAME, "/_tasks");
        listTasksRequest.addParameter("nodes", taskNode);
        listTasksRequest.addParameter("actions", actionName);
        listTasksRequest.addParameter("group_by", "none");
        return listTasksRequest;
    }

    private static Request getCancelTaskRequest(String taskNode, int taskId) {
        final var cancelTaskRequest = new Request(HttpPost.METHOD_NAME, Strings.format("/_tasks/%s:%d/_cancel", taskNode, taskId));
        cancelTaskRequest.addParameter("wait_for_completion", null);
        return cancelTaskRequest;
    }

    public static void assertOK(Response response) {
        assertThat(response.getStatusLine().getStatusCode(), oneOf(200, 201));
    }

}
