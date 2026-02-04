/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.http;

import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.elasticsearch.action.admin.cluster.health.TransportClusterHealthAction;
import org.elasticsearch.action.admin.cluster.settings.ClusterGetSettingsAction;
import org.elasticsearch.action.admin.cluster.state.ClusterStateAction;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesAction;
import org.elasticsearch.action.admin.indices.get.GetIndexAction;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsAction;
import org.elasticsearch.action.admin.indices.recovery.RecoveryAction;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsAction;
import org.elasticsearch.action.admin.indices.template.get.GetComponentTemplateAction;
import org.elasticsearch.action.admin.indices.template.get.GetComposableIndexTemplateAction;
import org.elasticsearch.action.admin.indices.template.get.GetIndexTemplatesAction;
import org.elasticsearch.action.admin.indices.template.post.SimulateIndexTemplateAction;
import org.elasticsearch.action.admin.indices.template.post.SimulateTemplateAction;
import org.elasticsearch.action.ingest.GetPipelineAction;
import org.elasticsearch.action.support.CancellableActionTestPlugin;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.RefCountingListener;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.rest.ObjectPath;

import java.util.Collection;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.action.support.ActionTestUtils.wrapAsRestResponseListener;
import static org.elasticsearch.test.TaskAssertions.assertAllTasksHaveFinished;
import static org.hamcrest.Matchers.greaterThan;

public class RestActionCancellationIT extends HttpSmokeTestCase {

    public void testIndicesRecoveryRestCancellation() {
        createIndex("test");
        ensureGreen("test");
        runRestActionCancellationTest(new Request(HttpGet.METHOD_NAME, "/_recovery"), RecoveryAction.NAME);
    }

    public void testCatRecoveryRestCancellation() {
        createIndex("test");
        ensureGreen("test");
        runRestActionCancellationTest(new Request(HttpGet.METHOD_NAME, "/_cat/recovery"), RecoveryAction.NAME);
    }

    public void testClusterHealthRestCancellation() {
        runRestActionCancellationTest(new Request(HttpGet.METHOD_NAME, "/_cluster/health"), TransportClusterHealthAction.NAME);
    }

    public void testClusterStateRestCancellation() {
        runRestActionCancellationTest(new Request(HttpGet.METHOD_NAME, "/_cluster/state"), ClusterStateAction.NAME);
    }

    public void testGetAliasesCancellation() {
        runRestActionCancellationTest(new Request(HttpGet.METHOD_NAME, "/_alias"), GetAliasesAction.NAME);
    }

    public void testCatAliasesCancellation() {
        runRestActionCancellationTest(new Request(HttpGet.METHOD_NAME, "/_cat/aliases"), GetAliasesAction.NAME);
    }

    public void testGetComponentTemplateCancellation() {
        runRestActionCancellationTest(new Request(HttpGet.METHOD_NAME, "/_component_template"), GetComponentTemplateAction.NAME);
    }

    public void testGetIndexTemplateCancellation() {
        runRestActionCancellationTest(new Request(HttpGet.METHOD_NAME, "/_template"), GetIndexTemplatesAction.NAME);
    }

    public void testGetComposableTemplateCancellation() {
        runRestActionCancellationTest(new Request(HttpGet.METHOD_NAME, "/_index_template"), GetComposableIndexTemplateAction.NAME);
    }

    public void testSimulateTemplateCancellation() {
        runRestActionCancellationTest(
            new Request(HttpPost.METHOD_NAME, "/_index_template/_simulate/random_index_template"),
            SimulateTemplateAction.NAME
        );
    }

    public void testSimulateIndexTemplateCancellation() {
        createIndex("test");
        runRestActionCancellationTest(
            new Request(HttpPost.METHOD_NAME, "/_index_template/_simulate_index/test"),
            SimulateIndexTemplateAction.NAME
        );
    }

    public void testClusterGetSettingsCancellation() {
        runRestActionCancellationTest(new Request(HttpGet.METHOD_NAME, "/_cluster/settings"), ClusterGetSettingsAction.NAME);
    }

    public void testGetPipelineCancellation() {
        runRestActionCancellationTest(new Request(HttpGet.METHOD_NAME, "/_ingest/pipeline"), GetPipelineAction.NAME);
    }

    public void testGetMappingsCancellation() {
        createIndex("test");
        runRestActionCancellationTest(new Request(HttpGet.METHOD_NAME, "/test/_mappings"), GetMappingsAction.NAME);
    }

    public void testGetIndicesCancellation() {
        createIndex("test");
        runRestActionCancellationTest(new Request(HttpGet.METHOD_NAME, "/test"), GetIndexAction.NAME);
    }

    public void testGetIndexSettingsCancellation() {
        createIndex("test");
        runRestActionCancellationTest(new Request(HttpGet.METHOD_NAME, "/test/_settings"), GetSettingsAction.NAME);
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
                                        wrapAsRestResponseListener(listeners.acquire(HttpSmokeTestCase::assertOK))
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

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), CancellableActionTestPlugin.class);
    }
}
