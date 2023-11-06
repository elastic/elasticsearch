/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.http;

import org.apache.http.client.methods.HttpGet;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthAction;
import org.elasticsearch.action.admin.cluster.state.ClusterStateAction;
import org.elasticsearch.action.admin.indices.recovery.RecoveryAction;
import org.elasticsearch.action.support.CancellableActionTestPlugin;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.plugins.Plugin;

import java.util.Collection;
import java.util.concurrent.CancellationException;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.action.support.ActionTestUtils.wrapAsRestResponseListener;
import static org.elasticsearch.test.TaskAssertions.assertAllTasksHaveFinished;

public class RestActionCancellationIT extends HttpSmokeTestCase {

    public void testIndicesRecoveryRestCancellation() throws Exception {
        createIndex("test");
        ensureGreen("test");
        runRestActionCancellationTest(new Request(HttpGet.METHOD_NAME, "/_recovery"), RecoveryAction.NAME);
    }

    public void testCatRecoveryRestCancellation() throws Exception {
        createIndex("test");
        ensureGreen("test");
        runRestActionCancellationTest(new Request(HttpGet.METHOD_NAME, "/_cat/recovery"), RecoveryAction.NAME);
    }

    public void testClusterHealthRestCancellation() throws Exception {
        runRestActionCancellationTest(new Request(HttpGet.METHOD_NAME, "/_cluster/health"), ClusterHealthAction.NAME);
    }

    public void testClusterStateRestCancellation() throws Exception {
        runRestActionCancellationTest(new Request(HttpGet.METHOD_NAME, "/_cluster/state"), ClusterStateAction.NAME);
    }

    private void runRestActionCancellationTest(Request request, String actionName) throws Exception {
        final var node = usually() ? internalCluster().getRandomNodeName() : internalCluster().startCoordinatingOnlyNode(Settings.EMPTY);

        try (
            var restClient = createRestClient(node);
            var capturingAction = CancellableActionTestPlugin.capturingActionOnNode(actionName, node)
        ) {
            expectThrows(
                CancellationException.class,
                () -> PlainActionFuture.<Response, Exception>get(
                    responseFuture -> capturingAction.captureAndCancel(
                        restClient.performRequestAsync(request, wrapAsRestResponseListener(responseFuture))::cancel
                    ),
                    10,
                    TimeUnit.SECONDS
                )
            );
            assertAllTasksHaveFinished(actionName);
        } catch (Exception e) {
            fail(e);
        }
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), CancellableActionTestPlugin.class);
    }
}
