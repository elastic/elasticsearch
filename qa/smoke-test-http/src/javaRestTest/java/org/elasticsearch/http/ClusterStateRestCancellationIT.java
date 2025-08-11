/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.http;

import org.apache.http.client.methods.HttpGet;
import org.elasticsearch.Version;
import org.elasticsearch.action.admin.cluster.state.ClusterStateAction;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.Cancellable;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.cluster.AbstractDiffable;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.tasks.TaskInfo;
import org.elasticsearch.xcontent.XContentBuilder;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.function.UnaryOperator;

import static org.elasticsearch.action.support.ActionTestUtils.wrapAsRestResponseListener;
import static org.elasticsearch.test.TaskAssertions.awaitTaskWithPrefix;

public class ClusterStateRestCancellationIT extends HttpSmokeTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), AssertingCustomPlugin.class);
    }

    private void updateClusterState(ClusterService clusterService, UnaryOperator<ClusterState> updateOperator) {
        final PlainActionFuture<Void> future = new PlainActionFuture<>();
        clusterService.submitStateUpdateTask("update state", new ClusterStateUpdateTask() {
            @Override
            public ClusterState execute(ClusterState currentState) {
                return updateOperator.apply(currentState);
            }

            @Override
            public void onFailure(String source, Exception e) {
                throw new AssertionError(source, e);
            }

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                future.onResponse(null);
            }
        });
        future.actionGet();
    }

    public void testClusterStateRestCancellation() throws Exception {

        final ClusterService clusterService = internalCluster().getInstance(ClusterService.class, internalCluster().getMasterName());
        updateClusterState(clusterService, s -> ClusterState.builder(s).putCustom(AssertingCustom.NAME, AssertingCustom.INSTANCE).build());

        final Request clusterStateRequest = new Request(HttpGet.METHOD_NAME, "/_cluster/state");
        clusterStateRequest.addParameter("wait_for_metadata_version", Long.toString(Long.MAX_VALUE));
        clusterStateRequest.addParameter("wait_for_timeout", "1h");
        if (randomBoolean()) {
            clusterStateRequest.addParameter("local", "true");
        }

        final PlainActionFuture<Response> future = new PlainActionFuture<>();
        logger.info("--> sending cluster state request");
        final Cancellable cancellable = getRestClient().performRequestAsync(clusterStateRequest, wrapAsRestResponseListener(future));

        awaitTaskWithPrefix(ClusterStateAction.NAME);

        logger.info("--> cancelling cluster state request");
        cancellable.cancel();
        expectThrows(CancellationException.class, future::actionGet);

        logger.info("--> checking cluster state task completed");
        assertBusy(() -> {
            updateClusterState(clusterService, s -> ClusterState.builder(s).build());
            final List<TaskInfo> tasks = client().admin().cluster().prepareListTasks().get().getTasks();
            assertTrue(tasks.toString(), tasks.stream().noneMatch(t -> t.getAction().equals(ClusterStateAction.NAME)));
        });

        updateClusterState(clusterService, s -> ClusterState.builder(s).removeCustom(AssertingCustom.NAME).build());
    }

    private static class AssertingCustom extends AbstractDiffable<ClusterState.Custom> implements ClusterState.Custom {

        static final String NAME = "asserting";
        static final AssertingCustom INSTANCE = new AssertingCustom();

        @Override
        public String getWriteableName() {
            return NAME;
        }

        @Override
        public Version getMinimalSupportedVersion() {
            return Version.CURRENT;
        }

        @Override
        public void writeTo(StreamOutput out) {
            // no content
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) {
            throw new AssertionError("task should have been cancelled before serializing this custom");
        }
    }

    public static class AssertingCustomPlugin extends Plugin {
        @Override
        public List<NamedWriteableRegistry.Entry> getNamedWriteables() {
            return Collections.singletonList(
                new NamedWriteableRegistry.Entry(ClusterState.Custom.class, AssertingCustom.NAME, in -> AssertingCustom.INSTANCE)
            );
        }
    }

}
