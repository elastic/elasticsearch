/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.TransportCreateIndexAction;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.admin.indices.mapping.put.TransportAutoPutMappingAction;
import org.elasticsearch.action.admin.indices.mapping.put.TransportPutMappingAction;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESIntegTestCase;
import org.hamcrest.Matchers;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.Future;

public class CreateIndexPriorityIT extends ESIntegTestCase {

    public static class TestPlugin extends Plugin {
        @Override
        public List<Setting<?>> getSettings() {
            return CollectionUtils.appendToCopyNoNullElements(
                super.getSettings(),
                MetadataCreateIndexService.CREATE_INDEX_PRIORITY_SETTING,
                MetadataMappingService.PUT_MAPPING_PRIORITY_SETTING
            );
        }
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopyNoNullElements(super.nodePlugins(), TestPlugin.class);
    }

    private static class RepeatingTask extends ClusterStateUpdateTask {

        private final ClusterService masterClusterService;
        private volatile boolean keepGoing = true;
        private int countdown = 5;

        RepeatingTask(ClusterService masterClusterService) {
            super(Priority.HIGH);
            this.masterClusterService = masterClusterService;
        }

        @Override
        public ClusterState execute(ClusterState currentState) {
            return ClusterState.builder(currentState).build();
        }

        @Override
        public void onFailure(Exception e) {
            fail(e);
        }

        @Override
        public void clusterStateProcessed(ClusterState initialState, ClusterState newState) {
            if (keepGoing || --countdown > 0) {
                // countdown to ensure a few more iterations run even after we're ready to proceed
                submitTask();
            }
        }

        void submitTask() {
            masterClusterService.submitUnbatchedStateUpdateTask("blocking task", RepeatingTask.this);
        }

        void awaitPendingAndStop(String taskSourcePrefix, SubscribableListener<?> listener, Future<?> future) {
            assertTrue(
                waitUntil(
                    () -> masterClusterService.getMasterService()
                        .pendingTasks()
                        .stream()
                        .anyMatch(pct -> pct.getSource().toString().startsWith(taskSourcePrefix))
                )
            );

            assertFalse(future.isDone());
            assertFalse(listener.isDone());
            keepGoing = false;
            safeAwait(listener.andThenAccept(ignored -> {
                assertFalse(keepGoing);
                assertThat(countdown, Matchers.equalTo(0));
            }));
            safeGet(future);
        }
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(otherSettings)
            .put(MetadataCreateIndexService.CREATE_INDEX_PRIORITY_SETTING.getKey(), "NORMAL")
            .put(MetadataMappingService.PUT_MAPPING_PRIORITY_SETTING.getKey(), "NORMAL")
            .build();
    }

    public void testReducePriorities() {
        final var masterClusterService = internalCluster().getCurrentMasterNodeInstance(ClusterService.class);
        final var indexName = "index-" + randomIdentifier();

        // starve all tasks at NORMAL or below
        final var createIndexBlockingTask = new RepeatingTask(masterClusterService);
        createIndexBlockingTask.submitTask();

        // now submit a create-index task, wait for it to be enqueued, verify its priority by the fact that it doesn't execute,
        // then remove the starvation to allow it through
        createIndexBlockingTask.awaitPendingAndStop(
            "create-index [" + indexName + "]",
            ClusterServiceUtils.addTemporaryStateListener(
                masterClusterService,
                cs -> cs.metadata().getProject(ProjectId.DEFAULT).index(indexName) != null
            ),
            client().execute(TransportCreateIndexAction.TYPE, new CreateIndexRequest(indexName))
        );

        // starve all tasks at NORMAL or below again
        final var putMappingBlockingTask = new RepeatingTask(masterClusterService);
        putMappingBlockingTask.submitTask();

        // now submit a put-mapping task, wait for it to be enqueued, verify its priority by the fact that it doesn't execute,
        // then remove the starvation to allow it through
        putMappingBlockingTask.awaitPendingAndStop(
            "put-mapping [" + indexName + "/",
            ClusterServiceUtils.addTemporaryStateListener(masterClusterService, cs -> {
                final var mappingVersion = cs.metadata().getProject(ProjectId.DEFAULT).index(indexName).getMappingVersion();
                if (mappingVersion == 1) {
                    return false;
                } else {
                    assertThat(mappingVersion, Matchers.greaterThan(1L));
                    return true;
                }
            }),
            client().execute(
                randomFrom(TransportPutMappingAction.TYPE, TransportAutoPutMappingAction.TYPE),
                new PutMappingRequest().setConcreteIndex(
                    masterClusterService.state().metadata().getProject(ProjectId.DEFAULT).index(indexName).getIndex()
                ).source("f", "type=keyword")
            )
        );
    }

}
