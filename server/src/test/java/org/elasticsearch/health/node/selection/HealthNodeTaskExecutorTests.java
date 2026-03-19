/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.health.node.selection;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.persistent.PersistentTaskState;
import org.elasticsearch.persistent.PersistentTasksExecutor;
import org.elasticsearch.persistent.PersistentTasksExecutorTestUtils;
import org.elasticsearch.persistent.PersistentTasksService;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Set;

import static org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata.Type.SIGTERM;
import static org.elasticsearch.persistent.PersistentTasksExecutorTestUtils.assertNonMasterIgnoresToggeableTask;
import static org.elasticsearch.persistent.PersistentTasksExecutorTestUtils.assertToggeableMasterTaskReconciliation;
import static org.elasticsearch.persistent.PersistentTasksExecutorTestUtils.stateWithLocallyAssignedClusterTask;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

public class HealthNodeTaskExecutorTests extends ESTestCase {

    private PersistentTasksService persistentTasksService;
    private ThreadPool threadPool;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        persistentTasksService = mock(PersistentTasksService.class);
        threadPool = new TestThreadPool(getTestName());
    }

    @Override
    public void tearDown() throws Exception {
        terminate(threadPool);
        super.tearDown();
    }

    public void testMasterTaskReconciliation() {
        assertToggeableMasterTaskReconciliation(
            HealthNode.TASK_NAME,
            new HealthNodeTaskParams(),
            HealthNodeTaskExecutor.ENABLED_SETTING,
            Set.of(),
            cs -> stateWithLocallyAssignedClusterTask(cs, HealthNode.TASK_NAME, new HealthNodeTaskParams()),
            persistentTasksService,
            threadPool,
            PersistentTasksExecutor.Scope.CLUSTER,
            null,
            (service, nodeSettings, clusterSettings) -> HealthNodeTaskExecutor.create(service, persistentTasksService, nodeSettings, clusterSettings)
        );
    }

    public void testNonMasterNeverStartsOrStopsTask() {
        assertNonMasterIgnoresToggeableTask(
            HealthNodeTaskExecutor.ENABLED_SETTING,
            Set.of(),
            cs -> stateWithLocallyAssignedClusterTask(cs, HealthNode.TASK_NAME, new HealthNodeTaskParams()),
            persistentTasksService,
            threadPool,
            (service, nodeSettings, clusterSettings) -> HealthNodeTaskExecutor.create(service, persistentTasksService, nodeSettings, clusterSettings)
        );
    }

    public void testDoesNothingIfNodeShuttingDownButNotYetReassigned() {
        final var clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        final var state = PersistentTasksExecutorTestUtils.stateWithLocalNode(randomBoolean());
        try (ClusterService clusterService = ClusterServiceUtils.createClusterService(state, threadPool, clusterSettings)) {
            final HealthNodeTaskExecutor executor = HealthNodeTaskExecutor.create(
                clusterService,
                persistentTasksService,
                Settings.EMPTY,
                clusterSettings
            );
            final HealthNode task = mock(HealthNode.class);
            executor.nodeOperation(task, new HealthNodeTaskParams(), mock(PersistentTaskState.class));

            // Local node is now marked for shutdown but the health task is still assigned to it.
            final SingleNodeShutdownMetadata.Type shutdownType = randomFrom(
                SingleNodeShutdownMetadata.Type.REMOVE,
                SingleNodeShutdownMetadata.Type.RESTART,
                SIGTERM
            );
            final ClusterState shutdownState = PersistentTasksExecutorTestUtils.stateWithNodeShuttingDown(
                stateWithLocallyAssignedClusterTask(state, HealthNode.TASK_NAME, new HealthNodeTaskParams()),
                shutdownType
            );
            HealthNodeTaskExecutorTests.<Void>safeAwait(
                listener -> clusterService.getClusterApplierService()
                    .onNewClusterState("node shutdown applied", () -> shutdownState, listener)
            );
            // The executor must not abort the task on its own.
            verify(task, never()).markAsLocallyAborted(anyString());
            verify(task, never()).markAsCompleted();
        }
    }
}
