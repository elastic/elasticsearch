/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.support;

import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.action.get.TransportMultiGetActionTests;
import org.elasticsearch.action.support.replication.StaleRequestException;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.project.TestProjectResolvers;
import org.elasticsearch.cluster.routing.SplitShardCountSummary;
import org.elasticsearch.cluster.service.ClusterApplierService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.PrioritizedEsThreadPoolExecutor;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.action.support.ReshardingActionHelper.ROUTE_REFRESH_TIMEOUT;
import static org.elasticsearch.common.UUIDs.randomBase64UUID;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ReshardingActionHelperTests extends ESTestCase {
    public void testStaleRequestExceptionTimesOut() {
        Index index = new Index("index1", randomBase64UUID());

        var threadPool = new TestThreadPool(ReshardingActionHelperTests.class.getSimpleName());

        ProjectId projectId = randomProjectIdOrDefault();
        var projectResolver = TestProjectResolvers.singleProject(projectId);
        final ProjectMetadata project = ProjectMetadata.builder(projectId)
            .put(
                new IndexMetadata.Builder(index.getName()).settings(
                    indexSettings(IndexVersion.current(), 1, 1).put(IndexMetadata.SETTING_INDEX_UUID, index.getUUID())
                )
            )
            .build();

        final ClusterState clusterState = ClusterState.builder(new ClusterName(TransportMultiGetActionTests.class.getSimpleName()))
            .metadata(new Metadata.Builder().put(project))
            .build();

        var clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenReturn(clusterState);

        final var settings = Settings.builder().put(ROUTE_REFRESH_TIMEOUT.getKey(), "100ms").build();
        when(clusterService.getSettings()).thenReturn(settings);

        final var clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        var clusterApplierService = new ClusterApplierService("node", settings, clusterSettings, threadPool) {
            private final PrioritizedEsThreadPoolExecutor directExecutor = new PrioritizedEsThreadPoolExecutor(
                "master-service",
                1,
                1,
                1,
                TimeUnit.SECONDS,
                r -> {
                    throw new AssertionError("should not create new threads");
                },
                null,
                null
            ) {
                @Override
                public void execute(Runnable command, final TimeValue timeout, final Runnable timeoutCallback) {
                    execute(command);
                }

                @Override
                public void execute(Runnable command) {
                    command.run();
                }
            };

            @Override
            protected PrioritizedEsThreadPoolExecutor createThreadPoolExecutor() {
                return directExecutor;
            }
        };

        try (clusterApplierService) {
            clusterApplierService.setInitialState(clusterState);
            clusterApplierService.setNodeConnectionsService(ClusterServiceUtils.createNoOpNodeConnectionsService());
            clusterApplierService.start();
            when(clusterService.getClusterApplierService()).thenReturn(clusterApplierService);
            var sut = new ReshardingActionHelper(clusterService, projectResolver, threadPool);

            var shardId = new ShardId(index, 0);
            // Note how the summary matches the number of shards in the index
            // meaning the check against the cluster state won't ever succeed.
            var exceptions = Map.of(shardId, new StaleRequestException(shardId, SplitShardCountSummary.fromInt(1)));

            var future = new PlainActionFuture<Void>();
            sut.waitForRoutingUpdate(exceptions, future);

            assertThrows(ElasticsearchTimeoutException.class, () -> future.actionGet(SAFE_AWAIT_TIMEOUT));
        } finally {
            ThreadPool.terminate(threadPool, SAFE_AWAIT_TIMEOUT.seconds(), TimeUnit.SECONDS);
        }
    }
}
