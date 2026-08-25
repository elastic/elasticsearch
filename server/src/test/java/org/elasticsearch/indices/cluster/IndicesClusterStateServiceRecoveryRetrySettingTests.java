/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.indices.cluster;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.seqno.RetentionLeaseSyncer;
import org.elasticsearch.index.shard.PrimaryReplicaSyncer;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.recovery.PeerRecoverySourceService;
import org.elasticsearch.indices.recovery.PeerRecoveryTargetService;
import org.elasticsearch.indices.recovery.RecoveryMetricsCollector;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.SnapshotShardContextFactory;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.snapshots.SnapshotShardsService;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.transport.MockTransport;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.indices.cluster.IndicesClusterStateService.LOCAL_RECOVERY_RETRY;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;

public class IndicesClusterStateServiceRecoveryRetrySettingTests extends ESTestCase {

    private ThreadPool threadPool;

    @Before
    public void createSuiteThreadPool() {
        threadPool = new TestThreadPool(this.getTestName());
    }

    @After
    public void stopThreadPool() {
        assertThat(ThreadPool.terminate(threadPool, SAFE_AWAIT_TIMEOUT.seconds(), TimeUnit.SECONDS), equalTo(true));
    }

    public void testLocalRecoveryRetryEnabledSettingDefaultValueAndDynamicUpdate() {
        // Expected default is false before enabling feature, update this when updating default value
        boolean expectedDefault = false;

        Set<Setting<?>> settingsSet = new HashSet<>(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        settingsSet.add(LOCAL_RECOVERY_RETRY);

        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, settingsSet);
        ClusterService clusterService = new ClusterService(Settings.EMPTY, clusterSettings, threadPool, null);
        IndicesClusterStateService indicesClusterStateService = createIndicesClusterStateService(clusterService, threadPool);
        try (clusterService; indicesClusterStateService) {
            assertEquals(expectedDefault, indicesClusterStateService.getLocalRecoveryRetryEnabled());

            // Flip
            clusterSettings.applySettings(Settings.builder().put(LOCAL_RECOVERY_RETRY.getKey(), !expectedDefault).build());
            assertEquals(!expectedDefault, indicesClusterStateService.getLocalRecoveryRetryEnabled());

            // Flip
            clusterSettings.applySettings(Settings.builder().put(LOCAL_RECOVERY_RETRY.getKey(), expectedDefault).build());
            assertEquals(expectedDefault, indicesClusterStateService.getLocalRecoveryRetryEnabled());
        }
    }

    public void testLocalRecoveryRetryEnabledSettingPresetValueAndDynamicUpdate() {
        // Preset value is negated default value to not pass accidentally
        boolean presetValue = !LOCAL_RECOVERY_RETRY.getDefault(Settings.EMPTY);
        Set<Setting<?>> settingsSet = new HashSet<>(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        settingsSet.add(LOCAL_RECOVERY_RETRY);

        Settings settings = Settings.builder().put(LOCAL_RECOVERY_RETRY.getKey(), presetValue).build();
        ClusterSettings clusterSettings = new ClusterSettings(settings, settingsSet);
        ClusterService clusterService = new ClusterService(settings, clusterSettings, threadPool, null);
        IndicesClusterStateService indicesClusterStateService = createIndicesClusterStateService(clusterService, threadPool);
        try (clusterService; indicesClusterStateService) {
            // Preset value
            assertEquals(presetValue, indicesClusterStateService.getLocalRecoveryRetryEnabled());

            // Flip
            clusterSettings.applySettings(Settings.builder().put(LOCAL_RECOVERY_RETRY.getKey(), !presetValue).build());
            assertEquals(!presetValue, indicesClusterStateService.getLocalRecoveryRetryEnabled());

            // Flip again
            clusterSettings.applySettings(Settings.builder().put(LOCAL_RECOVERY_RETRY.getKey(), presetValue).build());
            assertEquals(presetValue, indicesClusterStateService.getLocalRecoveryRetryEnabled());
        }
    }

    private static IndicesClusterStateService createIndicesClusterStateService(ClusterService clusterService, ThreadPool threadPool) {
        return new IndicesClusterStateService(
            Settings.EMPTY,
            mock(IndicesService.class),
            clusterService,
            threadPool,
            mock(PeerRecoveryTargetService.class),
            mock(ShardStateAction.class),
            mock(RepositoriesService.class),
            mock(SearchService.class),
            mock(PeerRecoverySourceService.class),
            new SnapshotShardsService(
                Settings.EMPTY,
                clusterService,
                mock(RepositoriesService.class),
                MockTransportService.createMockTransportService(new MockTransport(), threadPool),
                mock(IndicesService.class),
                mock(SnapshotShardContextFactory.class)
            ),
            mock(PrimaryReplicaSyncer.class),
            RetentionLeaseSyncer.EMPTY,
            mock(NodeClient.class),
            RecoveryMetricsCollector.NOOP
        );
    }
}
