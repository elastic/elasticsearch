/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest.geoip;

import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.project.TestProjectResolvers;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.node.Node;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.persistent.PersistentTasksService;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.threadpool.DefaultBuiltInExecutorBuilders;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.ingest.geoip.DatabaseNodeServiceTests.createClusterState;
import static org.elasticsearch.tasks.TaskId.EMPTY_TASK_ID;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class GeoIpDownloaderTaskExecutorTests extends ESTestCase {

    private ClusterService clusterService;
    private ClusterSettings clusterSettings;
    private ThreadPool threadPool;
    private NoOpClient client;
    private final AtomicInteger onDemandCount = new AtomicInteger();
    private final AtomicInteger restartCount = new AtomicInteger();

    @Before
    public void setup() {
        ProjectId projectId = ProjectId.DEFAULT;
        clusterService = mock(ClusterService.class);
        clusterSettings = new ClusterSettings(
            Settings.EMPTY,
            Set.of(
                GeoIpDownloaderTaskExecutor.EAGER_DOWNLOAD_SETTING,
                GeoIpDownloaderTaskExecutor.POLL_INTERVAL_SETTING,
                GeoIpDownloaderTaskExecutor.ENABLED_SETTING,
                GeoIpDownloader.ENDPOINT_SETTING
            )
        );
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);
        when(clusterService.getSettings()).thenReturn(Settings.EMPTY);
        when(clusterService.state()).thenReturn(createClusterState(projectId, new PersistentTasksCustomMetadata(1L, Map.of())));
        threadPool = new ThreadPool(
            Settings.builder().put(Node.NODE_NAME_SETTING.getKey(), "test").build(),
            MeterRegistry.NOOP,
            new DefaultBuiltInExecutorBuilders()
        );
        client = new NoOpClient(threadPool, TestProjectResolvers.singleProject(projectId));

        GeoIpDownloaderTaskExecutor executor = new GeoIpDownloaderTaskExecutor(client, mock(HttpClient.class), clusterService, threadPool);
        executor.init();

        GeoIpDownloader downloader = new GeoIpDownloader(
            client.projectClient(projectId),
            mock(HttpClient.class),
            clusterService,
            threadPool,
            Settings.EMPTY,
            1,
            "",
            "",
            "",
            EMPTY_TASK_ID,
            Map.of(),
            () -> GeoIpDownloaderTaskExecutor.POLL_INTERVAL_SETTING.getDefault(Settings.EMPTY),
            () -> GeoIpDownloaderTaskExecutor.EAGER_DOWNLOAD_SETTING.getDefault(Settings.EMPTY),
            projectId
        ) {
            {
                init(new PersistentTasksService(clusterService, threadPool, client), null, null, 0);
            }

            @Override
            public void requestRunOnDemand() {
                onDemandCount.incrementAndGet();
            }

            @Override
            public void restartPeriodicRun() {
                restartCount.incrementAndGet();
            }
        };
        executor.nodeOperation(downloader, new GeoIpTaskParams(), null);
        // nodeOperation calls restartPeriodicRun once (ENABLED_SETTING defaults to true)
        assertEquals(1, restartCount.get());
        onDemandCount.set(0);
        restartCount.set(0);
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
        threadPool.shutdownNow();
    }

    public void testEagerDownloadSetting() {
        assertEquals(0, onDemandCount.get());

        clusterSettings.applySettings(Settings.builder().put(GeoIpDownloaderTaskExecutor.EAGER_DOWNLOAD_SETTING.getKey(), true).build());
        assertEquals(1, onDemandCount.get());

        // Same value again — no-op
        clusterSettings.applySettings(Settings.builder().put(GeoIpDownloaderTaskExecutor.EAGER_DOWNLOAD_SETTING.getKey(), true).build());
        assertEquals(1, onDemandCount.get());

        // Setting to false does not trigger on-demand run
        clusterSettings.applySettings(Settings.builder().put(GeoIpDownloaderTaskExecutor.EAGER_DOWNLOAD_SETTING.getKey(), false).build());
        assertEquals(1, onDemandCount.get());
    }

    public void testPollIntervalSetting() {
        assertEquals(0, restartCount.get());

        clusterSettings.applySettings(
            Settings.builder().put(GeoIpDownloaderTaskExecutor.POLL_INTERVAL_SETTING.getKey(), TimeValue.timeValueDays(5)).build()
        );
        assertEquals(1, restartCount.get());

        // Same value again — no-op
        clusterSettings.applySettings(
            Settings.builder().put(GeoIpDownloaderTaskExecutor.POLL_INTERVAL_SETTING.getKey(), TimeValue.timeValueDays(5)).build()
        );
        assertEquals(1, restartCount.get());

        // Different value triggers restart
        clusterSettings.applySettings(
            Settings.builder().put(GeoIpDownloaderTaskExecutor.POLL_INTERVAL_SETTING.getKey(), TimeValue.timeValueDays(7)).build()
        );
        assertEquals(2, restartCount.get());
    }
}
