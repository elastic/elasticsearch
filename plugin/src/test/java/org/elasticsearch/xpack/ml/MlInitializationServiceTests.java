/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml;

import org.elasticsearch.Version;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexTemplateMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.ml.job.persistence.AnomalyDetectorsIndex;
import org.elasticsearch.xpack.ml.job.persistence.JobProvider;
import org.elasticsearch.xpack.ml.notifications.Auditor;
import org.junit.Before;

import java.net.InetAddress;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.function.BiConsumer;

import static org.elasticsearch.mock.orig.Mockito.doAnswer;
import static org.elasticsearch.mock.orig.Mockito.times;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class MlInitializationServiceTests extends ESTestCase {

    private ThreadPool threadPool;
    private ExecutorService executorService;
    private ClusterService clusterService;
    private Client client;
    private JobProvider jobProvider;

    @Before
    public void setUpMocks() {
        threadPool = mock(ThreadPool.class);
        executorService = mock(ExecutorService.class);
        clusterService = mock(ClusterService.class);
        client = mock(Client.class);
        jobProvider = mock(JobProvider.class);

        doAnswer(invocation -> {
            ((Runnable) invocation.getArguments()[0]).run();
            return null;
        }).when(executorService).execute(any(Runnable.class));
        when(threadPool.executor(ThreadPool.Names.GENERIC)).thenReturn(executorService);
        ScheduledFuture scheduledFuture = mock(ScheduledFuture.class);
        when(threadPool.schedule(any(), any(), any())).thenReturn(scheduledFuture);
    }

    public void testInitialize() throws Exception {
        JobProvider jobProvider = mockJobProvider();

        ClusterService clusterService = mock(ClusterService.class);
        MlInitializationService initializationService =
                new MlInitializationService(Settings.EMPTY, threadPool, clusterService, client, jobProvider);

        ClusterState cs = ClusterState.builder(new ClusterName("_name"))
                .nodes(DiscoveryNodes.builder()
                        .add(new DiscoveryNode("_node_id", new TransportAddress(InetAddress.getLoopbackAddress(), 9200), Version.CURRENT))
                        .localNodeId("_node_id")
                        .masterNodeId("_node_id"))
                .metaData(MetaData.builder())
                .build();
        initializationService.clusterChanged(new ClusterChangedEvent("_source", cs, cs));

        verify(clusterService, times(1)).submitStateUpdateTask(eq("install-ml-metadata"), any());
        assertThat(initializationService.getDailyManagementService().isStarted(), is(true));
        verify(jobProvider, times(1)).putNotificationMessageIndexTemplate(any());
        verify(jobProvider, times(1)).putMetaIndexTemplate(any());
        verify(jobProvider, times(1)).putJobStateIndexTemplate(any());
        verify(jobProvider, times(1)).putJobResultsIndexTemplate(any());
    }

    public void testInitialize_noMasterNode() throws Exception {
        MlInitializationService initializationService =
                new MlInitializationService(Settings.EMPTY, threadPool, clusterService, client, jobProvider);

        ClusterState cs = ClusterState.builder(new ClusterName("_name"))
                .nodes(DiscoveryNodes.builder()
                        .add(new DiscoveryNode("_node_id", new TransportAddress(InetAddress.getLoopbackAddress(), 9200), Version.CURRENT)))
                .metaData(MetaData.builder())
                .build();
        initializationService.clusterChanged(new ClusterChangedEvent("_source", cs, cs));

        verify(clusterService, times(0)).submitStateUpdateTask(eq("install-ml-metadata"), any());
        assertThat(initializationService.getDailyManagementService(), is(nullValue()));
        verify(jobProvider, times(0)).putNotificationMessageIndexTemplate(any());
        verify(jobProvider, times(0)).putMetaIndexTemplate(any());
        verify(jobProvider, times(0)).putJobStateIndexTemplate(any());
        verify(jobProvider, times(0)).putJobResultsIndexTemplate(any());
    }

    public void testInitialize_alreadyInitialized() throws Exception {
        ThreadPool threadPool = mock(ThreadPool.class);
        ExecutorService executorService = mock(ExecutorService.class);
        doAnswer(invocation -> {
            ((Runnable) invocation.getArguments()[0]).run();
            return null;
        }).when(executorService).execute(any(Runnable.class));
        when(threadPool.executor(ThreadPool.Names.GENERIC)).thenReturn(executorService);

        ClusterService clusterService = mock(ClusterService.class);
        JobProvider jobProvider = mockJobProvider();
        MlInitializationService initializationService =
                new MlInitializationService(Settings.EMPTY, threadPool, clusterService, client, jobProvider);

        ClusterState cs = ClusterState.builder(new ClusterName("_name"))
                .nodes(DiscoveryNodes.builder()
                        .add(new DiscoveryNode("_node_id", new TransportAddress(InetAddress.getLoopbackAddress(), 9200), Version.CURRENT))
                        .localNodeId("_node_id")
                        .masterNodeId("_node_id"))
                .metaData(MetaData.builder()
                        .put(IndexMetaData.builder(Auditor.NOTIFICATIONS_INDEX).settings(Settings.builder()
                                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0)
                                .put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT)
                        ))
                        .put(IndexMetaData.builder(JobProvider.ML_META_INDEX).settings(Settings.builder()
                                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0)
                                .put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT)
                        ))
                        .put(IndexMetaData.builder(AnomalyDetectorsIndex.jobStateIndexName()).settings(Settings.builder()
                                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0)
                                .put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT)
                        ))
                        .put(IndexTemplateMetaData.builder(Auditor.NOTIFICATIONS_INDEX).build())
                        .put(IndexTemplateMetaData.builder(JobProvider.ML_META_INDEX).build())
                        .put(IndexTemplateMetaData.builder(AnomalyDetectorsIndex.jobStateIndexName()).build())
                        .put(IndexTemplateMetaData.builder(AnomalyDetectorsIndex.jobResultsIndexPrefix()).build())
                        .putCustom(MlMetadata.TYPE, new MlMetadata.Builder().build()))
                .build();
        MlDailyManagementService initialDailyManagementService = mock(MlDailyManagementService.class);
        initializationService.setDailyManagementService(initialDailyManagementService);
        initializationService.clusterChanged(new ClusterChangedEvent("_source", cs, cs));

        verify(clusterService, times(0)).submitStateUpdateTask(eq("install-ml-metadata"), any());
        assertSame(initialDailyManagementService, initializationService.getDailyManagementService());
        verify(jobProvider, times(0)).putNotificationMessageIndexTemplate(any());
        verify(jobProvider, times(0)).putMetaIndexTemplate(any());
        verify(jobProvider, times(0)).putJobStateIndexTemplate(any());
        verify(jobProvider, times(0)).putJobResultsIndexTemplate(any());
    }

    public void testInitialize_onlyOnce() throws Exception {
        ThreadPool threadPool = mock(ThreadPool.class);
        ExecutorService executorService = mock(ExecutorService.class);
        doAnswer(invocation -> {
            ((Runnable) invocation.getArguments()[0]).run();
            return null;
        }).when(executorService).execute(any(Runnable.class));
        when(threadPool.executor(ThreadPool.Names.GENERIC)).thenReturn(executorService);

        ClusterService clusterService = mock(ClusterService.class);
        JobProvider jobProvider = mockJobProvider();
        MlInitializationService initializationService =
                new MlInitializationService(Settings.EMPTY, threadPool, clusterService, client, jobProvider);

        ClusterState cs = ClusterState.builder(new ClusterName("_name"))
                .nodes(DiscoveryNodes.builder()
                        .add(new DiscoveryNode("_node_id", new TransportAddress(InetAddress.getLoopbackAddress(), 9200), Version.CURRENT))
                        .localNodeId("_node_id")
                        .masterNodeId("_node_id"))
                .metaData(MetaData.builder())
                .build();
        initializationService.clusterChanged(new ClusterChangedEvent("_source", cs, cs));
        initializationService.clusterChanged(new ClusterChangedEvent("_source", cs, cs));

        verify(clusterService, times(1)).submitStateUpdateTask(eq("install-ml-metadata"), any());
        verify(jobProvider, times(1)).putNotificationMessageIndexTemplate(any());
        verify(jobProvider, times(1)).putMetaIndexTemplate(any());
        verify(jobProvider, times(1)).putJobStateIndexTemplate(any());
        verify(jobProvider, times(1)).putJobResultsIndexTemplate(any());
    }

    private JobProvider mockJobProvider() {
        JobProvider jobProvider = mock(JobProvider.class);

        doAnswer(invocation -> {
            BiConsumer<Boolean, Exception> listener = (BiConsumer<Boolean, Exception>)invocation.getArguments()[0];
            listener.accept(true,  null);
            return null;
        }).when(jobProvider).putMetaIndexTemplate(any());
        doAnswer(invocation -> {
            BiConsumer<Boolean, Exception> listener = (BiConsumer<Boolean, Exception>)invocation.getArguments()[0];
            listener.accept(true,  null);
            return null;
        }).when(jobProvider).putNotificationMessageIndexTemplate(any());
        doAnswer(invocation -> {
            BiConsumer<Boolean, Exception> listener = (BiConsumer<Boolean, Exception>)invocation.getArguments()[0];
            listener.accept(true,  null);
            return null;
        }).when(jobProvider).putJobStateIndexTemplate(any());
        doAnswer(invocation -> {
            BiConsumer<Boolean, Exception> listener = (BiConsumer<Boolean, Exception>)invocation.getArguments()[0];
            listener.accept(true,  null);
            return null;
        }).when(jobProvider).putJobResultsIndexTemplate(any());

        return jobProvider;
    }

    public void testNodeGoesFromMasterToNonMasterAndBack() throws Exception {
        MlInitializationService initializationService =
                new MlInitializationService(Settings.EMPTY, threadPool, clusterService, client, jobProvider);
        MlDailyManagementService initialDailyManagementService = mock(MlDailyManagementService.class);
        initializationService.setDailyManagementService(initialDailyManagementService);

        ClusterState masterCs = ClusterState.builder(new ClusterName("_name"))
                .nodes(DiscoveryNodes.builder()
                        .add(new DiscoveryNode("_node_id", new TransportAddress(InetAddress.getLoopbackAddress(), 9200), Version.CURRENT))
                        .localNodeId("_node_id")
                        .masterNodeId("_node_id"))
                .metaData(MetaData.builder())
                .build();
        ClusterState noMasterCs = ClusterState.builder(new ClusterName("_name"))
                .nodes(DiscoveryNodes.builder()
                        .add(new DiscoveryNode("_node_id", new TransportAddress(InetAddress.getLoopbackAddress(), 9200), Version.CURRENT)))
                .metaData(MetaData.builder())
                .build();
        initializationService.clusterChanged(new ClusterChangedEvent("_source", noMasterCs, masterCs));

        verify(initialDailyManagementService).stop();

        initializationService.clusterChanged(new ClusterChangedEvent("_source", masterCs, noMasterCs));
        MlDailyManagementService finalDailyManagementService = initializationService.getDailyManagementService();
        assertNotSame(initialDailyManagementService, finalDailyManagementService);
        assertThat(initializationService.getDailyManagementService().isStarted(), is(true));
    }
}
