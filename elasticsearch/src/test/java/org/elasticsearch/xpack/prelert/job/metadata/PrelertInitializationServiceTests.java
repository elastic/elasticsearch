/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.prelert.job.metadata;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.LocalTransportAddress;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.prelert.job.persistence.ElasticsearchJobProvider;

import java.util.concurrent.ExecutorService;

import static org.elasticsearch.mock.orig.Mockito.doAnswer;
import static org.elasticsearch.mock.orig.Mockito.times;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class PrelertInitializationServiceTests extends ESTestCase {

    public void testInitialize() {
        ThreadPool threadPool = mock(ThreadPool.class);
        ExecutorService executorService = mock(ExecutorService.class);
        doAnswer(invocation -> {
            ((Runnable) invocation.getArguments()[0]).run();
            return null;
        }).when(executorService).execute(any(Runnable.class));
        when(threadPool.executor(ThreadPool.Names.GENERIC)).thenReturn(executorService);

        ClusterService clusterService = mock(ClusterService.class);
        ElasticsearchJobProvider jobProvider = mock(ElasticsearchJobProvider.class);
        PrelertInitializationService initializationService =
                new PrelertInitializationService(Settings.EMPTY, threadPool, clusterService, jobProvider);

        ClusterState cs = ClusterState.builder(new ClusterName("_name"))
                .nodes(DiscoveryNodes.builder()
                        .add(new DiscoveryNode("_node_id", new LocalTransportAddress("_id"), Version.CURRENT))
                        .localNodeId("_node_id")
                        .masterNodeId("_node_id"))
                .metaData(MetaData.builder())
                .build();
        initializationService.clusterChanged(new ClusterChangedEvent("_source", cs, cs));

        verify(clusterService, times(1)).submitStateUpdateTask(eq("install-prelert-metadata"), any());
        verify(jobProvider, times(1)).createUsageMeteringIndex(any());
    }

    public void testInitialize_noMasterNode() {
        ThreadPool threadPool = mock(ThreadPool.class);
        ExecutorService executorService = mock(ExecutorService.class);
        doAnswer(invocation -> {
            ((Runnable) invocation.getArguments()[0]).run();
            return null;
        }).when(executorService).execute(any(Runnable.class));
        when(threadPool.executor(ThreadPool.Names.GENERIC)).thenReturn(executorService);

        ClusterService clusterService = mock(ClusterService.class);
        ElasticsearchJobProvider jobProvider = mock(ElasticsearchJobProvider.class);
        PrelertInitializationService initializationService =
                new PrelertInitializationService(Settings.EMPTY, threadPool, clusterService, jobProvider);

        ClusterState cs = ClusterState.builder(new ClusterName("_name"))
                .nodes(DiscoveryNodes.builder()
                        .add(new DiscoveryNode("_node_id", new LocalTransportAddress("_id"), Version.CURRENT)))
                .metaData(MetaData.builder())
                .build();
        initializationService.clusterChanged(new ClusterChangedEvent("_source", cs, cs));

        verify(clusterService, times(0)).submitStateUpdateTask(eq("install-prelert-metadata"), any());
        verify(jobProvider, times(0)).createUsageMeteringIndex(any());
    }

    public void testInitialize_alreadyInitialized() {
        ThreadPool threadPool = mock(ThreadPool.class);
        ExecutorService executorService = mock(ExecutorService.class);
        doAnswer(invocation -> {
            ((Runnable) invocation.getArguments()[0]).run();
            return null;
        }).when(executorService).execute(any(Runnable.class));
        when(threadPool.executor(ThreadPool.Names.GENERIC)).thenReturn(executorService);

        ClusterService clusterService = mock(ClusterService.class);
        ElasticsearchJobProvider jobProvider = mock(ElasticsearchJobProvider.class);
        PrelertInitializationService initializationService =
                new PrelertInitializationService(Settings.EMPTY, threadPool, clusterService, jobProvider);

        ClusterState cs = ClusterState.builder(new ClusterName("_name"))
                .nodes(DiscoveryNodes.builder()
                        .add(new DiscoveryNode("_node_id", new LocalTransportAddress("_id"), Version.CURRENT))
                        .localNodeId("_node_id")
                        .masterNodeId("_node_id"))
                .metaData(MetaData.builder()
                        .put(IndexMetaData.builder(ElasticsearchJobProvider.PRELERT_USAGE_INDEX).settings(Settings.builder()
                                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0)
                                .put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT)
                        ))
                        .putCustom(PrelertMetadata.TYPE, new PrelertMetadata.Builder().build()))
                .build();
        initializationService.clusterChanged(new ClusterChangedEvent("_source", cs, cs));

        verify(clusterService, times(0)).submitStateUpdateTask(eq("install-prelert-metadata"), any());
        verify(jobProvider, times(0)).createUsageMeteringIndex(any());
    }

}
