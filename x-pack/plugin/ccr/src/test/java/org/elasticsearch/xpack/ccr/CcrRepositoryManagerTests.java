/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ccr;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.RepositoryMissingException;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.ccr.repository.CcrRepository;

import java.util.Collections;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CcrRepositoryManagerTests extends ESTestCase {

    private RepositoriesService repositoriesService;
    private ClusterSettings clusterSettings;

    @Override
    public void setUp() throws Exception {
        super.setUp();

        Settings settings = Settings.EMPTY;
        ThreadPool threadPool = mock(ThreadPool.class);
        TransportService transportService = new TransportService(settings, mock(Transport.class), threadPool,
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            boundAddress -> DiscoveryNode.createLocal(settings, boundAddress.publishAddress(), UUIDs.randomBase64UUID()), null,
            Collections.emptySet());
        ClusterService clusterService = mock(ClusterService.class);
        clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);
        repositoriesService = new RepositoriesService(settings, clusterService,
            transportService, null, threadPool);
        CcrRepositoryManager repositoryManager = new CcrRepositoryManager(settings, clusterService, new NodeClient(Settings.EMPTY, threadPool));
    }

    public void testRepositoryIsRegistered() {
        String clusterName = "cluster_1";
        clusterSettings.applySettings(Settings.builder().put("cluster.remote." + clusterName + ".seeds", "192.168.0.1:8080").build());
        CcrRepository repository = (CcrRepository) repositoriesService.repository(clusterName);
        assertEquals(CcrRepository.TYPE, repository.getMetadata().type());
        assertEquals(clusterName, repository.getMetadata().name());
    }

    public void testRepositoryIsUnregistered() {
        String clusterName = "cluster_1";
        clusterSettings.applySettings(Settings.builder().put("cluster.remote." + clusterName + ".seeds", "192.168.0.1:8080").build());
        // Will throw is it does not exist
        repositoriesService.repository(clusterName);

        clusterSettings.applySettings(Settings.builder().put("cluster.remote.cluster_1.seeds", "").build());
        expectThrows(RepositoryMissingException.class, () -> repositoriesService.repository("cluster_1"));
    }
}
