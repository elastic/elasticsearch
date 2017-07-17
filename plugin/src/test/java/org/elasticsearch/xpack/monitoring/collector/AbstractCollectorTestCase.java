/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.monitoring.collector;

import java.util.Collection;
import java.util.concurrent.TimeUnit;

import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.xpack.monitoring.MonitoringSettings;
import org.elasticsearch.xpack.monitoring.test.MonitoringIntegTestCase;
import org.elasticsearch.xpack.security.InternalClient;

@ClusterScope(scope = ESIntegTestCase.Scope.SUITE, transportClientRatio = 0.0)
public abstract class AbstractCollectorTestCase extends MonitoringIntegTestCase {

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                .put(MonitoringSettings.INTERVAL.getKey(), "-1")
                .build();
    }

    public InternalClient securedClient() {
        return internalCluster().getInstance(InternalClient.class);
    }

    public InternalClient securedClient(String nodeId) {
        return internalCluster().getInstance(InternalClient.class, nodeId);
    }

    protected void assertCanCollect(Collector collector) {
        assertNotNull(collector);
        assertTrue("collector [" + collector.name() + "] should be able to collect data", collector.shouldCollect());
        Collection results = collector.collect();
        assertNotNull(results);
    }

    public void waitForNoBlocksOnNodes() throws Exception {
        assertBusy(() -> {
            for (String nodeId : internalCluster().getNodeNames()) {
                try {
                    waitForNoBlocksOnNode(nodeId);
                } catch (Exception e) {
                    fail("failed to wait for no blocks on node [" + nodeId + "]: " + e.getMessage());
                }
            }
        });
    }

    public void waitForNoBlocksOnNode(final String nodeId) throws Exception {
        assertBusy(() -> {
            ClusterBlocks clusterBlocks =
                    client(nodeId).admin().cluster().prepareState().setLocal(true).execute().actionGet().getState().blocks();
            assertTrue(clusterBlocks.global().isEmpty());
            assertTrue(clusterBlocks.indices().values().isEmpty());
        }, 30L, TimeUnit.SECONDS);
    }
}
