/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.support.master;

import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.disruption.NetworkDisruption;
import org.elasticsearch.test.transport.MockTransportService;

import java.util.Collection;
import java.util.HashSet;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;

import static org.hamcrest.Matchers.equalTo;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0, autoManageMasterNodes = false)
public class IndexingMasterFailoverIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        final HashSet<Class<? extends Plugin>> classes = new HashSet<>(super.nodePlugins());
        classes.add(MockTransportService.TestPlugin.class);
        return classes;
    }

    /**
     * Indexing operations which entail mapping changes require a blocking request to the master node to update the mapping.
     * If the master node is being disrupted or if it cannot commit cluster state changes, it needs to retry within timeout limits.
     * This retry logic is implemented in TransportMasterNodeAction and tested by the following master failover scenario.
     */
    public void testMasterFailoverDuringIndexingWithMappingChanges() throws Throwable {
        logger.info("--> start 4 nodes, 3 master, 1 data");

        internalCluster().setBootstrapMasterNodeIndex(2);

        internalCluster().startMasterOnlyNodes(3, Settings.EMPTY);

        String dataNode = internalCluster().startDataOnlyNode(Settings.EMPTY);

        logger.info("--> wait for all nodes to join the cluster");
        ensureStableCluster(4);

        // We index data with mapping changes into cluster and have master failover at same time
        client().admin()
            .indices()
            .prepareCreate("myindex")
            .setSettings(Settings.builder().put("index.number_of_shards", 1).put("index.number_of_replicas", 0))
            .get();
        ensureGreen("myindex");

        final CyclicBarrier barrier = new CyclicBarrier(2);

        Thread indexingThread = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    barrier.await();
                } catch (InterruptedException e) {
                    logger.warn("Barrier interrupted", e);
                    return;
                } catch (BrokenBarrierException e) {
                    logger.warn("Broken barrier", e);
                    return;
                }
                for (int i = 0; i < 10; i++) {
                    // index data with mapping changes
                    IndexResponse response = client(dataNode).prepareIndex("myindex").setSource("field_" + i, "val").get();
                    assertEquals(DocWriteResponse.Result.CREATED, response.getResult());
                }
            }
        });
        indexingThread.setName("indexingThread");
        indexingThread.start();

        barrier.await();

        // interrupt communication between master and other nodes in cluster
        NetworkDisruption partition = isolateMasterDisruption(NetworkDisruption.DISCONNECT);
        internalCluster().setDisruptionScheme(partition);

        logger.info("--> disrupting network");
        partition.startDisrupting();

        logger.info("--> waiting for new master to be elected");
        ensureStableCluster(3, dataNode);

        partition.stopDisrupting();
        logger.info("--> waiting to heal");
        ensureStableCluster(4);

        indexingThread.join();

        ensureGreen("myindex");
        refresh();
        assertThat(client().prepareSearch("myindex").get().getHits().getTotalHits().value, equalTo(10L));
    }

}
