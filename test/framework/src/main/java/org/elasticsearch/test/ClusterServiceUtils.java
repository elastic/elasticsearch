/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.test;

import org.apache.logging.log4j.core.util.Throwables;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateTaskListener;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.NodeConnectionsService;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterApplier;
import org.elasticsearch.cluster.service.ClusterApplierService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.MasterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.discovery.Discovery.AckListener;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Collections;
import java.util.EnumSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;

import static junit.framework.TestCase.fail;

public class ClusterServiceUtils {

    public static MasterService createMasterService(ThreadPool threadPool, ClusterState initialClusterState) {
        MasterService masterService = new MasterService(Settings.EMPTY, threadPool);
        AtomicReference<ClusterState> clusterStateRef = new AtomicReference<>(initialClusterState);
        masterService.setClusterStatePublisher((event, ackListener) -> clusterStateRef.set(event.state()));
        masterService.setClusterStateSupplier(clusterStateRef::get);
        masterService.start();
        return masterService;
    }

    public static MasterService createMasterService(ThreadPool threadPool, DiscoveryNode localNode) {
        ClusterState initialClusterState = ClusterState.builder(new ClusterName(ClusterServiceUtils.class.getSimpleName()))
            .nodes(DiscoveryNodes.builder()
                .add(localNode)
                .localNodeId(localNode.getId())
                .masterNodeId(localNode.getId()))
            .blocks(ClusterBlocks.EMPTY_CLUSTER_BLOCK).build();
        return createMasterService(threadPool, initialClusterState);
    }

    public static void setState(ClusterApplierService executor, ClusterState clusterState) {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Exception> exception = new AtomicReference<>();
        executor.onNewClusterState("test setting state",
            () -> ClusterState.builder(clusterState).version(clusterState.version() + 1).build(), new ClusterStateTaskListener() {
                @Override
                public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                    latch.countDown();
                }

                @Override
                public void onFailure(String source, Exception e) {
                    exception.set(e);
                    latch.countDown();
                }
            });
        try {
            latch.await();
            if (exception.get() != null) {
                Throwables.rethrow(exception.get());
            }
        } catch (InterruptedException e) {
            throw new ElasticsearchException("unexpected exception", e);
        }
    }

    public static void setState(MasterService executor, ClusterState clusterState) {
        CountDownLatch latch = new CountDownLatch(1);
        executor.submitStateUpdateTask("test setting state", new ClusterStateUpdateTask() {
            @Override
            public ClusterState execute(ClusterState currentState) throws Exception {
                // make sure we increment versions as listener may depend on it for change
                return ClusterState.builder(clusterState).build();
            }

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                latch.countDown();
            }

            @Override
            public void onFailure(String source, Exception e) {
                fail("unexpected exception" + e);
            }
        });
        try {
            latch.await();
        } catch (InterruptedException e) {
            throw new ElasticsearchException("unexpected interruption", e);
        }
    }

    public static ClusterService createClusterService(ThreadPool threadPool) {
        DiscoveryNode discoveryNode = new DiscoveryNode("node", ESTestCase.buildNewFakeTransportAddress(), Collections.emptyMap(),
                EnumSet.allOf(DiscoveryNode.Role.class), Version.CURRENT);
        return createClusterService(threadPool, discoveryNode);
    }

    public static ClusterService createClusterService(ThreadPool threadPool, DiscoveryNode localNode) {
        return createClusterService(threadPool, localNode, new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS));
    }

    public static ClusterService createClusterService(ThreadPool threadPool, DiscoveryNode localNode, ClusterSettings clusterSettings) {
        ClusterService clusterService = new ClusterService(Settings.builder().put("cluster.name", "ClusterServiceTests").build(),
            clusterSettings, threadPool, Collections.emptyMap());
        clusterService.setNodeConnectionsService(new NodeConnectionsService(Settings.EMPTY, null, null) {
            @Override
            public void connectToNodes(DiscoveryNodes discoveryNodes) {
                // skip
            }

            @Override
            public void disconnectFromNodesExcept(DiscoveryNodes nodesToKeep) {
                // skip
            }
        });
        ClusterState initialClusterState = ClusterState.builder(new ClusterName(ClusterServiceUtils.class.getSimpleName()))
            .nodes(DiscoveryNodes.builder()
                .add(localNode)
                .localNodeId(localNode.getId())
                .masterNodeId(localNode.getId()))
            .blocks(ClusterBlocks.EMPTY_CLUSTER_BLOCK).build();
        clusterService.getClusterApplierService().setInitialState(initialClusterState);
        clusterService.getMasterService().setClusterStatePublisher(
            createClusterStatePublisher(clusterService.getClusterApplierService()));
        clusterService.getMasterService().setClusterStateSupplier(clusterService.getClusterApplierService()::state);
        clusterService.start();
        return clusterService;
    }

    public static BiConsumer<ClusterChangedEvent, AckListener> createClusterStatePublisher(ClusterApplier clusterApplier) {
        return (event, ackListener) -> {
            CountDownLatch latch = new CountDownLatch(1);
            AtomicReference<Exception> ex = new AtomicReference<>();
            clusterApplier.onNewClusterState("mock_publish_to_self[" + event.source() + "]", () -> event.state(),
                new ClusterStateTaskListener() {
                    @Override
                    public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                        latch.countDown();
                    }

                    @Override
                    public void onFailure(String source, Exception e) {
                        ex.set(e);
                        latch.countDown();
                    }
                }
            );
            try {
                latch.await();
            } catch (InterruptedException e) {
                Throwables.rethrow(e);
            }
            if (ex.get() != null) {
                Throwables.rethrow(ex.get());
            }
        };
    }

    public static ClusterService createClusterService(ClusterState initialState, ThreadPool threadPool) {
        ClusterService clusterService = createClusterService(threadPool);
        setState(clusterService, initialState);
        return clusterService;
    }

    public static void setState(ClusterService clusterService, ClusterState.Builder clusterStateBuilder) {
        setState(clusterService, clusterStateBuilder.build());
    }

    /**
     * Sets the state on the cluster applier service
     */
    public static void setState(ClusterService clusterService, ClusterState clusterState) {
        setState(clusterService.getClusterApplierService(), clusterState);
    }
}
