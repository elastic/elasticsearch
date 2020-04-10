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
package org.elasticsearch.action.admin.cluster.state;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.coordination.ClusterBootstrapService;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.discovery.MasterNotDiscoveredException;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.TransportService;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.elasticsearch.cluster.routing.allocation.decider.EnableAllocationDecider.CLUSTER_ROUTING_REBALANCE_ENABLE_SETTING;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;

@ESIntegTestCase.ClusterScope(numDataNodes = 0, scope = ESIntegTestCase.Scope.TEST)
public class TransportClusterStateActionDisruptionIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singletonList(MockTransportService.TestPlugin.class);
    }

    public void testNonLocalRequestAlwaysFindsMaster() throws Exception {
        runRepeatedlyWhileChangingMaster(() -> {
            final ClusterStateRequestBuilder clusterStateRequestBuilder = client().admin().cluster().prepareState()
                .clear().setNodes(true).setMasterNodeTimeout("100ms");
            final ClusterStateResponse clusterStateResponse;
            try {
                clusterStateResponse = clusterStateRequestBuilder.get();
            } catch (MasterNotDiscoveredException e) {
                return; // ok, we hit the disconnected node
            }
            assertNotNull("should always contain a master node", clusterStateResponse.getState().nodes().getMasterNodeId());
        });
    }

    public void testLocalRequestAlwaysSucceeds() throws Exception {
        runRepeatedlyWhileChangingMaster(() -> {
            final String node = randomFrom(internalCluster().getNodeNames());
            final DiscoveryNodes discoveryNodes = client(node).admin().cluster().prepareState()
                .clear().setLocal(true).setNodes(true).setMasterNodeTimeout("100ms").get().getState().nodes();
            for (DiscoveryNode discoveryNode : discoveryNodes) {
                if (discoveryNode.getName().equals(node)) {
                    return;
                }
            }
            fail("nodes did not contain [" + node + "]: " + discoveryNodes);
        });
    }

    public void testNonLocalRequestAlwaysFindsMasterAndWaitsForMetadata() throws Exception {
        runRepeatedlyWhileChangingMaster(() -> {
            final String node = randomFrom(internalCluster().getNodeNames());
            final long metadataVersion
                = internalCluster().getInstance(ClusterService.class, node).getClusterApplierService().state().metadata().version();
            final long waitForMetadataVersion = randomLongBetween(Math.max(1, metadataVersion - 3), metadataVersion + 5);
            final ClusterStateRequestBuilder clusterStateRequestBuilder = client(node).admin().cluster().prepareState()
                .clear().setNodes(true).setMetadata(true)
                .setMasterNodeTimeout(TimeValue.timeValueMillis(100)).setWaitForTimeOut(TimeValue.timeValueMillis(100))
                .setWaitForMetadataVersion(waitForMetadataVersion);
            final ClusterStateResponse clusterStateResponse;
            try {
                clusterStateResponse = clusterStateRequestBuilder.get();
            } catch (MasterNotDiscoveredException e) {
                return; // ok, we hit the disconnected node
            }
            if (clusterStateResponse.isWaitForTimedOut() == false) {
                final ClusterState state = clusterStateResponse.getState();
                assertNotNull("should always contain a master node", state.nodes().getMasterNodeId());
                assertThat("waited for metadata version", state.metadata().version(), greaterThanOrEqualTo(waitForMetadataVersion));
            }
        });
    }

    public void testLocalRequestWaitsForMetadata() throws Exception {
        runRepeatedlyWhileChangingMaster(() -> {
            final String node = randomFrom(internalCluster().getNodeNames());
            final long metadataVersion
                = internalCluster().getInstance(ClusterService.class, node).getClusterApplierService().state().metadata().version();
            final long waitForMetadataVersion = randomLongBetween(Math.max(1, metadataVersion - 3), metadataVersion + 5);
            final ClusterStateResponse clusterStateResponse = client(node).admin().cluster()
                .prepareState().clear().setLocal(true).setMetadata(true).setWaitForMetadataVersion(waitForMetadataVersion)
                .setMasterNodeTimeout(TimeValue.timeValueMillis(100)).setWaitForTimeOut(TimeValue.timeValueMillis(100))
                .get();
            if (clusterStateResponse.isWaitForTimedOut() == false) {
                final Metadata metadata = clusterStateResponse.getState().metadata();
                assertThat("waited for metadata version " + waitForMetadataVersion + " with node " + node,
                    metadata.version(), greaterThanOrEqualTo(waitForMetadataVersion));
            }
        });
    }

    public void runRepeatedlyWhileChangingMaster(Runnable runnable) throws Exception {
        internalCluster().startNodes(3);

        assertBusy(() -> assertThat(client().admin().cluster().prepareState().clear().setMetadata(true)
            .get().getState().getLastCommittedConfiguration().getNodeIds().stream()
            .filter(n -> ClusterBootstrapService.isBootstrapPlaceholder(n) == false).collect(Collectors.toSet()), hasSize(3)));

        final String masterName = internalCluster().getMasterName();

        final AtomicBoolean shutdown = new AtomicBoolean();
        final Thread assertingThread = new Thread(() -> {
            while (shutdown.get() == false) {
                runnable.run();
            }
        }, "asserting thread");

        final Thread updatingThread = new Thread(() -> {
            String value = "none";
            while (shutdown.get() == false) {
                value = "none".equals(value) ? "all" : "none";
                final String nonMasterNode = randomValueOtherThan(masterName, () -> randomFrom(internalCluster().getNodeNames()));
                assertAcked(client(nonMasterNode).admin().cluster().prepareUpdateSettings().setPersistentSettings(
                    Settings.builder().put(CLUSTER_ROUTING_REBALANCE_ENABLE_SETTING.getKey(), value)));
            }
        }, "updating thread");

        final List<MockTransportService> mockTransportServices
            = StreamSupport.stream(internalCluster().getInstances(TransportService.class).spliterator(), false)
            .map(ts -> (MockTransportService) ts).collect(Collectors.toList());

        assertingThread.start();
        updatingThread.start();

        final MockTransportService masterTransportService
            = (MockTransportService) internalCluster().getInstance(TransportService.class, masterName);

        for (MockTransportService mockTransportService : mockTransportServices) {
            if (masterTransportService != mockTransportService) {
                masterTransportService.addFailToSendNoConnectRule(mockTransportService);
                mockTransportService.addFailToSendNoConnectRule(masterTransportService);
            }
        }

        assertBusy(() -> {
            final String nonMasterNode = randomValueOtherThan(masterName, () -> randomFrom(internalCluster().getNodeNames()));
            final String claimedMasterName = internalCluster().getMasterName(nonMasterNode);
            assertThat(claimedMasterName, not(equalTo(masterName)));
        });

        shutdown.set(true);
        assertingThread.join();
        updatingThread.join();
        internalCluster().close();
    }

}
