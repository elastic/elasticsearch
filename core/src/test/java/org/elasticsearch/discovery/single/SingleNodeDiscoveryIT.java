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

package org.elasticsearch.discovery.single;

import org.apache.lucene.util.IOUtils;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.discovery.zen.UnicastHostsProvider;
import org.elasticsearch.discovery.zen.UnicastZenPing;
import org.elasticsearch.discovery.zen.ZenPing;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.test.NodeConfigurationSource;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Stack;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.function.Function;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

@ESIntegTestCase.ClusterScope(
        scope = ESIntegTestCase.Scope.TEST,
        numDataNodes = 1,
        numClientNodes = 0,
        supportsDedicatedMasters = false,
        autoMinMasterNodes = false)
public class SingleNodeDiscoveryIT extends ESIntegTestCase {

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings
                .builder()
                .put(super.nodeSettings(nodeOrdinal))
                .put("discovery.type", "single-node")
                .put("transport.tcp.port", "0")
                .build();
    }

    public void testDoesNotRespondToZenPings() throws Exception {
        final Settings settings =
                Settings.builder().put("cluster.name", internalCluster().getClusterName()).build();
        final Version version = Version.CURRENT;
        final Stack<Closeable> closeables = new Stack<>();
        final TestThreadPool threadPool = new TestThreadPool(getClass().getName());
        try {
            final MockTransportService pingTransport =
                    MockTransportService.createNewService(settings, version, threadPool, null);
            pingTransport.start();
            closeables.push(pingTransport);
            final TransportService nodeTransport =
                    internalCluster().getInstance(TransportService.class);
            // try to ping the single node directly
            final UnicastHostsProvider provider =
                    () -> Collections.singletonList(nodeTransport.getLocalNode());
            final CountDownLatch latch = new CountDownLatch(1);
            final DiscoveryNodes nodes = DiscoveryNodes.builder()
                    .add(nodeTransport.getLocalNode())
                    .add(pingTransport.getLocalNode())
                    .localNodeId(pingTransport.getLocalNode().getId())
                    .build();
            final ClusterName clusterName = new ClusterName(internalCluster().getClusterName());
            final ClusterState state = ClusterState.builder(clusterName).nodes(nodes).build();
            final UnicastZenPing unicastZenPing =
                new UnicastZenPing(settings, threadPool, pingTransport, provider, () -> state) {
                    @Override
                    protected void finishPingingRound(PingingRound pingingRound) {
                        latch.countDown();
                        super.finishPingingRound(pingingRound);
                    }
                };
            unicastZenPing.start();
            closeables.push(unicastZenPing);
            final CompletableFuture<ZenPing.PingCollection> responses = new CompletableFuture<>();
            unicastZenPing.ping(responses::complete, TimeValue.timeValueSeconds(3));
            latch.await();
            responses.get();
            assertThat(responses.get().size(), equalTo(0));
        } finally {
            while (!closeables.isEmpty()) {
                IOUtils.closeWhileHandlingException(closeables.pop());
            }
            terminate(threadPool);
        }
    }

    public void testSingleNodesDoNotDiscoverEachOther() throws IOException, InterruptedException {
        final TransportService service = internalCluster().getInstance(TransportService.class);
        final int port = service.boundAddress().publishAddress().getPort();
        final NodeConfigurationSource configurationSource = new NodeConfigurationSource() {
            @Override
            public Settings nodeSettings(int nodeOrdinal) {
                return Settings
                        .builder()
                        .put("discovery.type", "single-node")
                        .put("http.enabled", false)
                        .put("transport.type", getTestTransportType())
                        /*
                         * We align the port ranges of the two as then with zen discovery these two
                         * nodes would find each other.
                         */
                        .put("transport.tcp.port", port + "-" + (port + 5 - 1))
                        .build();
            }

            @Override
            public Path nodeConfigPath(int nodeOrdinal) {
                return null;
            }
        };
        try (InternalTestCluster other =
                new InternalTestCluster(
                        randomLong(),
                        createTempDir(),
                        false,
                        false,
                        1,
                        1,
                        internalCluster().getClusterName(),
                        configurationSource,
                        0,
                        false,
                        "other",
                        Collections.singletonList(getTestTransportPlugin()),
                        Function.identity())) {
            other.beforeTest(random(), 0);
            final ClusterState first = internalCluster().getInstance(ClusterService.class).state();
            final ClusterState second = other.getInstance(ClusterService.class).state();
            assertThat(first.nodes().getSize(), equalTo(1));
            assertThat(second.nodes().getSize(), equalTo(1));
            assertThat(
                    first.nodes().getMasterNodeId(),
                    not(equalTo(second.nodes().getMasterNodeId())));
            assertThat(
                    first.metaData().clusterUUID(),
                    not(equalTo(second.metaData().clusterUUID())));
        }
    }

}
