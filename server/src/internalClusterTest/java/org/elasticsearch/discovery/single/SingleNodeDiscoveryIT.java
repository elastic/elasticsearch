/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.discovery.single;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.LogEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.coordination.JoinHelper;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.Node;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.test.MockHttpTransport;
import org.elasticsearch.test.MockLogAppender;
import org.elasticsearch.test.NodeConfigurationSource;
import org.elasticsearch.transport.RemoteTransportException;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.function.Function;

import static org.elasticsearch.discovery.DiscoveryModule.DISCOVERY_TYPE_SETTING;
import static org.elasticsearch.discovery.DiscoveryModule.MULTI_NODE_DISCOVERY_TYPE;
import static org.elasticsearch.discovery.DiscoveryModule.SINGLE_NODE_DISCOVERY_TYPE;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

@ESIntegTestCase.ClusterScope(
    scope = ESIntegTestCase.Scope.TEST,
    numDataNodes = 1,
    numClientNodes = 0,
    supportsDedicatedMasters = false,
    autoManageMasterNodes = false
)
public class SingleNodeDiscoveryIT extends ESIntegTestCase {

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(DISCOVERY_TYPE_SETTING.getKey(), SINGLE_NODE_DISCOVERY_TYPE)
            .put("transport.port", getPortRange())
            .build();
    }

    public void testSingleNodesDoNotDiscoverEachOther() throws IOException, InterruptedException {
        final TransportService service = internalCluster().getInstance(TransportService.class);
        final int port = service.boundAddress().publishAddress().getPort();
        final NodeConfigurationSource configurationSource = new NodeConfigurationSource() {
            @Override
            public Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
                return Settings.builder()
                    .put(DISCOVERY_TYPE_SETTING.getKey(), SINGLE_NODE_DISCOVERY_TYPE)
                    .put("transport.type", getTestTransportType())
                    /*
                     * We align the port ranges of the two as then with zen discovery these two
                     * nodes would find each other.
                     */
                    .put("transport.port", port + "-" + (port + 5 - 1))
                    .build();
            }

            @Override
            public Path nodeConfigPath(int nodeOrdinal) {
                return null;
            }
        };
        try (
            InternalTestCluster other = new InternalTestCluster(
                randomLong(),
                createTempDir(),
                false,
                false,
                1,
                1,
                internalCluster().getClusterName(),
                configurationSource,
                0,
                "other",
                Arrays.asList(getTestTransportPlugin(), MockHttpTransport.TestPlugin.class),
                Function.identity()
            )
        ) {
            other.beforeTest(random());
            final ClusterState first = internalCluster().getInstance(ClusterService.class).state();
            final ClusterState second = other.getInstance(ClusterService.class).state();
            assertThat(first.nodes().getSize(), equalTo(1));
            assertThat(second.nodes().getSize(), equalTo(1));
            assertThat(first.nodes().getMasterNodeId(), not(equalTo(second.nodes().getMasterNodeId())));
            assertThat(first.metadata().clusterUUID(), not(equalTo(second.metadata().clusterUUID())));
        }
    }

    public void testCannotJoinNodeWithSingleNodeDiscovery() throws Exception {
        MockLogAppender mockAppender = new MockLogAppender();
        mockAppender.addExpectation(
            new MockLogAppender.SeenEventExpectation("test", JoinHelper.class.getCanonicalName(), Level.INFO, "failed to join") {

                @Override
                public boolean innerMatch(final LogEvent event) {
                    return event.getThrown() != null
                        && event.getThrown().getClass() == RemoteTransportException.class
                        && event.getThrown().getCause() != null
                        && event.getThrown().getCause().getClass() == IllegalStateException.class
                        && event.getThrown()
                            .getCause()
                            .getMessage()
                            .contains("cannot join node with [discovery.type] set to [single-node]");
                }
            }
        );
        final TransportService service = internalCluster().getInstance(TransportService.class);
        final int port = service.boundAddress().publishAddress().getPort();
        final NodeConfigurationSource configurationSource = new NodeConfigurationSource() {
            @Override
            public Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
                return Settings.builder()
                    .put(DISCOVERY_TYPE_SETTING.getKey(), MULTI_NODE_DISCOVERY_TYPE)
                    .put("transport.type", getTestTransportType())
                    .put(Node.INITIAL_STATE_TIMEOUT_SETTING.getKey(), "0s")
                    /*
                     * We align the port ranges of the two as then with zen discovery these two
                     * nodes would find each other.
                     */
                    .put("transport.port", port + "-" + (port + 5 - 1))
                    .build();
            }

            @Override
            public Path nodeConfigPath(int nodeOrdinal) {
                return null;
            }
        };
        try (
            InternalTestCluster other = new InternalTestCluster(
                randomLong(),
                createTempDir(),
                false,
                false,
                1,
                1,
                internalCluster().getClusterName(),
                configurationSource,
                0,
                "other",
                Arrays.asList(getTestTransportPlugin(), MockHttpTransport.TestPlugin.class),
                Function.identity()
            );
            var ignored = mockAppender.capturing(JoinHelper.class)
        ) {
            other.beforeTest(random());
            final ClusterState first = internalCluster().getInstance(ClusterService.class).state();
            assertThat(first.nodes().getSize(), equalTo(1));
            assertBusy(mockAppender::assertAllExpectationsMatched);
        }
    }

    public void testStatePersistence() throws Exception {
        createIndex("test");
        internalCluster().fullRestart();
        assertTrue(indexExists("test"));
    }
}
