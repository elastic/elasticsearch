/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.cluster.coordination;

import org.apache.logging.log4j.Level;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.discovery.DiscoveryModule;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLogAppender;
import org.elasticsearch.test.transport.MockTransport;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportService;
import org.junit.Before;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptySet;
import static java.util.Collections.singletonList;
import static org.elasticsearch.cluster.coordination.ClusterBootstrapService.BOOTSTRAP_PLACEHOLDER_PREFIX;
import static org.elasticsearch.cluster.coordination.ClusterBootstrapService.INITIAL_MASTER_NODES_SETTING;
import static org.elasticsearch.cluster.coordination.ClusterBootstrapService.UNCONFIGURED_BOOTSTRAP_TIMEOUT_SETTING;
import static org.elasticsearch.common.settings.Settings.builder;
import static org.elasticsearch.discovery.DiscoveryModule.DISCOVERY_SEED_PROVIDERS_SETTING;
import static org.elasticsearch.discovery.SettingsBasedSeedHostsProvider.DISCOVERY_SEED_HOSTS_SETTING;
import static org.elasticsearch.node.Node.NODE_NAME_SETTING;
import static org.elasticsearch.test.NodeRoles.nonMasterNode;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.startsWith;

public class ClusterBootstrapServiceTests extends ESTestCase {

    private DiscoveryNode localNode, otherNode1, otherNode2;
    private DeterministicTaskQueue deterministicTaskQueue;
    private TransportService transportService;

    @Before
    public void createServices() {
        localNode = newDiscoveryNode("local");
        otherNode1 = newDiscoveryNode("other1");
        otherNode2 = newDiscoveryNode("other2");

        deterministicTaskQueue = new DeterministicTaskQueue();

        final MockTransport transport = new MockTransport() {
            @Override
            protected void onSendRequest(long requestId, String action, TransportRequest request, DiscoveryNode node) {
                throw new AssertionError("unexpected " + action);
            }
        };

        transportService = transport.createTransportService(
            Settings.EMPTY,
            deterministicTaskQueue.getThreadPool(),
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            boundTransportAddress -> localNode,
            null,
            emptySet()
        );
    }

    private DiscoveryNode newDiscoveryNode(String nodeName) {
        return DiscoveryNodeUtils.builder(randomAlphaOfLength(10)).name(nodeName).roles(Set.of(DiscoveryNodeRole.MASTER_ROLE)).build();
    }

    public void testBootstrapsAutomaticallyWithDefaultConfiguration() {
        final Settings.Builder settings = Settings.builder();
        final long timeout;
        if (randomBoolean()) {
            timeout = UNCONFIGURED_BOOTSTRAP_TIMEOUT_SETTING.get(Settings.EMPTY).millis();
        } else {
            timeout = randomLongBetween(1, 10000);
            settings.put(UNCONFIGURED_BOOTSTRAP_TIMEOUT_SETTING.getKey(), timeout + "ms");
        }

        final AtomicReference<Supplier<Iterable<DiscoveryNode>>> discoveredNodesSupplier = new AtomicReference<>(() -> {
            throw new AssertionError("should not be called yet");
        });

        final AtomicBoolean bootstrapped = new AtomicBoolean();
        ClusterBootstrapService clusterBootstrapService = new ClusterBootstrapService(
            settings.build(),
            transportService,
            () -> discoveredNodesSupplier.get().get(),
            () -> false,
            vc -> {
                assertTrue(bootstrapped.compareAndSet(false, true));
                assertThat(
                    vc.getNodeIds(),
                    equalTo(Stream.of(localNode, otherNode1, otherNode2).map(DiscoveryNode::getId).collect(Collectors.toSet()))
                );
                assertThat(deterministicTaskQueue.getCurrentTimeMillis(), greaterThanOrEqualTo(timeout));
            }
        );

        deterministicTaskQueue.scheduleAt(
            timeout - 1,
            () -> discoveredNodesSupplier.set(() -> Stream.of(localNode, otherNode1, otherNode2).collect(Collectors.toSet()))
        );

        transportService.start();
        clusterBootstrapService.scheduleUnconfiguredBootstrap();
        deterministicTaskQueue.runAllTasksInTimeOrder();
        assertTrue(bootstrapped.get());
    }

    public void testDoesNothingByDefaultIfHostsProviderConfigured() {
        testDoesNothingWithSettings(builder().putList(DISCOVERY_SEED_PROVIDERS_SETTING.getKey()));
    }

    public void testDoesNothingByDefaultIfSeedHostsConfigured() {
        testDoesNothingWithSettings(builder().putList(DISCOVERY_SEED_HOSTS_SETTING.getKey()));
    }

    public void testDoesNothingByDefaultIfMasterNodesConfigured() {
        testDoesNothingWithSettings(builder().putList(INITIAL_MASTER_NODES_SETTING.getKey()));
    }

    public void testDoesNothingByDefaultOnMasterIneligibleNodes() {
        localNode = DiscoveryNodeUtils.builder(randomAlphaOfLength(10)).name("local").roles(emptySet()).build();
        testDoesNothingWithSettings(Settings.builder());
    }

    private void testDoesNothingWithSettings(Settings.Builder builder) {
        ClusterBootstrapService clusterBootstrapService = new ClusterBootstrapService(builder.build(), transportService, () -> {
            throw new AssertionError("should not be called");
        }, () -> false, vc -> { throw new AssertionError("should not be called"); });
        transportService.start();
        clusterBootstrapService.scheduleUnconfiguredBootstrap();
        deterministicTaskQueue.runAllTasks();
    }

    public void testThrowsExceptionOnDuplicates() {
        final IllegalArgumentException illegalArgumentException = expectThrows(
            IllegalArgumentException.class,
            () -> new ClusterBootstrapService(
                builder().putList(INITIAL_MASTER_NODES_SETTING.getKey(), "duplicate-requirement", "duplicate-requirement").build(),
                transportService,
                Collections::emptyList,
                () -> false,
                vc -> {
                    throw new AssertionError("should not be called");
                }
            )
        );

        assertThat(illegalArgumentException.getMessage(), containsString(INITIAL_MASTER_NODES_SETTING.getKey()));
        assertThat(illegalArgumentException.getMessage(), containsString("duplicate-requirement"));
    }

    public void testBootstrapsOnDiscoveryOfAllRequiredNodes() {
        final AtomicBoolean bootstrapped = new AtomicBoolean();

        ClusterBootstrapService clusterBootstrapService = new ClusterBootstrapService(
            Settings.builder()
                .putList(INITIAL_MASTER_NODES_SETTING.getKey(), localNode.getName(), otherNode1.getName(), otherNode2.getName())
                .build(),
            transportService,
            () -> Stream.of(otherNode1, otherNode2).toList(),
            () -> false,
            vc -> {
                assertTrue(bootstrapped.compareAndSet(false, true));
                assertThat(vc.getNodeIds(), containsInAnyOrder(localNode.getId(), otherNode1.getId(), otherNode2.getId()));
                assertThat(vc.getNodeIds(), not(hasItem(containsString("placeholder"))));
            }
        );

        transportService.start();
        clusterBootstrapService.onFoundPeersUpdated();
        deterministicTaskQueue.runAllTasks();
        assertTrue(bootstrapped.get());

        bootstrapped.set(false);
        clusterBootstrapService.onFoundPeersUpdated();
        deterministicTaskQueue.runAllTasks();
        assertFalse(bootstrapped.get()); // should only bootstrap once
    }

    public void testBootstrapsOnDiscoveryOfTwoOfThreeRequiredNodes() {
        final AtomicBoolean bootstrapped = new AtomicBoolean();

        ClusterBootstrapService clusterBootstrapService = new ClusterBootstrapService(
            Settings.builder()
                .putList(INITIAL_MASTER_NODES_SETTING.getKey(), localNode.getName(), otherNode1.getName(), otherNode2.getName())
                .build(),
            transportService,
            () -> singletonList(otherNode1),
            () -> false,
            vc -> {
                assertTrue(bootstrapped.compareAndSet(false, true));
                assertThat(vc.getNodeIds(), hasSize(3));
                assertThat(vc.getNodeIds(), hasItem(localNode.getId()));
                assertThat(vc.getNodeIds(), hasItem(otherNode1.getId()));
                assertThat(vc.getNodeIds(), hasItem(allOf(startsWith(BOOTSTRAP_PLACEHOLDER_PREFIX), containsString(otherNode2.getName()))));
                assertTrue(vc.hasQuorum(Stream.of(localNode, otherNode1).map(DiscoveryNode::getId).toList()));
                assertFalse(vc.hasQuorum(singletonList(localNode.getId())));
                assertFalse(vc.hasQuorum(singletonList(otherNode1.getId())));
            }
        );

        transportService.start();
        clusterBootstrapService.onFoundPeersUpdated();
        deterministicTaskQueue.runAllTasks();
        assertTrue(bootstrapped.get());

        bootstrapped.set(false);
        clusterBootstrapService.onFoundPeersUpdated();
        deterministicTaskQueue.runAllTasks();
        assertFalse(bootstrapped.get()); // should only bootstrap once
    }

    public void testBootstrapsOnDiscoveryOfThreeOfFiveRequiredNodes() {
        final AtomicBoolean bootstrapped = new AtomicBoolean();

        ClusterBootstrapService clusterBootstrapService = new ClusterBootstrapService(
            Settings.builder()
                .putList(
                    INITIAL_MASTER_NODES_SETTING.getKey(),
                    localNode.getName(),
                    otherNode1.getName(),
                    otherNode2.getName(),
                    "missing-node-1",
                    "missing-node-2"
                )
                .build(),
            transportService,
            () -> Stream.of(otherNode1, otherNode2).toList(),
            () -> false,
            vc -> {
                assertTrue(bootstrapped.compareAndSet(false, true));
                assertThat(vc.getNodeIds(), hasSize(5));
                assertThat(vc.getNodeIds(), hasItem(localNode.getId()));
                assertThat(vc.getNodeIds(), hasItem(otherNode1.getId()));
                assertThat(vc.getNodeIds(), hasItem(otherNode2.getId()));

                final List<String> placeholders = vc.getNodeIds().stream().filter(ClusterBootstrapService::isBootstrapPlaceholder).toList();
                assertThat(placeholders.size(), equalTo(2));
                assertNotEquals(placeholders.get(0), placeholders.get(1));
                assertThat(placeholders, hasItem(containsString("missing-node-1")));
                assertThat(placeholders, hasItem(containsString("missing-node-2")));

                assertTrue(vc.hasQuorum(Stream.of(localNode, otherNode1, otherNode2).map(DiscoveryNode::getId).toList()));
                assertFalse(vc.hasQuorum(Stream.of(localNode, otherNode1).map(DiscoveryNode::getId).toList()));
                assertFalse(vc.hasQuorum(Stream.of(localNode, otherNode1).map(DiscoveryNode::getId).toList()));
            }
        );

        transportService.start();
        clusterBootstrapService.onFoundPeersUpdated();
        deterministicTaskQueue.runAllTasks();
        assertTrue(bootstrapped.get());

        bootstrapped.set(false);
        clusterBootstrapService.onFoundPeersUpdated();
        deterministicTaskQueue.runAllTasks();
        assertFalse(bootstrapped.get()); // should only bootstrap once
    }

    public void testDoesNotBootstrapIfNoNodesDiscovered() {
        ClusterBootstrapService clusterBootstrapService = new ClusterBootstrapService(
            Settings.builder()
                .putList(INITIAL_MASTER_NODES_SETTING.getKey(), localNode.getName(), otherNode1.getName(), otherNode2.getName())
                .build(),
            transportService,
            Collections::emptyList,
            () -> true,
            vc -> {
                throw new AssertionError("should not be called");
            }
        );

        transportService.start();
        clusterBootstrapService.onFoundPeersUpdated();
        deterministicTaskQueue.runAllTasks();
    }

    public void testDoesNotBootstrapIfTwoOfFiveNodesDiscovered() {
        ClusterBootstrapService clusterBootstrapService = new ClusterBootstrapService(
            Settings.builder()
                .putList(
                    INITIAL_MASTER_NODES_SETTING.getKey(),
                    localNode.getName(),
                    otherNode1.getName(),
                    otherNode2.getName(),
                    "not-a-node-1",
                    "not-a-node-2"
                )
                .build(),
            transportService,
            () -> Stream.of(otherNode1).toList(),
            () -> false,
            vc -> {
                throw new AssertionError("should not be called");
            }
        );

        transportService.start();
        clusterBootstrapService.onFoundPeersUpdated();
        deterministicTaskQueue.runAllTasks();
    }

    public void testDoesNotBootstrapIfThreeOfSixNodesDiscovered() {
        ClusterBootstrapService clusterBootstrapService = new ClusterBootstrapService(
            Settings.builder()
                .putList(
                    INITIAL_MASTER_NODES_SETTING.getKey(),
                    localNode.getName(),
                    otherNode1.getName(),
                    otherNode2.getName(),
                    "not-a-node-1",
                    "not-a-node-2",
                    "not-a-node-3"
                )
                .build(),
            transportService,
            () -> Stream.of(otherNode1, otherNode2).toList(),
            () -> false,
            vc -> {
                throw new AssertionError("should not be called");
            }
        );

        transportService.start();
        clusterBootstrapService.onFoundPeersUpdated();
        deterministicTaskQueue.runAllTasks();
    }

    public void testDoesNotBootstrapIfAlreadyBootstrapped() {
        ClusterBootstrapService clusterBootstrapService = new ClusterBootstrapService(
            Settings.builder()
                .putList(INITIAL_MASTER_NODES_SETTING.getKey(), localNode.getName(), otherNode1.getName(), otherNode2.getName())
                .build(),
            transportService,
            () -> Stream.of(otherNode1, otherNode2).toList(),
            () -> true,
            vc -> {
                throw new AssertionError("should not be called");
            }
        );

        transportService.start();
        clusterBootstrapService.onFoundPeersUpdated();
        deterministicTaskQueue.runAllTasks();
    }

    public void testDoesNotBootstrapsOnNonMasterNode() {
        localNode = DiscoveryNodeUtils.builder(randomAlphaOfLength(10)).name("local").roles(emptySet()).build();
        ClusterBootstrapService clusterBootstrapService = new ClusterBootstrapService(
            Settings.builder()
                .putList(INITIAL_MASTER_NODES_SETTING.getKey(), localNode.getName(), otherNode1.getName(), otherNode2.getName())
                .build(),
            transportService,
            () -> Stream.of(localNode, otherNode1, otherNode2).toList(),
            () -> false,
            vc -> {
                throw new AssertionError("should not be called");
            }
        );
        transportService.start();
        clusterBootstrapService.onFoundPeersUpdated();
        deterministicTaskQueue.runAllTasks();
    }

    public void testDoesNotBootstrapsIfLocalNodeNotInInitialMasterNodes() {
        ClusterBootstrapService clusterBootstrapService = new ClusterBootstrapService(
            Settings.builder().putList(INITIAL_MASTER_NODES_SETTING.getKey(), otherNode1.getName(), otherNode2.getName()).build(),
            transportService,
            () -> Stream.of(localNode, otherNode1, otherNode2).toList(),
            () -> false,
            vc -> {
                throw new AssertionError("should not be called");
            }
        );
        transportService.start();
        clusterBootstrapService.onFoundPeersUpdated();
        deterministicTaskQueue.runAllTasks();
    }

    public void testDoesNotBootstrapsIfNotConfigured() {
        ClusterBootstrapService clusterBootstrapService = new ClusterBootstrapService(
            Settings.builder().putList(INITIAL_MASTER_NODES_SETTING.getKey()).build(),
            transportService,
            () -> Stream.of(localNode, otherNode1, otherNode2).toList(),
            () -> false,
            vc -> {
                throw new AssertionError("should not be called");
            }
        );
        transportService.start();
        clusterBootstrapService.scheduleUnconfiguredBootstrap();
        clusterBootstrapService.onFoundPeersUpdated();
        deterministicTaskQueue.runAllTasks();
    }

    public void testRetriesBootstrappingOnException() {
        final AtomicLong bootstrappingAttempts = new AtomicLong();
        ClusterBootstrapService clusterBootstrapService = new ClusterBootstrapService(
            Settings.builder()
                .putList(INITIAL_MASTER_NODES_SETTING.getKey(), localNode.getName(), otherNode1.getName(), otherNode2.getName())
                .build(),
            transportService,
            () -> Stream.of(otherNode1, otherNode2).toList(),
            () -> false,
            vc -> {
                bootstrappingAttempts.incrementAndGet();
                if (bootstrappingAttempts.get() < 5L) {
                    throw new ElasticsearchException("test");
                }
            }
        );

        transportService.start();
        clusterBootstrapService.onFoundPeersUpdated();
        deterministicTaskQueue.runAllTasks();
        assertThat(bootstrappingAttempts.get(), greaterThanOrEqualTo(5L));
        assertThat(deterministicTaskQueue.getCurrentTimeMillis(), greaterThanOrEqualTo(40000L));
    }

    public void testCancelsBootstrapIfRequirementMatchesMultipleNodes() {
        AtomicReference<Iterable<DiscoveryNode>> discoveredNodes = new AtomicReference<>(Stream.of(otherNode1, otherNode2).toList());
        ClusterBootstrapService clusterBootstrapService = new ClusterBootstrapService(
            Settings.builder().putList(INITIAL_MASTER_NODES_SETTING.getKey(), localNode.getAddress().getAddress()).build(),
            transportService,
            discoveredNodes::get,
            () -> false,
            vc -> {
                throw new AssertionError("should not be called");
            }
        );

        transportService.start();
        clusterBootstrapService.onFoundPeersUpdated();
        deterministicTaskQueue.runAllTasks();

        discoveredNodes.set(emptyList());
        clusterBootstrapService.onFoundPeersUpdated();
        deterministicTaskQueue.runAllTasks();
    }

    public void testCancelsBootstrapIfNodeMatchesMultipleRequirements() {
        AtomicReference<Iterable<DiscoveryNode>> discoveredNodes = new AtomicReference<>(List.of(otherNode1, otherNode2));
        ClusterBootstrapService clusterBootstrapService = new ClusterBootstrapService(
            Settings.builder()
                .putList(INITIAL_MASTER_NODES_SETTING.getKey(), otherNode1.getAddress().toString(), otherNode1.getName())
                .build(),
            transportService,
            discoveredNodes::get,
            () -> false,
            vc -> {
                throw new AssertionError("should not be called");
            }
        );

        transportService.start();
        clusterBootstrapService.onFoundPeersUpdated();
        deterministicTaskQueue.runAllTasks();

        discoveredNodes.set(
            List.of(
                DiscoveryNodeUtils.builder(randomAlphaOfLength(10))
                    .name(otherNode1.getName())
                    .roles(Set.of(DiscoveryNodeRole.MASTER_ROLE))
                    .build(),
                DiscoveryNodeUtils.builder(randomAlphaOfLength(10))
                    .name("yet-another-node")
                    .address(otherNode1.getAddress())
                    .roles(Set.of(DiscoveryNodeRole.MASTER_ROLE))
                    .build()
            )
        );

        clusterBootstrapService.onFoundPeersUpdated();
        deterministicTaskQueue.runAllTasks();
    }

    public void testMatchesOnNodeName() {
        final AtomicBoolean bootstrapped = new AtomicBoolean();
        ClusterBootstrapService clusterBootstrapService = new ClusterBootstrapService(
            Settings.builder().putList(INITIAL_MASTER_NODES_SETTING.getKey(), localNode.getName()).build(),
            transportService,
            Collections::emptyList,
            () -> false,
            vc -> assertTrue(bootstrapped.compareAndSet(false, true))
        );

        transportService.start();
        clusterBootstrapService.onFoundPeersUpdated();
        deterministicTaskQueue.runAllTasks();
        assertTrue(bootstrapped.get());
    }

    public void testMatchesOnNodeAddress() {
        final AtomicBoolean bootstrapped = new AtomicBoolean();
        ClusterBootstrapService clusterBootstrapService = new ClusterBootstrapService(
            Settings.builder().putList(INITIAL_MASTER_NODES_SETTING.getKey(), localNode.getAddress().toString()).build(),
            transportService,
            Collections::emptyList,
            () -> false,
            vc -> assertTrue(bootstrapped.compareAndSet(false, true))
        );

        transportService.start();
        clusterBootstrapService.onFoundPeersUpdated();
        deterministicTaskQueue.runAllTasks();
        assertTrue(bootstrapped.get());
    }

    public void testMatchesOnNodeHostAddress() {
        final AtomicBoolean bootstrapped = new AtomicBoolean();
        ClusterBootstrapService clusterBootstrapService = new ClusterBootstrapService(
            Settings.builder().putList(INITIAL_MASTER_NODES_SETTING.getKey(), localNode.getAddress().getAddress()).build(),
            transportService,
            Collections::emptyList,
            () -> false,
            vc -> assertTrue(bootstrapped.compareAndSet(false, true))
        );

        transportService.start();
        clusterBootstrapService.onFoundPeersUpdated();
        deterministicTaskQueue.runAllTasks();
        assertTrue(bootstrapped.get());
    }

    public void testDoesNotJustMatchEverything() {
        ClusterBootstrapService clusterBootstrapService = new ClusterBootstrapService(
            Settings.builder().putList(INITIAL_MASTER_NODES_SETTING.getKey(), randomAlphaOfLength(10)).build(),
            transportService,
            Collections::emptyList,
            () -> false,
            vc -> {
                throw new AssertionError("should not be called");
            }
        );

        transportService.start();
        clusterBootstrapService.onFoundPeersUpdated();
        deterministicTaskQueue.runAllTasks();
    }

    public void testDoesNotIncludeExtraNodes() {
        final DiscoveryNode extraNode = newDiscoveryNode("extra-node");
        final AtomicBoolean bootstrapped = new AtomicBoolean();
        ClusterBootstrapService clusterBootstrapService = new ClusterBootstrapService(
            Settings.builder()
                .putList(INITIAL_MASTER_NODES_SETTING.getKey(), localNode.getName(), otherNode1.getName(), otherNode2.getName())
                .build(),
            transportService,
            () -> List.of(otherNode1, otherNode2, extraNode),
            () -> false,
            vc -> {
                assertTrue(bootstrapped.compareAndSet(false, true));
                assertThat(vc.getNodeIds(), not(hasItem(extraNode.getId())));
            }
        );

        transportService.start();
        clusterBootstrapService.onFoundPeersUpdated();
        deterministicTaskQueue.runAllTasks();
        assertTrue(bootstrapped.get());
    }

    public void testBootstrapsAutomaticallyWithSingleNodeDiscovery() {
        final Settings.Builder settings = Settings.builder()
            .put(DiscoveryModule.DISCOVERY_TYPE_SETTING.getKey(), DiscoveryModule.SINGLE_NODE_DISCOVERY_TYPE)
            .put(NODE_NAME_SETTING.getKey(), localNode.getName());
        final AtomicBoolean bootstrapped = new AtomicBoolean();

        ClusterBootstrapService clusterBootstrapService = new ClusterBootstrapService(
            settings.build(),
            transportService,
            Collections::emptyList,
            () -> false,
            vc -> {
                assertTrue(bootstrapped.compareAndSet(false, true));
                assertThat(vc.getNodeIds(), hasSize(1));
                assertThat(vc.getNodeIds(), hasItem(localNode.getId()));
                assertTrue(vc.hasQuorum(singletonList(localNode.getId())));
            }
        );

        transportService.start();
        clusterBootstrapService.onFoundPeersUpdated();
        deterministicTaskQueue.runAllTasks();
        assertTrue(bootstrapped.get());

        bootstrapped.set(false);
        clusterBootstrapService.onFoundPeersUpdated();
        deterministicTaskQueue.runAllTasks();
        assertFalse(bootstrapped.get()); // should only bootstrap once
    }

    public void testFailBootstrapWithBothSingleNodeDiscoveryAndInitialMasterNodes() {
        final Settings.Builder settings = Settings.builder()
            .put(DiscoveryModule.DISCOVERY_TYPE_SETTING.getKey(), DiscoveryModule.SINGLE_NODE_DISCOVERY_TYPE)
            .put(NODE_NAME_SETTING.getKey(), localNode.getName())
            .put(INITIAL_MASTER_NODES_SETTING.getKey(), "test");

        assertThat(
            expectThrows(
                IllegalArgumentException.class,
                () -> new ClusterBootstrapService(settings.build(), transportService, Collections::emptyList, () -> false, vc -> fail())
            ).getMessage(),
            containsString(
                "setting [" + INITIAL_MASTER_NODES_SETTING.getKey() + "] is not allowed when [discovery.type] is set " + "to [single-node]"
            )
        );
    }

    public void testFailBootstrapNonMasterEligibleNodeWithSingleNodeDiscovery() {
        final Settings.Builder settings = Settings.builder()
            .put(DiscoveryModule.DISCOVERY_TYPE_SETTING.getKey(), DiscoveryModule.SINGLE_NODE_DISCOVERY_TYPE)
            .put(NODE_NAME_SETTING.getKey(), localNode.getName())
            .put(nonMasterNode());

        assertThat(
            expectThrows(
                IllegalArgumentException.class,
                () -> new ClusterBootstrapService(settings.build(), transportService, Collections::emptyList, () -> false, vc -> fail())
            ).getMessage(),
            containsString("node with [discovery.type] set to [single-node] must be master-eligible")
        );
    }

    public void testBootstrapStateLogging() {
        final var mockAppender = new MockLogAppender();
        try (var ignored = mockAppender.capturing(ClusterBootstrapService.class)) {
            mockAppender.addExpectation(
                new MockLogAppender.SeenEventExpectation(
                    "fresh node message",
                    ClusterBootstrapService.class.getCanonicalName(),
                    Level.INFO,
                    "this node has not joined a bootstrapped cluster yet; [cluster.initial_master_nodes] is set to [node1, node2]"
                )
            );

            final var metadataBuilder = Metadata.builder().clusterUUIDCommitted(false);
            if (randomBoolean()) {
                metadataBuilder.clusterUUID(UUIDs.randomBase64UUID(random()));
            }
            new ClusterBootstrapService(
                Settings.builder().putList(INITIAL_MASTER_NODES_SETTING.getKey(), "node1", "node2").build(),
                transportService,
                Collections::emptyList,
                () -> false,
                vc -> {
                    throw new AssertionError("should not be called");
                }
            ).logBootstrapState(metadataBuilder.build());

            mockAppender.assertAllExpectationsMatched();

            final String infoMessagePattern = """
                this node is locked into cluster UUID [test-uuid] and will not attempt further cluster bootstrapping""";
            mockAppender.addExpectation(
                new MockLogAppender.SeenEventExpectation(
                    "bootstrapped node message",
                    ClusterBootstrapService.class.getCanonicalName(),
                    Level.INFO, infoMessagePattern
                )
            );

            new ClusterBootstrapService(Settings.EMPTY, transportService, Collections::emptyList, () -> false, vc -> {
                throw new AssertionError("should not be called");
            }).logBootstrapState(Metadata.builder().clusterUUID("test-uuid").clusterUUIDCommitted(true).build());

            mockAppender.assertAllExpectationsMatched();

            final var warningMessagePattern = """
                this node is locked into cluster UUID [test-uuid] but [cluster.initial_master_nodes] is set to [node1, node2]; \
                remove this setting to avoid possible data loss caused by subsequent cluster bootstrap attempts; \
                for further information see \
                https://www.elastic.co/guide/en/elasticsearch/reference/*/important-settings.html#initial_master_nodes""";

            mockAppender.addExpectation(
                new MockLogAppender.SeenEventExpectation(
                    "bootstrapped node message if bootstrapping still configured",
                    ClusterBootstrapService.class.getCanonicalName(),
                    Level.WARN,
                    warningMessagePattern
                )
            );

            new ClusterBootstrapService(
                Settings.builder().putList(INITIAL_MASTER_NODES_SETTING.getKey(), "node1", "node2").build(),
                transportService,
                Collections::emptyList,
                () -> false,
                vc -> {
                    throw new AssertionError("should not be called");
                }
            ).logBootstrapState(Metadata.builder().clusterUUID("test-uuid").clusterUUIDCommitted(true).build());

            mockAppender.assertAllExpectationsMatched();

            mockAppender.addExpectation(
                new MockLogAppender.SeenEventExpectation(
                    "bootstrapped node message if bootstrapping still configured",
                    ClusterBootstrapService.class.getCanonicalName(),
                    Level.WARN,
                    warningMessagePattern
                )
            );

            var startTime = deterministicTaskQueue.getCurrentTimeMillis();
            while (deterministicTaskQueue.getCurrentTimeMillis() <= startTime + TimeValue.timeValueHours(12).millis()) {
                deterministicTaskQueue.runAllRunnableTasks();
                deterministicTaskQueue.advanceTime();
            }

            mockAppender.assertAllExpectationsMatched();

            mockAppender.addExpectation(
                new MockLogAppender.SeenEventExpectation(
                    "bootstrapped node message if discovery type is single node ",
                    ClusterBootstrapService.class.getCanonicalName(),
                    Level.INFO,
                    infoMessagePattern
                )
            );

            new ClusterBootstrapService(
                Settings.builder().put(DiscoveryModule.DISCOVERY_TYPE_SETTING.getKey(), DiscoveryModule.SINGLE_NODE_DISCOVERY_TYPE).build(),
                transportService,
                Collections::emptyList,
                () -> false,
                vc -> {
                    throw new AssertionError("should not be called");
                }
            ).logBootstrapState(Metadata.builder().clusterUUID("test-uuid").clusterUUIDCommitted(true).build());

            mockAppender.assertAllExpectationsMatched();
        }
    }

    public void testNoRepeatedLoggingWithSingleNodeDiscoveryType() {
        new ClusterBootstrapService(
            Settings.builder().put(DiscoveryModule.DISCOVERY_TYPE_SETTING.getKey(), DiscoveryModule.SINGLE_NODE_DISCOVERY_TYPE).build(),
            transportService,
            Collections::emptyList,
            () -> false,
            vc -> {
                throw new AssertionError("should not be called");
            }
        ).logBootstrapState(Metadata.builder().clusterUUID("test-uuid").clusterUUIDCommitted(true).build());

        assertFalse((deterministicTaskQueue.hasDeferredTasks() || deterministicTaskQueue.hasRunnableTasks()));
    }
}
