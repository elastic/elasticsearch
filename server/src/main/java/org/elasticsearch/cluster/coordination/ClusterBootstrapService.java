/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.cluster.coordination;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.coordination.CoordinationMetadata.VotingConfiguration;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.discovery.DiscoveryModule;
import org.elasticsearch.node.Node;
import org.elasticsearch.threadpool.ThreadPool.Names;
import org.elasticsearch.transport.TransportService;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static java.util.Collections.emptyList;
import static java.util.Collections.unmodifiableSet;
import static org.elasticsearch.discovery.DiscoveryModule.DISCOVERY_SEED_PROVIDERS_SETTING;
import static org.elasticsearch.discovery.SettingsBasedSeedHostsProvider.DISCOVERY_SEED_HOSTS_SETTING;

public class ClusterBootstrapService implements Coordinator.PeerFinderListener {

    public static final Setting<List<String>> INITIAL_MASTER_NODES_SETTING = Setting.listSetting(
        "cluster.initial_master_nodes",
        emptyList(),
        Function.identity(),
        Property.NodeScope
    );

    public static final Setting<TimeValue> UNCONFIGURED_BOOTSTRAP_TIMEOUT_SETTING = Setting.timeSetting(
        "discovery.unconfigured_bootstrap_timeout",
        TimeValue.timeValueSeconds(3),
        TimeValue.timeValueMillis(1),
        Property.NodeScope
    );

    static final String BOOTSTRAP_PLACEHOLDER_PREFIX = "{bootstrap-placeholder}-";

    private static final Logger logger = LogManager.getLogger(ClusterBootstrapService.class);
    private final Set<String> bootstrapRequirements;
    @Nullable // null if discoveryIsConfigured()
    private final TimeValue unconfiguredBootstrapTimeout;
    private final TransportService transportService;
    private final Supplier<Iterable<DiscoveryNode>> discoveredNodesSupplier;
    private final BooleanSupplier isBootstrappedSupplier;
    private final Consumer<VotingConfiguration> votingConfigurationConsumer;
    private final AtomicBoolean bootstrappingPermitted = new AtomicBoolean(true);

    public ClusterBootstrapService(
        Settings settings,
        TransportService transportService,
        Supplier<Iterable<DiscoveryNode>> discoveredNodesSupplier,
        BooleanSupplier isBootstrappedSupplier,
        Consumer<VotingConfiguration> votingConfigurationConsumer
    ) {
        if (DiscoveryModule.isSingleNodeDiscovery(settings)) {
            if (INITIAL_MASTER_NODES_SETTING.exists(settings)) {
                throw new IllegalArgumentException(
                    "setting ["
                        + INITIAL_MASTER_NODES_SETTING.getKey()
                        + "] is not allowed when ["
                        + DiscoveryModule.DISCOVERY_TYPE_SETTING.getKey()
                        + "] is set to ["
                        + DiscoveryModule.SINGLE_NODE_DISCOVERY_TYPE
                        + "]"
                );
            }
            if (DiscoveryNode.isMasterNode(settings) == false) {
                throw new IllegalArgumentException(
                    "node with ["
                        + DiscoveryModule.DISCOVERY_TYPE_SETTING.getKey()
                        + "] set to ["
                        + DiscoveryModule.SINGLE_NODE_DISCOVERY_TYPE
                        + "] must be master-eligible"
                );
            }
            bootstrapRequirements = Collections.singleton(Node.NODE_NAME_SETTING.get(settings));
            unconfiguredBootstrapTimeout = null;
        } else {
            final List<String> initialMasterNodes = INITIAL_MASTER_NODES_SETTING.get(settings);
            bootstrapRequirements = unmodifiableSet(new LinkedHashSet<>(initialMasterNodes));
            if (bootstrapRequirements.size() != initialMasterNodes.size()) {
                throw new IllegalArgumentException(
                    "setting [" + INITIAL_MASTER_NODES_SETTING.getKey() + "] contains duplicates: " + initialMasterNodes
                );
            }
            unconfiguredBootstrapTimeout = discoveryIsConfigured(settings) ? null : UNCONFIGURED_BOOTSTRAP_TIMEOUT_SETTING.get(settings);
        }

        this.transportService = transportService;
        this.discoveredNodesSupplier = discoveredNodesSupplier;
        this.isBootstrappedSupplier = isBootstrappedSupplier;
        this.votingConfigurationConsumer = votingConfigurationConsumer;
    }

    public static boolean discoveryIsConfigured(Settings settings) {
        return Stream.of(DISCOVERY_SEED_PROVIDERS_SETTING, DISCOVERY_SEED_HOSTS_SETTING, INITIAL_MASTER_NODES_SETTING)
            .anyMatch(s -> s.exists(settings));
    }

    void logBootstrapState(Metadata metadata) {
        if (metadata.clusterUUIDCommitted()) {
            final var clusterUUID = metadata.clusterUUID();
            if (bootstrapRequirements.isEmpty()) {
                logger.info("this node is locked into cluster UUID [{}] and will not attempt further cluster bootstrapping", clusterUUID);
            } else {
                logger.warn(
                    """
                        this node is locked into cluster UUID [{}] but [{}] is set to {}; \
                        remove this setting to avoid possible data loss caused by subsequent cluster bootstrap attempts""",
                    clusterUUID,
                    INITIAL_MASTER_NODES_SETTING.getKey(),
                    bootstrapRequirements
                );
            }
        } else {
            logger.info(
                "this node has not joined a bootstrapped cluster yet; [{}] is set to {}",
                INITIAL_MASTER_NODES_SETTING.getKey(),
                bootstrapRequirements
            );
        }
    }

    @Override
    public void onFoundPeersUpdated() {
        final Set<DiscoveryNode> nodes = getDiscoveredNodes();
        if (bootstrappingPermitted.get()
            && transportService.getLocalNode().isMasterNode()
            && bootstrapRequirements.isEmpty() == false
            && isBootstrappedSupplier.getAsBoolean() == false) {

            final Tuple<Set<DiscoveryNode>, List<String>> requirementMatchingResult;
            try {
                requirementMatchingResult = checkRequirements(nodes);
            } catch (IllegalStateException e) {
                logger.warn("bootstrapping cancelled", e);
                bootstrappingPermitted.set(false);
                return;
            }

            final Set<DiscoveryNode> nodesMatchingRequirements = requirementMatchingResult.v1();
            final List<String> unsatisfiedRequirements = requirementMatchingResult.v2();
            logger.trace(
                "nodesMatchingRequirements={}, unsatisfiedRequirements={}, bootstrapRequirements={}",
                nodesMatchingRequirements,
                unsatisfiedRequirements,
                bootstrapRequirements
            );

            if (nodesMatchingRequirements.contains(transportService.getLocalNode()) == false) {
                logger.info(
                    "skipping cluster bootstrapping as local node does not match bootstrap requirements: {}",
                    bootstrapRequirements
                );
                bootstrappingPermitted.set(false);
                return;
            }

            if (nodesMatchingRequirements.size() * 2 > bootstrapRequirements.size()) {
                startBootstrap(nodesMatchingRequirements, unsatisfiedRequirements);
            }
        }
    }

    void scheduleUnconfiguredBootstrap() {
        if (unconfiguredBootstrapTimeout == null) {
            return;
        }

        if (transportService.getLocalNode().isMasterNode() == false) {
            return;
        }

        logger.info(
            "no discovery configuration found, will perform best-effort cluster bootstrapping after [{}] "
                + "unless existing master is discovered",
            unconfiguredBootstrapTimeout
        );

        transportService.getThreadPool().scheduleUnlessShuttingDown(unconfiguredBootstrapTimeout, Names.GENERIC, new Runnable() {
            @Override
            public void run() {
                final Set<DiscoveryNode> discoveredNodes = getDiscoveredNodes();
                logger.debug("performing best-effort cluster bootstrapping with {}", discoveredNodes);
                startBootstrap(discoveredNodes, emptyList());
            }

            @Override
            public String toString() {
                return "unconfigured-discovery delayed bootstrap";
            }
        });
    }

    private Set<DiscoveryNode> getDiscoveredNodes() {
        return Stream.concat(
            Stream.of(transportService.getLocalNode()),
            StreamSupport.stream(discoveredNodesSupplier.get().spliterator(), false)
        ).collect(Collectors.toSet());
    }

    private void startBootstrap(Set<DiscoveryNode> discoveryNodes, List<String> unsatisfiedRequirements) {
        assert discoveryNodes.stream().allMatch(DiscoveryNode::isMasterNode) : discoveryNodes;
        assert unsatisfiedRequirements.size() < discoveryNodes.size() : discoveryNodes + " smaller than " + unsatisfiedRequirements;
        if (bootstrappingPermitted.compareAndSet(true, false)) {
            doBootstrap(
                new VotingConfiguration(
                    Stream.concat(
                        discoveryNodes.stream().map(DiscoveryNode::getId),
                        unsatisfiedRequirements.stream().map(s -> BOOTSTRAP_PLACEHOLDER_PREFIX + s)
                    ).collect(Collectors.toSet())
                )
            );
        }
    }

    public static boolean isBootstrapPlaceholder(String nodeId) {
        return nodeId.startsWith(BOOTSTRAP_PLACEHOLDER_PREFIX);
    }

    private void doBootstrap(VotingConfiguration votingConfiguration) {
        assert transportService.getLocalNode().isMasterNode();

        try {
            votingConfigurationConsumer.accept(votingConfiguration);
        } catch (Exception e) {
            logger.warn(() -> "exception when bootstrapping with " + votingConfiguration + ", rescheduling", e);
            transportService.getThreadPool().scheduleUnlessShuttingDown(TimeValue.timeValueSeconds(10), Names.GENERIC, new Runnable() {
                @Override
                public void run() {
                    doBootstrap(votingConfiguration);
                }

                @Override
                public String toString() {
                    return "retry of failed bootstrapping with " + votingConfiguration;
                }
            });
        }
    }

    private static boolean matchesRequirement(DiscoveryNode discoveryNode, String requirement) {
        return discoveryNode.getName().equals(requirement)
            || discoveryNode.getAddress().toString().equals(requirement)
            || discoveryNode.getAddress().getAddress().equals(requirement);
    }

    private Tuple<Set<DiscoveryNode>, List<String>> checkRequirements(Set<DiscoveryNode> nodes) {
        final Set<DiscoveryNode> selectedNodes = new HashSet<>();
        final List<String> unmatchedRequirements = new ArrayList<>();
        for (final String bootstrapRequirement : bootstrapRequirements) {
            final Set<DiscoveryNode> matchingNodes = nodes.stream()
                .filter(n -> matchesRequirement(n, bootstrapRequirement))
                .collect(Collectors.toSet());

            if (matchingNodes.size() == 0) {
                unmatchedRequirements.add(bootstrapRequirement);
            }

            if (matchingNodes.size() > 1) {
                throw new IllegalStateException("requirement [" + bootstrapRequirement + "] matches multiple nodes: " + matchingNodes);
            }

            for (final DiscoveryNode matchingNode : matchingNodes) {
                if (selectedNodes.add(matchingNode) == false) {
                    throw new IllegalStateException(
                        "node ["
                            + matchingNode
                            + "] matches multiple requirements: "
                            + bootstrapRequirements.stream().filter(r -> matchesRequirement(matchingNode, r)).toList()
                    );
                }
            }
        }

        return Tuple.tuple(selectedNodes, unmatchedRequirements);
    }
}
