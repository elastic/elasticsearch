/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.discovery;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.Version;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.coordination.Coordinator;
import org.elasticsearch.cluster.coordination.ElectionStrategy;
import org.elasticsearch.cluster.coordination.LeaderHeartbeatService;
import org.elasticsearch.cluster.coordination.PreVoteCollector;
import org.elasticsearch.cluster.coordination.Reconfigurator;
import org.elasticsearch.cluster.coordination.StatefulPreVoteCollector;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.RerouteService;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.service.ClusterApplier;
import org.elasticsearch.cluster.service.MasterService;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.logging.DeprecationCategory;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.gateway.GatewayMetaState;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.monitor.NodeHealthService;
import org.elasticsearch.plugins.ClusterCoordinationPlugin;
import org.elasticsearch.plugins.DiscoveryPlugin;
import org.elasticsearch.transport.TransportService;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.elasticsearch.node.Node.NODE_NAME_SETTING;

/**
 * A module for loading classes for node discovery.
 */
public class DiscoveryModule {
    private static final Logger logger = LogManager.getLogger(DiscoveryModule.class);

    public static final String MULTI_NODE_DISCOVERY_TYPE = "multi-node";
    public static final String SINGLE_NODE_DISCOVERY_TYPE = "single-node";
    @Deprecated
    public static final String LEGACY_MULTI_NODE_DISCOVERY_TYPE = "zen";

    public static final Setting<String> DISCOVERY_TYPE_SETTING = new Setting<>(
        "discovery.type",
        MULTI_NODE_DISCOVERY_TYPE,
        Function.identity(),
        Property.NodeScope
    );

    public static final Setting<List<String>> DISCOVERY_SEED_PROVIDERS_SETTING = Setting.stringListSetting(
        "discovery.seed_providers",
        Property.NodeScope
    );

    public static final String DEFAULT_ELECTION_STRATEGY = "default";

    public static final Setting<String> ELECTION_STRATEGY_SETTING = new Setting<>(
        "cluster.election.strategy",
        DEFAULT_ELECTION_STRATEGY,
        Function.identity(),
        Property.NodeScope
    );

    private final Coordinator coordinator;

    public DiscoveryModule(
        Settings settings,
        TransportService transportService,
        Client client,
        NamedWriteableRegistry namedWriteableRegistry,
        NetworkService networkService,
        MasterService masterService,
        ClusterApplier clusterApplier,
        ClusterSettings clusterSettings,
        List<DiscoveryPlugin> discoveryPlugins,
        List<ClusterCoordinationPlugin> clusterCoordinationPlugins,
        AllocationService allocationService,
        Path configFile,
        GatewayMetaState gatewayMetaState,
        RerouteService rerouteService,
        NodeHealthService nodeHealthService,
        CircuitBreakerService circuitBreakerService
    ) {
        final Collection<BiConsumer<DiscoveryNode, ClusterState>> joinValidators = new ArrayList<>();
        final Map<String, Supplier<SeedHostsProvider>> hostProviders = new HashMap<>();
        hostProviders.put("settings", () -> new SettingsBasedSeedHostsProvider(settings, transportService));
        hostProviders.put("file", () -> new FileBasedSeedHostsProvider(configFile));
        final Map<String, ElectionStrategy> electionStrategies = new HashMap<>();
        electionStrategies.put(DEFAULT_ELECTION_STRATEGY, ElectionStrategy.DEFAULT_INSTANCE);
        for (DiscoveryPlugin plugin : discoveryPlugins) {
            plugin.getSeedHostProviders(transportService, networkService).forEach((key, value) -> {
                if (hostProviders.put(key, value) != null) {
                    throw new IllegalArgumentException("Cannot register seed provider [" + key + "] twice");
                }
            });
        }

        for (ClusterCoordinationPlugin plugin : clusterCoordinationPlugins) {
            BiConsumer<DiscoveryNode, ClusterState> joinValidator = plugin.getJoinValidator();
            if (joinValidator != null) {
                joinValidators.add(joinValidator);
            }
            plugin.getElectionStrategies().forEach((key, value) -> {
                if (electionStrategies.put(key, value) != null) {
                    throw new IllegalArgumentException("Cannot register election strategy [" + key + "] twice");
                }
            });
        }

        List<String> seedProviderNames = DISCOVERY_SEED_PROVIDERS_SETTING.get(settings);
        // for bwc purposes, add settings provider even if not explicitly specified
        if (seedProviderNames.contains("settings") == false) {
            List<String> extendedSeedProviderNames = new ArrayList<>();
            extendedSeedProviderNames.add("settings");
            extendedSeedProviderNames.addAll(seedProviderNames);
            seedProviderNames = extendedSeedProviderNames;
        }

        final Set<String> missingProviderNames = new HashSet<>(seedProviderNames);
        missingProviderNames.removeAll(hostProviders.keySet());
        if (missingProviderNames.isEmpty() == false) {
            throw new IllegalArgumentException("Unknown seed providers " + missingProviderNames);
        }

        List<SeedHostsProvider> filteredSeedProviders = seedProviderNames.stream().map(hostProviders::get).map(Supplier::get).toList();

        String discoveryType = DISCOVERY_TYPE_SETTING.get(settings);

        final SeedHostsProvider seedHostsProvider = hostsResolver -> {
            final List<TransportAddress> addresses = new ArrayList<>();
            for (SeedHostsProvider provider : filteredSeedProviders) {
                addresses.addAll(provider.getSeedAddresses(hostsResolver));
            }
            return Collections.unmodifiableList(addresses);
        };

        final ElectionStrategy electionStrategy = electionStrategies.get(ELECTION_STRATEGY_SETTING.get(settings));
        if (electionStrategy == null) {
            throw new IllegalArgumentException("Unknown election strategy " + ELECTION_STRATEGY_SETTING.get(settings));
        }

        if (LEGACY_MULTI_NODE_DISCOVERY_TYPE.equals(discoveryType)) {
            assert Version.CURRENT.major == Version.V_7_0_0.major + 1;
            DeprecationLogger.getLogger(DiscoveryModule.class)
                .critical(
                    DeprecationCategory.SETTINGS,
                    "legacy-discovery-type",
                    "Support for setting [{}] to [{}] is deprecated and will be removed in a future version. Set this setting to [{}] "
                        + "instead.",
                    DISCOVERY_TYPE_SETTING.getKey(),
                    LEGACY_MULTI_NODE_DISCOVERY_TYPE,
                    MULTI_NODE_DISCOVERY_TYPE
                );
        }

        var reconfigurator = getReconfigurator(settings, clusterSettings, clusterCoordinationPlugins);
        var preVoteCollectorFactory = getPreVoteCollectorFactory(clusterCoordinationPlugins);
        var leaderHeartbeatService = getLeaderHeartbeatService(settings, clusterCoordinationPlugins);

        if (MULTI_NODE_DISCOVERY_TYPE.equals(discoveryType)
            || LEGACY_MULTI_NODE_DISCOVERY_TYPE.equals(discoveryType)
            || SINGLE_NODE_DISCOVERY_TYPE.equals(discoveryType)) {
            coordinator = new Coordinator(
                NODE_NAME_SETTING.get(settings),
                settings,
                clusterSettings,
                transportService,
                client,
                namedWriteableRegistry,
                allocationService,
                masterService,
                gatewayMetaState::getPersistedState,
                seedHostsProvider,
                clusterApplier,
                joinValidators,
                new Random(Randomness.get().nextLong()),
                rerouteService,
                electionStrategy,
                nodeHealthService,
                circuitBreakerService,
                reconfigurator,
                leaderHeartbeatService,
                preVoteCollectorFactory
            );
        } else {
            throw new IllegalArgumentException("Unknown discovery type [" + discoveryType + "]");
        }

        logger.info("using discovery type [{}] and seed hosts providers {}", discoveryType, seedProviderNames);
    }

    // visible for testing
    static Reconfigurator getReconfigurator(
        Settings settings,
        ClusterSettings clusterSettings,
        List<ClusterCoordinationPlugin> clusterCoordinationPlugins
    ) {
        final var reconfiguratorFactories = clusterCoordinationPlugins.stream()
            .map(ClusterCoordinationPlugin::getReconfiguratorFactory)
            .flatMap(Optional::stream)
            .toList();

        if (reconfiguratorFactories.size() > 1) {
            throw new IllegalStateException("multiple reconfigurator factories found: " + reconfiguratorFactories);
        }

        if (reconfiguratorFactories.size() == 1) {
            return reconfiguratorFactories.get(0).newReconfigurator(settings, clusterSettings);
        }

        return new Reconfigurator(settings, clusterSettings);
    }

    // visible for testing
    static PreVoteCollector.Factory getPreVoteCollectorFactory(List<ClusterCoordinationPlugin> clusterCoordinationPlugins) {
        final var preVoteCollectorFactories = clusterCoordinationPlugins.stream()
            .map(ClusterCoordinationPlugin::getPreVoteCollectorFactory)
            .flatMap(Optional::stream)
            .toList();

        if (preVoteCollectorFactories.size() > 1) {
            throw new IllegalStateException("multiple pre-vote collector factories found: " + preVoteCollectorFactories);
        }

        if (preVoteCollectorFactories.size() == 1) {
            return preVoteCollectorFactories.get(0);
        }

        return StatefulPreVoteCollector::new;
    }

    static LeaderHeartbeatService getLeaderHeartbeatService(Settings settings, List<ClusterCoordinationPlugin> clusterCoordinationPlugins) {
        final var heartbeatServices = clusterCoordinationPlugins.stream()
            .map(plugin -> plugin.getLeaderHeartbeatService(settings))
            .flatMap(Optional::stream)
            .toList();

        if (heartbeatServices.size() > 1) {
            throw new IllegalStateException("multiple leader heart beat service factories found: " + heartbeatServices);
        }

        if (heartbeatServices.size() == 1) {
            return heartbeatServices.get(0);
        }

        return LeaderHeartbeatService.NO_OP;
    }

    public static boolean isSingleNodeDiscovery(Settings settings) {
        return SINGLE_NODE_DISCOVERY_TYPE.equals(DISCOVERY_TYPE_SETTING.get(settings));
    }

    public Coordinator getCoordinator() {
        return coordinator;
    }
}
