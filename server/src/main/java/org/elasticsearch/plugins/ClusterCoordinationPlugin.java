/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugins;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.coordination.CoordinationState;
import org.elasticsearch.cluster.coordination.ElectionStrategy;
import org.elasticsearch.cluster.coordination.LeaderHeartbeatService;
import org.elasticsearch.cluster.coordination.PreVoteCollector;
import org.elasticsearch.cluster.coordination.Reconfigurator;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.gateway.PersistedClusterStateService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.NamedXContentRegistry;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiConsumer;

public interface ClusterCoordinationPlugin {

    /**
     * Returns a consumer that validate the initial join cluster state. The validator, unless <code>null</code> is called exactly once per
     * join attempt but might be called multiple times during the lifetime of a node. Validators are expected to throw a
     * {@link IllegalStateException} if the node and the cluster-state are incompatible.
     */
    default BiConsumer<DiscoveryNode, ClusterState> getJoinValidator() {
        return null;
    }

    /**
     * Allows plugging in election strategies (see {@link ElectionStrategy}) that define a customized notion of an election quorum.
     */
    default Map<String, ElectionStrategy> getElectionStrategies() {
        return Collections.emptyMap();
    }

    default Optional<PersistedStateFactory> getPersistedStateFactory() {
        return Optional.empty();
    }

    default Optional<PersistedClusterStateServiceFactory> getPersistedClusterStateServiceFactory() {
        return Optional.empty();
    }

    default Optional<ReconfiguratorFactory> getReconfiguratorFactory() {
        return Optional.empty();
    }

    default Optional<PreVoteCollector.Factory> getPreVoteCollectorFactory() {
        return Optional.empty();
    }

    default Optional<LeaderHeartbeatService> getLeaderHeartbeatService(Settings settings) {
        return Optional.empty();
    }

    interface PersistedStateFactory {
        CoordinationState.PersistedState createPersistedState(
            Settings settings,
            TransportService transportService,
            PersistedClusterStateService persistedClusterStateService
        ) throws IOException;
    }

    interface PersistedClusterStateServiceFactory {
        PersistedClusterStateService newPersistedClusterStateService(
            NodeEnvironment nodeEnvironment,
            NamedXContentRegistry xContentRegistry,
            ClusterSettings clusterSettings,
            ThreadPool threadPool
        );
    }

    interface ReconfiguratorFactory {
        Reconfigurator newReconfigurator(Settings settings, ClusterSettings clusterSettings);
    }
}
