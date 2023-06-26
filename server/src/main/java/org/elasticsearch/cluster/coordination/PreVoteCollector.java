/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.coordination;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.monitor.NodeHealthService;
import org.elasticsearch.transport.TransportService;

import java.util.function.LongConsumer;

public abstract class PreVoteCollector {
    private final Logger logger = LogManager.getLogger(PreVoteCollector.class);

    // Tuple for simple atomic updates. null until the first call to `update()`.
    protected volatile Tuple<DiscoveryNode, PreVoteResponse> state; // DiscoveryNode component is null if there is currently no known
                                                                    // leader.

    // only for testing
    PreVoteResponse getPreVoteResponse() {
        return state.v2();
    }

    // only for testing
    @Nullable
    DiscoveryNode getLeader() {
        return state.v1();
    }

    public void update(final PreVoteResponse preVoteResponse, @Nullable final DiscoveryNode leader) {
        logger.trace("updating with preVoteResponse={}, leader={}", preVoteResponse, leader);
        state = new Tuple<>(leader, preVoteResponse);
    }

    public abstract Releasable start(ClusterState clusterState, Iterable<DiscoveryNode> broadcastNodes);

    public interface Factory {
        PreVoteCollector create(
            TransportService transportService,
            Runnable startElection,
            LongConsumer updateMaxTermSeen,
            ElectionStrategy electionStrategy,
            NodeHealthService nodeHealthService,
            LeaderHeartbeatService leaderHeartbeatService
        );
    }
}
