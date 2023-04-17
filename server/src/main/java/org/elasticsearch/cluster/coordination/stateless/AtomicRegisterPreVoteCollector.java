/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.coordination.stateless;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.coordination.PreVoteCollector;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.core.Releasable;

import java.util.concurrent.atomic.AtomicBoolean;

public class AtomicRegisterPreVoteCollector extends PreVoteCollector {
    private final StoreHeartbeatService heartbeatService;
    private final Runnable startElection;

    public AtomicRegisterPreVoteCollector(StoreHeartbeatService heartbeatService, Runnable startElection) {
        this.heartbeatService = heartbeatService;
        this.startElection = startElection;
    }

    @Override
    public Releasable start(ClusterState clusterState, Iterable<DiscoveryNode> broadcastNodes) {
        final var shouldRun = new AtomicBoolean(true);
        heartbeatService.runIfNoRecentLeader(() -> {
            if (shouldRun.getAndSet(false)) {
                startElection.run();
            }
        });

        return () -> shouldRun.set(false);
    }
}
