/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.indices.recovery;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.util.concurrent.Executor;
import java.util.function.Consumer;

public class PeerRecoverySourceClusterStateDelay {
    private PeerRecoverySourceClusterStateDelay() {}

    private static final Logger logger = LogManager.getLogger(PeerRecoverySourceClusterStateDelay.class);

    /**
     * Waits for the given cluster state version to be applied locally before proceeding with recovery
     */
    public static <T> void ensureClusterStateVersion(
        long clusterStateVersion,
        ClusterService clusterService,
        Executor executor,
        ThreadContext threadContext,
        ActionListener<T> listener,
        Consumer<ActionListener<T>> proceedWithRecovery
    ) {
        if (clusterStateVersion <= clusterService.state().version()) {
            // either our locally-applied cluster state is already fresh enough, or request.clusterStateVersion() == 0 for bwc
            proceedWithRecovery.accept(listener);
        } else {
            logger.debug("delaying {} until application of cluster state version {}", proceedWithRecovery, clusterStateVersion);
            final var waitListener = new SubscribableListener<Void>();
            final var clusterStateVersionListener = new ClusterStateListener() {
                @Override
                public void clusterChanged(ClusterChangedEvent event) {
                    if (clusterStateVersion <= event.state().version()) {
                        waitListener.onResponse(null);
                    }
                }

                @Override
                public String toString() {
                    return "ClusterStateListener for " + proceedWithRecovery;
                }
            };
            clusterService.addListener(clusterStateVersionListener);
            waitListener.addListener(ActionListener.running(() -> clusterService.removeListener(clusterStateVersionListener)));
            if (clusterStateVersion <= clusterService.state().version()) {
                waitListener.onResponse(null);
            }
            waitListener.addListener(
                listener.delegateFailureAndWrap((l, ignored) -> proceedWithRecovery.accept(l)),
                executor,
                threadContext
            );
            // NB no timeout. If we never apply the fresh cluster state then eventually we leave the cluster which removes the recovery
            // from the routing table so the target shard will fail.
        }
    }
}
