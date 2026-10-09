/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.snapshots;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.repositories.RepositoryData;
import org.elasticsearch.transport.ActionNotFoundTransportException;

import java.util.List;
import java.util.Set;

/**
 * Keeps the shard generations in a {@link RepositoryFilesCache} current by asking the master for them, see
 * {@link TransportGetShardGenerationsAction}.
 * <p>
 * The master is asked when the repository generation in the cluster state is newer than what the cache has, or when a shard on this
 * node has no shard generation yet, always for all the primary shards on this node at once. At most one request per repository is in
 * flight: a trigger that comes meanwhile is remembered and, if it still matters when the answer is in, answered by one more request.
 * <p>
 * An answer is only accepted if it is from a repository generation at least as new as the one that triggered the request, because the
 * master may not have caught up with the cluster state yet. A request that fails (e.g. because the master does not have the action
 * during a rolling upgrade) or whose answer is not accepted is not repeated because of cluster state changes, which are too frequent for
 * that: it is repeated by the next periodic {@link Trigger#TICK}.
 */
class ShardGenerationsRefresher {

    private static final Logger logger = LogManager.getLogger(ShardGenerationsRefresher.class);

    enum Trigger {
        /**
         * A cluster state change, which can start a request unless the previous one failed
         */
        CLUSTER_STATE,
        /**
         * The periodic evaluation, which always can
         */
        TICK
    }

    /**
     * Sends the request to the master
     */
    interface MasterClient {
        /**
         * @param observedRepositoryGeneration the repository generation in the cluster state of this node
         */
        void getShardGenerations(long observedRepositoryGeneration, List<ShardId> shardIds, ActionListener<GetShardGenerationsResponse> l);
    }

    private final String repositoryName;
    private final MasterClient master;
    private final RepositoryFilesCache cache;

    private boolean requestInFlight;
    // set while a request is in flight and a trigger came that the request may not cover, for the latest such trigger
    private boolean triggerPending;
    private long pendingObservedGeneration = RepositoryData.UNKNOWN_REPO_GEN;
    @Nullable
    private Set<ShardId> pendingShards;
    // set after a request failed or was not accepted, until the next tick
    private boolean waitForTick;
    private boolean closed;

    ShardGenerationsRefresher(String repositoryName, MasterClient master, RepositoryFilesCache cache) {
        this.repositoryName = repositoryName;
        this.master = master;
        this.cache = cache;
    }

    /**
     * Asks the master for the shard generations if they are not current.
     *
     * @param observedRepositoryGeneration the repository generation in the cluster state of this node
     * @param shards                       the primary shards on this node
     */
    void refresh(Trigger trigger, long observedRepositoryGeneration, Set<ShardId> shards) {
        if (observedRepositoryGeneration < RepositoryData.EMPTY_REPO_GEN || shards.isEmpty()) {
            return; // the repository generation is not known (yet), or there is nothing to ask for
        }
        synchronized (this) {
            if (closed || (trigger == Trigger.CLUSTER_STATE && waitForTick)) {
                return;
            }
            if (trigger == Trigger.TICK) {
                waitForTick = false;
            }
            if (isCurrent(observedRepositoryGeneration, shards)) {
                return;
            }
            if (requestInFlight) {
                triggerPending = true;
                pendingObservedGeneration = Math.max(pendingObservedGeneration, observedRepositoryGeneration);
                pendingShards = Set.copyOf(shards);
                return;
            }
            requestInFlight = true;
        }
        sendRequest(observedRepositoryGeneration, shards);
    }

    private boolean isCurrent(long observedRepositoryGeneration, Set<ShardId> shards) {
        return cache.getRepositoryGeneration() >= observedRepositoryGeneration && shards.stream().allMatch(cache::hasShardGeneration);
    }

    private void sendRequest(long observedRepositoryGeneration, Set<ShardId> shards) {
        final ActionListener<GetShardGenerationsResponse> listener = ActionListener.wrap(
            response -> onResponse(observedRepositoryGeneration, response),
            this::onFailure
        );
        try {
            master.getShardGenerations(observedRepositoryGeneration, List.copyOf(shards), listener);
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    private void onResponse(long triggeringRepositoryGeneration, GetShardGenerationsResponse response) {
        final boolean accepted = response.getRepositoryGeneration() >= triggeringRepositoryGeneration;
        if (accepted) {
            cache.onShardGenerations(response.getRepositoryGeneration(), response.getShardGenerations());
        } else {
            logger.debug(
                "[{}] the master is at repository generation [{}], older than [{}] that this node has seen: asking again later",
                repositoryName,
                response.getRepositoryGeneration(),
                triggeringRepositoryGeneration
            );
        }
        completeRequest(accepted);
    }

    private void onFailure(Exception e) {
        if (ExceptionsHelper.unwrapCause(e) instanceof ActionNotFoundTransportException) {
            // e.g. a rolling upgrade, where the master is not upgraded yet
            logger.debug("[{}] the master cannot tell the shard generations yet: asking again later", repositoryName);
        } else {
            logger.debug(() -> "[" + repositoryName + "] failed to get the shard generations from the master: asking again later", e);
        }
        completeRequest(false);
    }

    private void completeRequest(boolean accepted) {
        final long observedGeneration;
        final Set<ShardId> shards;
        synchronized (this) {
            requestInFlight = false;
            waitForTick = accepted == false;
            if (accepted == false || triggerPending == false) {
                triggerPending = false;
                pendingObservedGeneration = RepositoryData.UNKNOWN_REPO_GEN;
                pendingShards = null;
                return;
            }
            observedGeneration = pendingObservedGeneration;
            shards = pendingShards;
            triggerPending = false;
            pendingObservedGeneration = RepositoryData.UNKNOWN_REPO_GEN;
            pendingShards = null;
        }
        refresh(Trigger.CLUSTER_STATE, observedGeneration, shards);
    }

    /**
     * Stops asking, e.g. because the tracking of the repository was turned off. An answer that is still due is ignored.
     */
    synchronized void close() {
        closed = true;
    }
}
