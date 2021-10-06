/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.searchablesnapshots.cache.shared;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.RerouteService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.searchablesnapshots.action.cache.FrozenCacheInfoAction;
import org.elasticsearch.xpack.searchablesnapshots.action.cache.FrozenCacheInfoResponse;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Keeps track of the nodes in the cluster and whether they do or do not have a frozen-tier shared cache, because we can only allocate
 * frozen-tier shards to such nodes.
 */
public class FrozenCacheInfoService {

    private static final Logger logger = LogManager.getLogger(FrozenCacheInfoService.class);

    private final Object mutex = new Object();

    /**
     * The known data nodes, along with an indication whether they have a frozen cache or not
     */
    private final Map<DiscoveryNode, NodeStateHolder> nodeStates = new HashMap<>();

    public void updateNodes(Client client, Set<DiscoveryNode> nodes, RerouteService rerouteService) {
        final List<AsyncNodeFetch> newFetches;
        synchronized (mutex) {
            // clean up nodes that left the cluster
            nodeStates.keySet().removeIf(n -> nodes.contains(n) == false);

            // skip nodes with known state
            nodes.removeAll(nodeStates.keySet());

            // reach out to any new nodes
            newFetches = new ArrayList<>(nodes.size());
            for (DiscoveryNode newNode : nodes) {
                final NodeStateHolder nodeStateHolder = new NodeStateHolder();
                final NodeStateHolder prevState = nodeStates.put(newNode, nodeStateHolder);
                assert prevState == null;

                logger.trace("fetching frozen cache state for {}", newNode);
                newFetches.add(new AsyncNodeFetch(client, rerouteService, newNode, nodeStateHolder));
            }
        }

        newFetches.forEach(Runnable::run);
    }

    public NodeState getNodeState(DiscoveryNode discoveryNode) {
        final NodeStateHolder nodeStateHolder;
        synchronized (mutex) {
            nodeStateHolder = nodeStates.get(discoveryNode);
        }
        return nodeStateHolder == null ? NodeState.UNKNOWN : nodeStateHolder.nodeState;
    }

    public void clear() {
        synchronized (mutex) {
            nodeStates.clear();
        }
    }

    public boolean isFetching() {
        synchronized (mutex) {
            return nodeStates.values().stream().anyMatch(nodeStateHolder -> nodeStateHolder.nodeState == NodeState.FETCHING);
        }
    }

    private class AsyncNodeFetch extends AbstractRunnable {

        private final Client client;
        private final RerouteService rerouteService;
        private final DiscoveryNode discoveryNode;
        private final NodeStateHolder nodeStateHolder;

        AsyncNodeFetch(Client client, RerouteService rerouteService, DiscoveryNode discoveryNode, NodeStateHolder nodeStateHolder) {
            this.client = client;
            this.rerouteService = rerouteService;
            this.discoveryNode = discoveryNode;
            this.nodeStateHolder = nodeStateHolder;
        }

        @Override
        protected void doRun() {
            client.execute(
                FrozenCacheInfoAction.INSTANCE,
                new FrozenCacheInfoAction.Request(discoveryNode),
                new ActionListener<FrozenCacheInfoResponse>() {
                    @Override
                    public void onResponse(FrozenCacheInfoResponse response) {
                        updateEntry(response.hasFrozenCache() ? NodeState.HAS_CACHE : NodeState.NO_CACHE);
                        rerouteService.reroute("frozen cache state retrieved", Priority.LOW, ActionListener.wrap(() -> {}));
                    }

                    @Override
                    public void onFailure(Exception e) {
                        retryOrRecordFailure(e);
                    }
                }
            );
        }

        @Override
        public void onFailure(Exception e) {
            logger.debug(new ParameterizedMessage("--> failed fetching frozen cache info from [{}]", discoveryNode), e);
            // Failed even to execute the nodes info action, just give up
            updateEntry(NodeState.FAILED);
        }

        private void retryOrRecordFailure(Exception e) {
            final boolean shouldRetry;
            synchronized (mutex) {
                shouldRetry = nodeStates.get(discoveryNode) == nodeStateHolder;
            }
            logger.debug(
                new ParameterizedMessage("failed to retrieve node settings from node {}, shouldRetry={}", discoveryNode, shouldRetry),
                e
            );
            if (shouldRetry) {
                // failure is likely something like a CircuitBreakingException, so there's no sense in an immediate retry
                client.threadPool().scheduleUnlessShuttingDown(TimeValue.timeValueSeconds(1), ThreadPool.Names.SAME, AsyncNodeFetch.this);
            } else {
                updateEntry(NodeState.FAILED);
            }
        }

        private void updateEntry(NodeState nodeState) {
            assert nodeStateHolder.nodeState == NodeState.FETCHING : discoveryNode + " already set to " + nodeStateHolder.nodeState;
            assert nodeState != NodeState.FETCHING && nodeState != NodeState.UNKNOWN : "cannot set " + discoveryNode + " to " + nodeState;
            logger.trace("updating entry for {} to {}", discoveryNode, nodeState);
            nodeStateHolder.nodeState = nodeState;
        }

    }

    /**
     * Records whether a particular data node does/doesn't have a nonzero frozen cache.
     */
    private static class NodeStateHolder {
        volatile NodeState nodeState = NodeState.FETCHING;
    }

    /**
     * The state of the retrieval of the frozen cache info on a node
     */
    public enum NodeState {
        /**
         * The node is unknown.
         */
        UNKNOWN,

        /**
         * A request to retrieve the state of the node is in flight.
         */
        FETCHING,

        /**
         * The node has a frozen cache configured.
         */
        HAS_CACHE,

        /**
         * The node has no frozen cache configured.
         */
        NO_CACHE,

        /**
         * The retrieval completely failed, perhaps because we're no longer the master, or the node left the cluster.
         */
        FAILED,
    }

}
