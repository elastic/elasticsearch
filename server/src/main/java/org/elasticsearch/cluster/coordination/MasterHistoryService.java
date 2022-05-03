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
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.admin.cluster.coordination.MasterHistoryAction;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.ConnectionProfile;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportService;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This service provides access to this node's view of the master history, as well as access to other nodes' view of master stability.
 */
public class MasterHistoryService {
    private final TransportService transportService;
    private final MasterHistory localMasterHistory;
    private final ClusterService clusterService;

    /*
     * This is a map of a node to the view of master history it has. This is populated asynchronously and is not guaranteed to have an
     * entry for every node.
     */
    private final Map<DiscoveryNode, List<DiscoveryNode>> nodeToHistoryMap = new ConcurrentHashMap<>();
    private static final Logger logger = LogManager.getLogger(MasterHistoryService.class);

    public MasterHistoryService(TransportService transportService, ThreadPool threadPool, ClusterService clusterService) {
        this.transportService = transportService;
        this.localMasterHistory = new MasterHistory(threadPool, clusterService);
        this.clusterService = clusterService;
    }

    /**
     * This returns the MasterHistory as seen from this node. The returned MasterHistory will be automatically updated whenever the
     * ClusterState on this node is updated with new information about the master.
     * @return The MasterHistory from this node's point of view. This MasterHistory object will be updated whenever the ClusterState changes
     */
    public MasterHistory getLocalMasterHistory() {
        return localMasterHistory;
    }

    /**
     * This method returns a static view of the MasterHistory on the node given. This MasterHistory is static in that it will not be
     * updated even if the ClusterState is updated on this node or the remote node. The history is retrieved asynchronously, and only if
     * requestRemoteMasterHistory has been called for this node. If anything has gone wrong fetching it, or if it has not been fetched in
     * time for the current request, the returned value will be null.
     * @param node The node whose MasterHistory we want
     * @return The MasterHistory from the remote node's point of view. This MasterHistory object will not be updated with future changes
     */
    @Nullable
    public List<DiscoveryNode> getRemoteMasterHistory(DiscoveryNode node) {
        return nodeToHistoryMap.get(node);
    }

    /**
     * This method attempts to fetch the master history from the requested node. If we are able to successfully fetch it, it will be
     * available in a later call to getRemoteMasterHistory. The client is not notified if or when the remote history is successfully
     * retrieved. This method only fetches the remote master history once, and it is never updated unless this method is called again. If
     * two calls are made to this method, the response of one will overwrite the response of the other (with no guarantee of the ordering
     * of responses).
     * This is a remote call, so clients should avoid calling it any more often than necessary.
     * @param node The node whose view of the master history we want to fetch
     */
    public void requestRemoteMasterHistory(DiscoveryNode node) {
        long startTime = System.nanoTime();
        transportService.openConnection(
            // Note: This connection is explicitly closed onResponse or onFailure of the request below
            node,
            ConnectionProfile.buildDefaultConnectionProfile(clusterService.getSettings()),
            new ActionListener<>() {
                @Override
                public void onResponse(Transport.Connection connection) {
                    logger.trace("Opened connection to {}, making master history request", node);
                    transportService.sendRequest(
                        node,
                        MasterHistoryAction.NAME,
                        new MasterHistoryAction.Request(),
                        new ActionListenerResponseHandler<>(new ActionListener<>() {

                            @Override
                            public void onResponse(MasterHistoryAction.Response response) {
                                connection.close();
                                long endTime = System.nanoTime();
                                logger.trace("Received history from {} in {}", node, TimeValue.timeValueNanos(endTime - startTime));
                                nodeToHistoryMap.put(node, response.getMasterHistory());
                            }

                            @Override
                            public void onFailure(Exception e) {
                                connection.close();
                                logger.error("Exception in master history request to master node", e);
                            }
                        }, MasterHistoryAction.Response::new)
                    );
                }

                @Override
                public void onFailure(Exception e) {
                    logger.error("Exception connecting to master node", e);
                }
            }
        );
    }
}
