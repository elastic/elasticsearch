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
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.component.LifecycleListener;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.ConnectionProfile;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportService;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This service provides access to this node's view of the master history, as well as access to other nodes' view of master stability.
 */
public class MasterHistoryService implements ClusterStateListener {
    private final TransportService transportService;
    private final MasterHistory localMasterHistory;
    /*
     * This is a map of a node to the view of master history it has. This is populated asynchronously and is not guaranteed to have an
     * entry for every node.
     */
    private final Map<DiscoveryNode, List<DiscoveryNode>> nodeToHistoryMap = new HashMap<>();;
    private static final Logger logger = LogManager.getLogger(MasterHistoryService.class);

    public MasterHistoryService(
        TransportService transportService,
        Coordinator coordinator,
        ThreadPool threadPool,
        ClusterService clusterService
    ) {
        this.transportService = transportService;
        this.localMasterHistory = new MasterHistory(threadPool, clusterService);
        // Set the initial state for the local history once it is available:
        coordinator.addLifecycleListener(new LifecycleListener() {
            @Override
            public void afterStart() {
                localMasterHistory.clusterChanged(
                    new ClusterChangedEvent(MasterHistoryService.class.getName(), clusterService.state(), clusterService.state())
                );
            }
        });
        clusterService.addListener(this);
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
     * updated even if the ClusterState is updated on this node or the remote node. The history is retrieved asynchronously. If anything
     * has gone wrong fetching it, or if it has not been fetched in time for the current request, it will be null.
     * @param node The node whose MasterHistory we want
     * @return The MasterHistory from the remote node's point of view. This MasterHistory object will not be updated with future changes
     */
    @Nullable
    public List<DiscoveryNode> getRemoteMasterHistory(DiscoveryNode node) {
        synchronized (nodeToHistoryMap) {
            return nodeToHistoryMap.get(node);
        }
    }

    /*
     * If we detect that the same master has gone null 3 or more times, we attempt to fetch the master history as seen from that node so
     * that it is ready for future yse,
     */
    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        DiscoveryNode currentMaster = event.state().nodes().getMasterNode();
        DiscoveryNode previousMaster = event.previousState().nodes().getMasterNode();
        if (currentMaster == null && previousMaster != null) {
            if (localMasterHistory.hasSameMasterGoneNullNTimes(3)) {
                DiscoveryNode master = localMasterHistory.getMostRecentNonNullMaster();
                if (master != null) {
                    transportService.openConnection(
                        master,
                        ConnectionProfile.buildDefaultConnectionProfile(event.state().getMetadata().settings()),
                        new ActionListener<>() {
                            @Override
                            public void onResponse(Transport.Connection connection) {
                                logger.trace("Opened connection to {}, making master history request", master);
                                transportService.sendRequest(
                                    master,
                                    MasterHistoryAction.NAME,
                                    new MasterHistoryAction.Request(),
                                    new ActionListenerResponseHandler<>(new ActionListener<>() {

                                        @Override
                                        public void onResponse(MasterHistoryAction.Response response) {
                                            logger.trace("Received history from {}", master);
                                            synchronized (nodeToHistoryMap) {
                                                nodeToHistoryMap.put(master, response.getMasterHistory());
                                            }
                                        }

                                        @Override
                                        public void onFailure(Exception e) {
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
        }
    }
}
