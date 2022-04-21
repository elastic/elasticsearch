/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.coordination;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.admin.cluster.coordination.MasterHistoryAction;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.component.LifecycleListener;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 * This service provides access to this node's view of the master history, as well as access to other nodes' view of master stability.
 */
public class MasterHistoryService {
    private final TransportService transportService;
    private final MasterHistory localMasterHistory;

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
     * updated even if the ClusterState is updated on this node or the remote node.
     * @param node The node whose MasterHistory we want
     * @return The MasterHistory from the remote node's point of view. This MasterHistory object will not be updated with future changes
     * @throws ExecutionException If an exception occurs while trying to fetch the remote node's MasterHistory
     * @throws InterruptedException If the thread is interrupted
     */
    public List<DiscoveryNode> getRemoteMasterHistory(DiscoveryNode node) throws ExecutionException, InterruptedException {
        MasterHistoryAction.Request getMasterHistoryRequest = new MasterHistoryAction.Request();
        final MasterHistoryAction.Response[] responseArray = new MasterHistoryAction.Response[1];
        final Exception[] exceptionArray = new Exception[1];
        final Object lock = new Object();
        transportService.connectToNode(node, new ActionListener<>() {
            @Override
            public void onResponse(Releasable releasable) {
                transportService.sendRequest(
                    node,
                    MasterHistoryAction.NAME,
                    getMasterHistoryRequest,
                    new ActionListenerResponseHandler<>(new ActionListener<>() {

                        @Override
                        public void onResponse(MasterHistoryAction.Response response) {
                            responseArray[0] = response;
                            synchronized (lock) {
                                lock.notifyAll();
                            }
                        }

                        @Override
                        public void onFailure(Exception e) {
                            exceptionArray[0] = e;
                            synchronized (lock) {
                                lock.notifyAll();
                            }
                        }
                    }, MasterHistoryAction.Response::new)
                );
            }

            @Override
            public void onFailure(Exception e) {
                exceptionArray[0] = e;
                synchronized (lock) {
                    lock.notifyAll();
                }
            }
        });

        while (responseArray[0] == null && exceptionArray[0] == null) {
            synchronized (lock) {
                if (responseArray[0] == null && exceptionArray[0] == null) {
                    lock.wait(1000);
                }
            }
        }
        if (exceptionArray[0] != null) {
            throw new ExecutionException(exceptionArray[0]);
        }
        return responseArray[0].getMasterHistory();
    }
}
