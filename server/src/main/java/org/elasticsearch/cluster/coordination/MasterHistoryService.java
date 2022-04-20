/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.coordination;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.cluster.coordination.MasterHistoryAction;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.concurrent.ExecutionException;

/**
 * This service provides access to this node's view of the master history, as well as access to other nodes' view of master stability.
 */
public class MasterHistoryService {
    private final NodeClient client;
    private final MutableMasterHistory localMasterHistory;

    public MasterHistoryService(NodeClient client, ThreadPool threadPool, ClusterService clusterService) {
        this.client = client;
        this.localMasterHistory = new MutableMasterHistory(threadPool, clusterService);
    }

    /**
     * This returns the MasterHistory as seen from this node. The returned MasterHistory will be automatically updated whenever the
     * ClusterState on this node is updated with new information about the master.
     * @return The MasterHistory from this node's point of view. This MasterHistory object will be updated whenever the ClusterState changes
     */
    public MutableMasterHistory getLocalMasterHistory() {
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
    public MasterHistory getRemoteMasterHistory(DiscoveryNode node) throws ExecutionException, InterruptedException {
        MasterHistoryAction.Request getMasterHistoryRequest = new MasterHistoryAction.Request();
        getMasterHistoryRequest.remoteAddress(node.getAddress().address());
        ActionFuture<MasterHistoryAction.Response> result = client.execute(MasterHistoryAction.INSTANCE, getMasterHistoryRequest);
        MasterHistoryAction.Response response = result.get();
        return response.getMasterHistory();
    }
}
