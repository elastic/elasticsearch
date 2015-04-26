/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.action.admin.cluster.tasks;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeReadOperationAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

/**
 */
public class TransportPendingClusterTasksAction extends TransportMasterNodeReadOperationAction<PendingClusterTasksRequest, PendingClusterTasksResponse> {

    private final ClusterService clusterService;

    @Inject
    public TransportPendingClusterTasksAction(Settings settings, TransportService transportService, ClusterService clusterService, ThreadPool threadPool, ActionFilters actionFilters) {
        super(settings, PendingClusterTasksAction.NAME, transportService, clusterService, threadPool, actionFilters, PendingClusterTasksRequest.class);
        this.clusterService = clusterService;
    }

    @Override
    protected String executor() {
        // very lightweight operation in memory, no need to fork to a thread
        return ThreadPool.Names.SAME;
    }

    @Override
    protected ClusterBlockException checkBlock(PendingClusterTasksRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }

    @Override
    protected PendingClusterTasksResponse newResponse() {
        return new PendingClusterTasksResponse();
    }

    @Override
    protected void masterOperation(PendingClusterTasksRequest request, ClusterState state, ActionListener<PendingClusterTasksResponse> listener) throws ElasticsearchException {
        listener.onResponse(new PendingClusterTasksResponse(clusterService.pendingTasks()));
    }
}
