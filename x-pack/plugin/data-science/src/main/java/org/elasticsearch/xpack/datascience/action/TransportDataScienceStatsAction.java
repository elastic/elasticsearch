/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.datascience.action;

import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.nodes.TransportNodesAction;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.datascience.action.DataScienceStatsAction;
import org.elasticsearch.xpack.datascience.DataSciencePlugin;

import java.io.IOException;
import java.util.List;

public class TransportDataScienceStatsAction extends TransportNodesAction<DataScienceStatsAction.Request, DataScienceStatsAction.Response,
    DataScienceStatsAction.NodeRequest, DataScienceStatsAction.NodeResponse> {


    @Inject
    public TransportDataScienceStatsAction(TransportService transportService, ClusterService clusterService,
                                           ThreadPool threadPool, ActionFilters actionFilters) {
        super(DataScienceStatsAction.NAME, threadPool, clusterService, transportService, actionFilters,
            DataScienceStatsAction.Request::new, DataScienceStatsAction.NodeRequest::new, ThreadPool.Names.MANAGEMENT,
            DataScienceStatsAction.NodeResponse.class);
    }

    @Override
    protected DataScienceStatsAction.Response newResponse(DataScienceStatsAction.Request request,
                                                          List<DataScienceStatsAction.NodeResponse> nodes,
                                                          List<FailedNodeException> failures) {
        return new DataScienceStatsAction.Response(clusterService.getClusterName(), nodes, failures);
    }

    @Override
    protected DataScienceStatsAction.NodeRequest newNodeRequest(DataScienceStatsAction.Request request) {
        return new DataScienceStatsAction.NodeRequest(request);
    }

    @Override
    protected DataScienceStatsAction.NodeResponse newNodeResponse(StreamInput in) throws IOException {
        return new DataScienceStatsAction.NodeResponse(in);
    }

    @Override
    protected DataScienceStatsAction.NodeResponse nodeOperation(DataScienceStatsAction.NodeRequest request, Task task) {
        DataScienceStatsAction.NodeResponse statsResponse = new DataScienceStatsAction.NodeResponse(clusterService.localNode());
        statsResponse.setCumulativeCardinalityUsage(DataSciencePlugin.cumulativeCardUsage.get());
        return statsResponse;
    }

}
