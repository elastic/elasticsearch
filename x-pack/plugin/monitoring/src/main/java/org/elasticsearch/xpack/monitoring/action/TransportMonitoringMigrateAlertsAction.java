/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.monitoring.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.monitoring.action.MonitoringMigrateAlertsAction;
import org.elasticsearch.xpack.core.monitoring.action.MonitoringMigrateAlertsRequest;
import org.elasticsearch.xpack.core.monitoring.action.MonitoringMigrateAlertsResponse;

import java.io.IOException;

public class TransportMonitoringMigrateAlertsAction extends TransportMasterNodeAction<MonitoringMigrateAlertsRequest,
    MonitoringMigrateAlertsResponse> {

    @Inject
    public TransportMonitoringMigrateAlertsAction(TransportService transportService, ClusterService clusterService, ThreadPool threadPool,
                                                  ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver) {
        super(MonitoringMigrateAlertsAction.NAME, transportService, clusterService, threadPool, actionFilters,
            MonitoringMigrateAlertsRequest::new, indexNameExpressionResolver);
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.MANAGEMENT;
    }

    @Override
    protected MonitoringMigrateAlertsResponse read(StreamInput in) throws IOException {
        return new MonitoringMigrateAlertsResponse(in);
    }

    @Override
    protected void masterOperation(Task task, MonitoringMigrateAlertsRequest request, ClusterState state,
                                   ActionListener<MonitoringMigrateAlertsResponse> listener) throws Exception {

    }

    @Override
    protected ClusterBlockException checkBlock(MonitoringMigrateAlertsRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }
}
