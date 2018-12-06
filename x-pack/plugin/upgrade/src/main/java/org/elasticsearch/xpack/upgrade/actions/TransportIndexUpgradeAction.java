/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.upgrade.actions;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.upgrade.actions.IndexUpgradeAction;
import org.elasticsearch.xpack.upgrade.IndexUpgradeService;

public class TransportIndexUpgradeAction extends TransportMasterNodeAction<IndexUpgradeAction.Request, BulkByScrollResponse> {

    private final IndexUpgradeService indexUpgradeService;

    @Inject
    public TransportIndexUpgradeAction(TransportService transportService, ClusterService clusterService,
                                       ThreadPool threadPool, ActionFilters actionFilters,
                                       IndexUpgradeService indexUpgradeService,
                                       IndexNameExpressionResolver indexNameExpressionResolver) {
        super(IndexUpgradeAction.NAME, transportService, clusterService, threadPool, actionFilters,
            IndexUpgradeAction.Request::new, indexNameExpressionResolver);
        this.indexUpgradeService = indexUpgradeService;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.GENERIC;
    }

    @Override
    protected BulkByScrollResponse newResponse() {
        return new BulkByScrollResponse();
    }

    @Override
    protected ClusterBlockException checkBlock(IndexUpgradeAction.Request request, ClusterState state) {
        // Cluster is not affected but we look up repositories in metadata
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }

    @Override
    protected final void masterOperation(Task task, IndexUpgradeAction.Request request, ClusterState state,
                                         ActionListener<BulkByScrollResponse> listener) {
        TaskId taskId = new TaskId(clusterService.localNode().getId(), task.getId());
        indexUpgradeService.upgrade(taskId, request.index(), state, listener);
    }

    @Override
    protected final void masterOperation(IndexUpgradeAction.Request request, ClusterState state,
                                         ActionListener<BulkByScrollResponse> listener) {
        throw new UnsupportedOperationException("the task parameter is required");
    }

}
