/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.TransportMasterNodeReadAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.ParentTaskAssigningClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ml.action.ResultsIndexUpgradeAction;
import org.elasticsearch.xpack.ml.ResultsIndexUpgradeService;

import static org.elasticsearch.xpack.ml.ResultsIndexUpgradeService.checkInternalIndexVersion;

public class TransportResultsIndexUpgradeAction
    extends TransportMasterNodeReadAction<ResultsIndexUpgradeAction.Request, AcknowledgedResponse> {

    private final Client client;
    private final ResultsIndexUpgradeService resultsIndexUpgradeService;

    @Inject
    public TransportResultsIndexUpgradeAction(TransportService transportService, ClusterService clusterService,
                                              ThreadPool threadPool, ActionFilters actionFilters, Client client,
                                              IndexNameExpressionResolver indexNameExpressionResolver) {
        super(ResultsIndexUpgradeAction.NAME, transportService, clusterService, threadPool,
            actionFilters, ResultsIndexUpgradeAction.Request::new, indexNameExpressionResolver);
        this.client = client;
        this.resultsIndexUpgradeService = new ResultsIndexUpgradeService(indexNameExpressionResolver,
            logger,
            executor(),
            indexMetadata -> checkInternalIndexVersion(indexMetadata) == false);
    }

    @Override
    protected void masterOperation(Task task, ResultsIndexUpgradeAction.Request request, ClusterState state,
                                   ActionListener<AcknowledgedResponse> listener) {
        TaskId taskId = new TaskId(clusterService.localNode().getId(), task.getId());
        ParentTaskAssigningClient parentAwareClient = new ParentTaskAssigningClient(client, taskId);
        try {
            resultsIndexUpgradeService.upgrade(parentAwareClient, request, state, listener);
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    @Override
    protected final void masterOperation(ResultsIndexUpgradeAction.Request request, ClusterState state,
                                         ActionListener<AcknowledgedResponse> listener) {
        throw new UnsupportedOperationException("the task parameter is required");
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected AcknowledgedResponse newResponse() {
        return new AcknowledgedResponse();
    }

    @Override
    protected ClusterBlockException checkBlock(ResultsIndexUpgradeAction.Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }
}
