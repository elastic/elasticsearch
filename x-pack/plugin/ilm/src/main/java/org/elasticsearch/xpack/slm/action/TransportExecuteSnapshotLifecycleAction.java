/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.slm.action;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.slm.SnapshotLifecycleMetadata;
import org.elasticsearch.xpack.core.slm.SnapshotLifecyclePolicyMetadata;
import org.elasticsearch.xpack.core.slm.action.ExecuteSnapshotLifecycleAction;
import org.elasticsearch.xpack.slm.SnapshotLifecycleService;
import org.elasticsearch.xpack.slm.SnapshotLifecycleTask;
import org.elasticsearch.xpack.slm.history.SnapshotHistoryStore;

import java.util.Optional;

public class TransportExecuteSnapshotLifecycleAction extends TransportMasterNodeAction<
    ExecuteSnapshotLifecycleAction.Request,
    ExecuteSnapshotLifecycleAction.Response> {

    private final Client client;
    private final SnapshotHistoryStore historyStore;

    @Inject
    public TransportExecuteSnapshotLifecycleAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Client client,
        SnapshotHistoryStore historyStore
    ) {
        super(
            ExecuteSnapshotLifecycleAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            ExecuteSnapshotLifecycleAction.Request::new,
            indexNameExpressionResolver,
            ExecuteSnapshotLifecycleAction.Response::new,
            ThreadPool.Names.GENERIC
        );
        this.client = client;
        this.historyStore = historyStore;
    }

    @Override
    protected void masterOperation(
        final Task task,
        final ExecuteSnapshotLifecycleAction.Request request,
        final ClusterState state,
        final ActionListener<ExecuteSnapshotLifecycleAction.Response> listener
    ) {
        try {
            final String policyId = request.getLifecycleId();
            SnapshotLifecycleMetadata snapMeta = state.metadata().custom(SnapshotLifecycleMetadata.TYPE, SnapshotLifecycleMetadata.EMPTY);
            SnapshotLifecyclePolicyMetadata policyMetadata = snapMeta.getSnapshotConfigurations().get(policyId);
            if (policyMetadata == null) {
                listener.onFailure(new IllegalArgumentException("no such snapshot lifecycle policy [" + policyId + "]"));
                return;
            }

            final Optional<String> snapshotName = SnapshotLifecycleTask.maybeTakeSnapshot(
                SnapshotLifecycleService.getJobId(policyMetadata),
                client,
                clusterService,
                historyStore
            );
            if (snapshotName.isPresent()) {
                listener.onResponse(new ExecuteSnapshotLifecycleAction.Response(snapshotName.get()));
            } else {
                listener.onFailure(new ElasticsearchException("failed to execute snapshot lifecycle policy [" + policyId + "]"));
            }
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    @Override
    protected ClusterBlockException checkBlock(ExecuteSnapshotLifecycleAction.Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }
}
