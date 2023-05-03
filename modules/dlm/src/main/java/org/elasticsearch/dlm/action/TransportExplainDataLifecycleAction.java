/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.dlm.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.rollover.RolloverInfo;
import org.elasticsearch.action.dlm.ExplainIndexDataLifecycle;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeReadAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.DataLifecycle;
import org.elasticsearch.cluster.metadata.DataLifecyclePrivilegesCheck;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.dlm.DataLifecycleErrorStore;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.ArrayList;
import java.util.List;

/**
 * Transport action handling the explain DLM lifecycle requests for one or more DLM managed indices.
 */
public class TransportExplainDataLifecycleAction extends TransportMasterNodeReadAction<
    ExplainDataLifecycleAction.Request,
    ExplainDataLifecycleAction.Response> {

    private final DataLifecycleErrorStore errorStore;
    private final DataLifecyclePrivilegesCheck privilegesCheck;

    @Inject
    public TransportExplainDataLifecycleAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        DataLifecycleErrorStore dataLifecycleServiceErrorStore,
        DataLifecyclePrivilegesCheck privilegesCheck
    ) {
        super(
            ExplainDataLifecycleAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            ExplainDataLifecycleAction.Request::new,
            indexNameExpressionResolver,
            ExplainDataLifecycleAction.Response::new,
            ThreadPool.Names.MANAGEMENT
        );
        this.errorStore = dataLifecycleServiceErrorStore;
        this.privilegesCheck = privilegesCheck;
    }

    @Override
    protected void masterOperation(
        Task task,
        ExplainDataLifecycleAction.Request request,
        ClusterState state,
        ActionListener<ExplainDataLifecycleAction.Response> listener
    ) {
        // Empty array to force cluster privilege check, but no need for index privileges checks
        // TODO this is hacky, and we don't do this in the final implementation
        privilegesCheck.checkCanConfigure(Strings.EMPTY_ARRAY, ActionListener.wrap(ignored -> {
            String[] concreteIndices = indexNameExpressionResolver.concreteIndexNames(state, request);
            List<ExplainIndexDataLifecycle> explainIndices = new ArrayList<>(concreteIndices.length);
            Metadata metadata = state.metadata();
            for (String index : concreteIndices) {
                IndexAbstraction indexAbstraction = metadata.getIndicesLookup().get(index);
                if (indexAbstraction == null) {
                    continue;
                }
                IndexMetadata idxMetadata = metadata.index(index);
                if (idxMetadata == null) {
                    continue;
                }
                DataStream parentDataStream = indexAbstraction.getParentDataStream();
                if (parentDataStream == null || parentDataStream.isIndexManagedByDLM(idxMetadata.getIndex(), metadata::index) == false) {
                    explainIndices.add(new ExplainIndexDataLifecycle(index, false, null, null, null, null, null));
                    continue;
                }

                RolloverInfo rolloverInfo = idxMetadata.getRolloverInfos().get(parentDataStream.getName());
                TimeValue generationDate = parentDataStream.getGenerationLifecycleDate(idxMetadata);
                ExplainIndexDataLifecycle explainIndexDataLifecycle = new ExplainIndexDataLifecycle(
                    index,
                    true,
                    idxMetadata.getCreationDate(),
                    rolloverInfo == null ? null : rolloverInfo.getTime(),
                    generationDate,
                    parentDataStream.getLifecycle(),
                    errorStore.getError(index)
                );
                explainIndices.add(explainIndexDataLifecycle);
            }

            ClusterSettings clusterSettings = clusterService.getClusterSettings();
            listener.onResponse(
                new ExplainDataLifecycleAction.Response(
                    explainIndices,
                    request.includeDefaults() && DataLifecycle.isEnabled()
                        ? clusterSettings.get(DataLifecycle.CLUSTER_DLM_DEFAULT_ROLLOVER_SETTING)
                        : null
                )
            );
        }, listener::onFailure));
    }

    @Override
    protected ClusterBlockException checkBlock(ExplainDataLifecycleAction.Request request, ClusterState state) {
        return state.blocks()
            .indicesBlockedException(ClusterBlockLevel.METADATA_READ, indexNameExpressionResolver.concreteIndexNames(state, request));
    }
}
