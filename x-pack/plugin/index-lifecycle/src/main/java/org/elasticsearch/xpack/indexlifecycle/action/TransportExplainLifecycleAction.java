/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.indexlifecycle.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.info.TransportClusterInfoAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.indexlifecycle.LifecycleSettings;
import org.elasticsearch.xpack.core.indexlifecycle.action.ExplainLifecycleAction;
import org.elasticsearch.xpack.core.indexlifecycle.action.ExplainLifecycleAction.Request;
import org.elasticsearch.xpack.core.indexlifecycle.action.ExplainLifecycleAction.Response;
import org.elasticsearch.xpack.core.indexlifecycle.action.IndexLifecycleExplainResponse;

import java.util.ArrayList;
import java.util.List;

public class TransportExplainLifecycleAction
        extends TransportClusterInfoAction<ExplainLifecycleAction.Request, ExplainLifecycleAction.Response> {

    @Inject
    public TransportExplainLifecycleAction(Settings settings, TransportService transportService, ClusterService clusterService,
            ThreadPool threadPool, ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver) {
        super(settings, ExplainLifecycleAction.NAME, transportService, clusterService, threadPool, actionFilters,
                ExplainLifecycleAction.Request::new, indexNameExpressionResolver);
    }

    @Override
    protected Response newResponse() {
        return new ExplainLifecycleAction.Response();
    }

    @Override
    protected String executor() {
        // very lightweight operation, no need to fork
        return ThreadPool.Names.SAME;
    }

    @Override
    protected ClusterBlockException checkBlock(ExplainLifecycleAction.Request request, ClusterState state) {
        return state.blocks().indicesBlockedException(ClusterBlockLevel.METADATA_READ,
                indexNameExpressionResolver.concreteIndexNames(state, request));
    }

    @Override
    protected void doMasterOperation(Request request, String[] concreteIndices, ClusterState state, ActionListener<Response> listener) {
        List<IndexLifecycleExplainResponse> indexReponses = new ArrayList<>();
        for (String index : concreteIndices) {
            IndexMetaData idxMetadata = state.metaData().index(index);
            Settings idxSettings = idxMetadata.getSettings();
            String policyName = LifecycleSettings.LIFECYCLE_NAME_SETTING.get(idxSettings);
            final IndexLifecycleExplainResponse indexResponse;
            if (Strings.hasLength(policyName)) {
                indexResponse = IndexLifecycleExplainResponse.newManagedIndexResponse(index, policyName,
                        LifecycleSettings.LIFECYCLE_SKIP_SETTING.get(idxSettings),
                        LifecycleSettings.LIFECYCLE_INDEX_CREATION_DATE_SETTING.get(idxSettings),
                        LifecycleSettings.LIFECYCLE_PHASE_SETTING.get(idxSettings),
                        LifecycleSettings.LIFECYCLE_ACTION_SETTING.get(idxSettings),
                        LifecycleSettings.LIFECYCLE_STEP_SETTING.get(idxSettings),
                        LifecycleSettings.LIFECYCLE_FAILED_STEP_SETTING.get(idxSettings),
                        LifecycleSettings.LIFECYCLE_PHASE_TIME_SETTING.get(idxSettings),
                        LifecycleSettings.LIFECYCLE_ACTION_TIME_SETTING.get(idxSettings),
                        LifecycleSettings.LIFECYCLE_STEP_TIME_SETTING.get(idxSettings),
                        new BytesArray(LifecycleSettings.LIFECYCLE_STEP_INFO_SETTING.get(idxSettings)));
            } else {
                indexResponse = IndexLifecycleExplainResponse.newUnmanagedIndexResponse(index);
            }
            indexReponses.add(indexResponse);
        }
        listener.onResponse(new ExplainLifecycleAction.Response(indexReponses));
    }

}
