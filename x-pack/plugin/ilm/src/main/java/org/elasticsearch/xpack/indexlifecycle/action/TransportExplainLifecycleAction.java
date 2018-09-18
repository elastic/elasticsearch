/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.indexlifecycle.action;

import org.elasticsearch.ElasticsearchParseException;
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
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.indexlifecycle.ExplainLifecycleRequest;
import org.elasticsearch.xpack.core.indexlifecycle.ExplainLifecycleResponse;
import org.elasticsearch.xpack.core.indexlifecycle.IndexLifecycleExplainResponse;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.indexlifecycle.LifecycleSettings;
import org.elasticsearch.xpack.core.indexlifecycle.PhaseExecutionInfo;
import org.elasticsearch.xpack.core.indexlifecycle.action.ExplainLifecycleAction;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class TransportExplainLifecycleAction
        extends TransportClusterInfoAction<ExplainLifecycleRequest, ExplainLifecycleResponse> {

    private final NamedXContentRegistry xContentRegistry;

    @Inject
    public TransportExplainLifecycleAction(Settings settings, TransportService transportService, ClusterService clusterService,
                                           ThreadPool threadPool, ActionFilters actionFilters,
                                           IndexNameExpressionResolver indexNameExpressionResolver,
                                           NamedXContentRegistry xContentRegistry) {
        super(settings, ExplainLifecycleAction.NAME, transportService, clusterService, threadPool, actionFilters,
                ExplainLifecycleRequest::new, indexNameExpressionResolver);
        this.xContentRegistry = xContentRegistry;
    }

    @Override
    protected ExplainLifecycleResponse newResponse() {
        return new ExplainLifecycleResponse();
    }

    @Override
    protected String executor() {
        // very lightweight operation, no need to fork
        return ThreadPool.Names.SAME;
    }

    @Override
    protected ClusterBlockException checkBlock(ExplainLifecycleRequest request, ClusterState state) {
        return state.blocks().indicesBlockedException(ClusterBlockLevel.METADATA_READ,
                indexNameExpressionResolver.concreteIndexNames(state, request));
    }

    @Override
    protected void doMasterOperation(ExplainLifecycleRequest request, String[] concreteIndices, ClusterState state,
            ActionListener<ExplainLifecycleResponse> listener) {
        Map<String, IndexLifecycleExplainResponse> indexReponses = new HashMap<>();
        for (String index : concreteIndices) {
            IndexMetaData idxMetadata = state.metaData().index(index);
            Settings idxSettings = idxMetadata.getSettings();
            String policyName = LifecycleSettings.LIFECYCLE_NAME_SETTING.get(idxSettings);
            String currentPhase = LifecycleSettings.LIFECYCLE_PHASE_SETTING.get(idxSettings);
            // parse existing phase steps from the phase definition in the index settings
            String phaseDef = idxSettings.get(LifecycleSettings.LIFECYCLE_PHASE_DEFINITION);
            PhaseExecutionInfo phaseExecutionInfo = null;
            if (Strings.isNullOrEmpty(phaseDef) == false) {
                try (XContentParser parser = JsonXContent.jsonXContent.createParser(xContentRegistry,
                        DeprecationHandler.THROW_UNSUPPORTED_OPERATION, phaseDef)) {
                    phaseExecutionInfo = PhaseExecutionInfo.parse(parser, currentPhase);
                } catch (IOException e) {
                    listener.onFailure(new ElasticsearchParseException(
                        "failed to parse [" + LifecycleSettings.LIFECYCLE_PHASE_DEFINITION + "] for index [" + index + "]", e));
                    return;
                }
            }
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
                    new BytesArray(LifecycleSettings.LIFECYCLE_STEP_INFO_SETTING.get(idxSettings)),
                    phaseExecutionInfo);
            } else {
                indexResponse = IndexLifecycleExplainResponse.newUnmanagedIndexResponse(index);
            }
            indexReponses.put(indexResponse.getIndex(), indexResponse);
        }
        listener.onResponse(new ExplainLifecycleResponse(indexReponses));
    }

}
