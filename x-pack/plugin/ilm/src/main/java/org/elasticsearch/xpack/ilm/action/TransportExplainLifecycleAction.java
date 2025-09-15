/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ilm.action;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.info.TransportClusterInfoAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.LifecycleExecutionState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.ilm.ErrorStep;
import org.elasticsearch.xpack.core.ilm.ExplainLifecycleRequest;
import org.elasticsearch.xpack.core.ilm.ExplainLifecycleResponse;
import org.elasticsearch.xpack.core.ilm.IndexLifecycleExplainResponse;
import org.elasticsearch.xpack.core.ilm.LifecycleSettings;
import org.elasticsearch.xpack.core.ilm.PhaseExecutionInfo;
import org.elasticsearch.xpack.core.ilm.RolloverAction;
import org.elasticsearch.xpack.core.ilm.action.ExplainLifecycleAction;
import org.elasticsearch.xpack.ilm.IndexLifecycleService;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

import static org.elasticsearch.index.IndexSettings.LIFECYCLE_ORIGINATION_DATE;
import static org.elasticsearch.xpack.core.ilm.WaitForRolloverReadyStep.applyDefaultConditions;

public class TransportExplainLifecycleAction extends TransportClusterInfoAction<ExplainLifecycleRequest, ExplainLifecycleResponse> {

    private final NamedXContentRegistry xContentRegistry;
    private final IndexLifecycleService indexLifecycleService;

    @Inject
    public TransportExplainLifecycleAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        NamedXContentRegistry xContentRegistry,
        IndexLifecycleService indexLifecycleService
    ) {
        super(
            ExplainLifecycleAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            ExplainLifecycleRequest::new,
            indexNameExpressionResolver,
            ExplainLifecycleResponse::new
        );
        this.xContentRegistry = xContentRegistry;
        this.indexLifecycleService = indexLifecycleService;
    }

    @Override
    protected void doMasterOperation(
        Task task,
        ExplainLifecycleRequest request,
        String[] concreteIndices,
        ClusterState state,
        ActionListener<ExplainLifecycleResponse> listener
    ) {
        boolean rolloverOnlyIfHasDocuments = LifecycleSettings.LIFECYCLE_ROLLOVER_ONLY_IF_HAS_DOCUMENTS_SETTING.get(
            state.metadata().settings()
        );
        Map<String, IndexLifecycleExplainResponse> indexResponses = new TreeMap<>();
        for (String index : concreteIndices) {
            final IndexLifecycleExplainResponse indexResponse;
            try {
                indexResponse = getIndexLifecycleExplainResponse(
                    index,
                    state.metadata(),
                    request.onlyErrors(),
                    request.onlyManaged(),
                    indexLifecycleService,
                    xContentRegistry,
                    rolloverOnlyIfHasDocuments
                );
            } catch (IOException e) {
                listener.onFailure(new ElasticsearchParseException("failed to parse phase definition for index [" + index + "]", e));
                return;
            }

            if (indexResponse != null) {
                indexResponses.put(indexResponse.getIndex(), indexResponse);
            }
        }
        listener.onResponse(new ExplainLifecycleResponse(indexResponses));
    }

    @Nullable
    static IndexLifecycleExplainResponse getIndexLifecycleExplainResponse(
        String indexName,
        Metadata metadata,
        boolean onlyErrors,
        boolean onlyManaged,
        IndexLifecycleService indexLifecycleService,
        NamedXContentRegistry xContentRegistry,
        boolean rolloverOnlyIfHasDocuments
    ) throws IOException {
        IndexMetadata indexMetadata = metadata.index(indexName);
        Settings idxSettings = indexMetadata.getSettings();
        LifecycleExecutionState lifecycleState = indexMetadata.getLifecycleExecutionState();
        String policyName = indexMetadata.getLifecyclePolicyName();
        String currentPhase = lifecycleState.phase();
        String stepInfo = lifecycleState.stepInfo();
        String previousStepInfo = lifecycleState.previousStepInfo();
        BytesArray stepInfoBytes = null;
        if (stepInfo != null) {
            stepInfoBytes = new BytesArray(stepInfo);
        }
        BytesArray previousStepInfoBytes = null;
        if (previousStepInfo != null) {
            previousStepInfoBytes = new BytesArray(previousStepInfo);
        }
        Long indexCreationDate = indexMetadata.getCreationDate();

        // parse existing phase steps from the phase definition in the index settings
        String phaseDef = lifecycleState.phaseDefinition();
        PhaseExecutionInfo phaseExecutionInfo = null;
        if (Strings.isNullOrEmpty(phaseDef) == false) {
            try (
                XContentParser parser = JsonXContent.jsonXContent.createParser(
                    XContentParserConfiguration.EMPTY.withRegistry(xContentRegistry),
                    phaseDef
                )
            ) {
                phaseExecutionInfo = PhaseExecutionInfo.parse(parser, currentPhase);

                // Try to add default rollover conditions to the response.
                var phase = phaseExecutionInfo.getPhase();
                if (phase != null) {
                    var rolloverAction = (RolloverAction) phase.getActions().get(RolloverAction.NAME);
                    if (rolloverAction != null) {
                        var conditions = applyDefaultConditions(rolloverAction.getConditions(), rolloverOnlyIfHasDocuments);
                        phase.getActions().put(RolloverAction.NAME, new RolloverAction(conditions));
                    }
                }
            }
        }

        final IndexLifecycleExplainResponse indexResponse;
        if (metadata.isIndexManagedByILM(indexMetadata)) {
            // If this is requesting only errors, only include indices in the error step or which are using a nonexistent policy
            if (onlyErrors == false
                || (ErrorStep.NAME.equals(lifecycleState.step()) || indexLifecycleService.policyExists(policyName) == false)) {
                Long originationDate = idxSettings.getAsLong(LIFECYCLE_ORIGINATION_DATE, -1L);
                indexResponse = IndexLifecycleExplainResponse.newManagedIndexResponse(
                    indexName,
                    indexCreationDate,
                    policyName,
                    originationDate != -1L ? originationDate : lifecycleState.lifecycleDate(),
                    lifecycleState.phase(),
                    lifecycleState.action(),
                    // treat a missing policy as if the index is in the error step
                    indexLifecycleService.policyExists(policyName) == false ? ErrorStep.NAME : lifecycleState.step(),
                    lifecycleState.failedStep(),
                    lifecycleState.isAutoRetryableError(),
                    lifecycleState.failedStepRetryCount(),
                    lifecycleState.phaseTime(),
                    lifecycleState.actionTime(),
                    lifecycleState.stepTime(),
                    lifecycleState.snapshotRepository(),
                    lifecycleState.snapshotName(),
                    lifecycleState.shrinkIndexName(),
                    stepInfoBytes,
                    previousStepInfoBytes,
                    phaseExecutionInfo,
                    LifecycleSettings.LIFECYCLE_SKIP_SETTING.get(idxSettings)
                );
            } else {
                indexResponse = null;
            }
        } else if (onlyManaged == false && onlyErrors == false) {
            indexResponse = IndexLifecycleExplainResponse.newUnmanagedIndexResponse(indexName);
        } else {
            indexResponse = null;
        }
        return indexResponse;
    }
}
