/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ilm;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.shrink.ResizeRequest;
import org.elasticsearch.action.admin.indices.shrink.ResizeType;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.ProjectState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.LifecycleExecutionState;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;

import java.util.Objects;
import java.util.function.BiFunction;

/**
 * Clones the index with the specified settings, using the name that was generated in a previous {@link GenerateUniqueIndexNameStep} step.
 */
public class CloneIndexStep extends AsyncActionStep {

    public static final String NAME = "clone-index";
    private static final Logger logger = LogManager.getLogger(CloneIndexStep.class);

    private final BiFunction<String, LifecycleExecutionState, String> targetIndexNameSupplier;
    private final Settings targetIndexSettings;

    public CloneIndexStep(
        StepKey key,
        StepKey nextStepKey,
        Client client,
        BiFunction<String, LifecycleExecutionState, String> targetIndexNameSupplier,
        Settings targetIndexSettings
    ) {
        super(key, nextStepKey, client);
        this.targetIndexNameSupplier = targetIndexNameSupplier;
        this.targetIndexSettings = targetIndexSettings;
    }

    @Override
    public boolean isRetryable() {
        return true;
    }

    @Override
    public void performAction(
        IndexMetadata indexMetadata,
        ProjectState currentState,
        ClusterStateObserver observer,
        ActionListener<Void> listener
    ) {
        final String targetIndexName = targetIndexNameSupplier.apply(
            indexMetadata.getIndex().getName(),
            indexMetadata.getLifecycleExecutionState()
        );
        if (currentState.metadata().index(targetIndexName) != null) {
            logger.warn(
                "skipping [{}] step for index [{}] as part of policy [{}] as the target index [{}] already exists",
                CloneIndexStep.NAME,
                indexMetadata.getIndex().getName(),
                indexMetadata.getLifecyclePolicyName(),
                targetIndexName
            );
            listener.onResponse(null);
            return;
        }

        Settings relevantTargetSettings = Settings.builder()
            .put(targetIndexSettings)
            // We add the skip setting to prevent ILM from processing the cloned index before the execution state has been copied - which
            // could happen if the shards of the cloned index take a long time to allocate.
            .put(LifecycleSettings.LIFECYCLE_SKIP, true)
            .build();
        ResizeRequest resizeRequest = new ResizeRequest(targetIndexName, indexMetadata.getIndex().getName()).masterNodeTimeout(
            TimeValue.MAX_VALUE
        );
        resizeRequest.setResizeType(ResizeType.CLONE);
        resizeRequest.getTargetIndexRequest().settings(relevantTargetSettings);

        getClient(currentState.projectId()).admin()
            .indices()
            .resizeIndex(resizeRequest, listener.delegateFailureAndWrap((l, response) -> l.onResponse(null)));
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), targetIndexSettings);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        CloneIndexStep other = (CloneIndexStep) obj;
        return super.equals(obj) && Objects.equals(targetIndexSettings, other.targetIndexSettings);
    }

}
