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
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;

import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Resizes an index with the specified settings, using the name that was generated in a previous {@link GenerateUniqueIndexNameStep} step.
 */
public class ResizeIndexStep extends AsyncActionStep {
    private static final Logger logger = LogManager.getLogger(ResizeIndexStep.class);

    private final ResizeType resizeType;
    private final BiFunction<String, LifecycleExecutionState, String> targetIndexNameSupplier;
    private final Function<IndexMetadata, Settings> targetIndexSettingsSupplier;
    @Nullable
    private final ByteSizeValue maxPrimaryShardSize;

    public ResizeIndexStep(
        StepKey key,
        StepKey nextStepKey,
        Client client,
        ResizeType resizeType,
        BiFunction<String, LifecycleExecutionState, String> targetIndexNameSupplier,
        Function<IndexMetadata, Settings> targetIndexSettingsSupplier,
        @Nullable ByteSizeValue maxPrimaryShardSize
    ) {
        super(key, nextStepKey, client);
        this.resizeType = resizeType;
        this.targetIndexNameSupplier = targetIndexNameSupplier;
        this.targetIndexSettingsSupplier = targetIndexSettingsSupplier;
        this.maxPrimaryShardSize = maxPrimaryShardSize;
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
        LifecycleExecutionState lifecycleState = indexMetadata.getLifecycleExecutionState();
        if (lifecycleState.lifecycleDate() == null) {
            throw new IllegalStateException("source index [" + indexMetadata.getIndex().getName() + "] is missing lifecycle date");
        }

        final String targetIndexName = targetIndexNameSupplier.apply(indexMetadata.getIndex().getName(), lifecycleState);
        if (currentState.metadata().index(targetIndexName) != null) {
            logger.warn(
                "skipping [{}] step for index [{}] as part of policy [{}] as the target index [{}] already exists",
                getKey().name(),
                indexMetadata.getIndex().getName(),
                indexMetadata.getLifecyclePolicyName(),
                targetIndexName
            );
            listener.onResponse(null);
            return;
        }

        Settings relevantTargetSettings = Settings.builder()
            .put(targetIndexSettingsSupplier.apply(indexMetadata))
            // We add the skip setting to prevent ILM from processing the shrunken index before the execution state has been copied - which
            // could happen if the shards of the shrunken index take a long time to allocate.
            .put(LifecycleSettings.LIFECYCLE_SKIP, true)
            .build();

        ResizeRequest resizeRequest = new ResizeRequest(targetIndexName, indexMetadata.getIndex().getName()).masterNodeTimeout(
            TimeValue.MAX_VALUE
        );
        resizeRequest.setResizeType(resizeType);
        resizeRequest.setMaxPrimaryShardSize(maxPrimaryShardSize);
        resizeRequest.getTargetIndexRequest().settings(relevantTargetSettings);

        getClient(currentState.projectId()).admin()
            .indices()
            .resizeIndex(resizeRequest, listener.delegateFailureAndWrap((l, response) -> l.onResponse(null)));

    }

    public ResizeType getResizeType() {
        return resizeType;
    }

    public BiFunction<String, LifecycleExecutionState, String> getTargetIndexNameSupplier() {
        return targetIndexNameSupplier;
    }

    public Function<IndexMetadata, Settings> getTargetIndexSettingsSupplier() {
        return targetIndexSettingsSupplier;
    }

    public ByteSizeValue getMaxPrimaryShardSize() {
        return maxPrimaryShardSize;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), resizeType, maxPrimaryShardSize);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        ResizeIndexStep other = (ResizeIndexStep) obj;
        return super.equals(obj)
            && Objects.equals(resizeType, other.resizeType)
            && Objects.equals(maxPrimaryShardSize, other.maxPrimaryShardSize);
    }

}
