/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ilm;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.ProjectState;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.LifecycleExecutionState;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.index.Index;

import java.util.Objects;
import java.util.function.BiFunction;

/**
 * This step replaces a data stream backing index with the target index, as part of the data stream's backing indices.
 * Eg. if data stream `foo-stream` is backed by indices [`foo-stream-000001`, `foo-stream-000002`] and we'd like to replace the first
 * generation index, `foo-stream-000001`, with `shrink-foo-stream-000001`, after this step the `foo-stream` data stream will contain
 * the following indices
 * <p>
 * [`shrink-foo-stream-000001`, `foo-stream-000002`]
 * <p>
 * The `foo-stream-000001` index will continue to exist but will not be part of the data stream anymore.
 * <p>
 * As the last generation is the write index of the data stream, replacing the last generation index is not allowed.
 * <p>
 * This is useful in scenarios following a restore from snapshot operation where the restored index will take the place of the source
 * index in the ILM lifecycle or in the case where we shrink an index and the shrunk index will take the place of the original index.
 */
public class ReplaceDataStreamBackingIndexStep extends ClusterStateActionStep {
    public static final String NAME = "replace-datastream-backing-index";
    private static final Logger logger = LogManager.getLogger(ReplaceDataStreamBackingIndexStep.class);

    private final BiFunction<String, LifecycleExecutionState, String> targetIndexNameSupplier;

    public ReplaceDataStreamBackingIndexStep(
        StepKey key,
        StepKey nextStepKey,
        BiFunction<String, LifecycleExecutionState, String> targetIndexNameSupplier
    ) {
        super(key, nextStepKey);
        this.targetIndexNameSupplier = targetIndexNameSupplier;
    }

    @Override
    public boolean isRetryable() {
        return true;
    }

    public BiFunction<String, LifecycleExecutionState, String> getTargetIndexNameSupplier() {
        return targetIndexNameSupplier;
    }

    @Override
    public ProjectState performAction(Index index, ProjectState projectState) {
        IndexMetadata originalIndexMetadata = projectState.metadata().index(index);
        if (originalIndexMetadata == null) {
            // Index must have been since deleted, skip the shrink action
            logger.debug("[{}] lifecycle action for index [{}] executed but index no longer exists", NAME, index.getName());
            return projectState;
        }

        String originalIndex = index.getName();
        String targetIndexName = targetIndexNameSupplier.apply(originalIndex, originalIndexMetadata.getLifecycleExecutionState());
        String policyName = originalIndexMetadata.getLifecyclePolicyName();
        IndexAbstraction indexAbstraction = projectState.metadata().getIndicesLookup().get(index.getName());
        assert indexAbstraction != null : "invalid cluster metadata. index [" + index.getName() + "] was not found";
        DataStream dataStream = indexAbstraction.getParentDataStream();
        if (dataStream == null) {
            String errorMessage = Strings.format(
                "index [%s] is not part of a data stream. stopping execution of lifecycle "
                    + "[%s] until the index is added to a data stream",
                originalIndex,
                policyName
            );
            logger.debug(errorMessage);
            throw new IllegalStateException(errorMessage);
        }

        boolean isFailureStoreWriteIndex = index.equals(dataStream.getWriteFailureIndex());
        if (isFailureStoreWriteIndex || dataStream.getWriteIndex().equals(index)) {
            String errorMessage = Strings.format(
                "index [%s] is the%s write index for data stream [%s], pausing "
                    + "ILM execution of lifecycle [%s] until this index is no longer the write index for the data stream via manual or "
                    + "automated rollover",
                originalIndex,
                isFailureStoreWriteIndex ? " failure store" : "",
                dataStream.getName(),
                policyName
            );
            logger.debug(errorMessage);
            throw new IllegalStateException(errorMessage);
        }

        IndexMetadata targetIndexMetadata = projectState.metadata().index(targetIndexName);
        if (targetIndexMetadata == null) {
            String errorMessage = Strings.format(
                "target index [%s] doesn't exist. stopping execution of lifecycle [%s] for index [%s]",
                targetIndexName,
                policyName,
                originalIndex
            );
            logger.debug(errorMessage);
            throw new IllegalStateException(errorMessage);
        }

        DataStream updatedDataStream = dataStream.isFailureStoreIndex(originalIndex)
            ? dataStream.replaceFailureStoreIndex(index, targetIndexMetadata.getIndex())
            : dataStream.replaceBackingIndex(index, targetIndexMetadata.getIndex());
        return projectState.withProject(ProjectMetadata.builder(projectState.metadata()).put(updatedDataStream).build());
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), targetIndexNameSupplier);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        ReplaceDataStreamBackingIndexStep other = (ReplaceDataStreamBackingIndexStep) obj;
        return super.equals(obj) && Objects.equals(targetIndexNameSupplier, other.targetIndexNameSupplier);
    }
}
