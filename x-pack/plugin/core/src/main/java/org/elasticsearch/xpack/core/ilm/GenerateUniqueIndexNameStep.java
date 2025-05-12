/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ilm;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.LifecycleExecutionState;
import org.elasticsearch.cluster.metadata.LifecycleExecutionState.Builder;
import org.elasticsearch.common.IndexNameGenerator;
import org.elasticsearch.index.Index;

import java.util.Objects;
import java.util.function.BiFunction;

import static org.elasticsearch.common.IndexNameGenerator.validateGeneratedIndexName;

/**
 * Generates a unique index name prefixing the original index name with the configured
 * prefix, concatenated with a random UUID. The generated index name will be stored in the lifecycle
 * execution state in the field designated by the configured setter method {@link #lifecycleStateSetter}
 * <p>
 * The generated name will be in the format {prefix-randomUUID-indexName}
 */
public class GenerateUniqueIndexNameStep extends ClusterStateActionStep {
    private static final Logger logger = LogManager.getLogger(GenerateUniqueIndexNameStep.class);

    public static final String NAME = "generate-index-name";

    private final String prefix;
    private final BiFunction<String, Builder, Builder> lifecycleStateSetter;

    public GenerateUniqueIndexNameStep(
        StepKey key,
        StepKey nextStepKey,
        String prefix,
        BiFunction<String, Builder, Builder> lifecycleStateSetter
    ) {
        super(key, nextStepKey);
        this.prefix = prefix;
        this.lifecycleStateSetter = lifecycleStateSetter;
    }

    @Override
    public boolean isRetryable() {
        return true;
    }

    String prefix() {
        return prefix;
    }

    BiFunction<String, Builder, Builder> lifecycleStateSetter() {
        return lifecycleStateSetter;
    }

    @Override
    public ClusterState performAction(Index index, ClusterState clusterState) {
        IndexMetadata indexMetadata = clusterState.metadata().getProject().index(index);
        if (indexMetadata == null) {
            // Index must have been since deleted, ignore it
            logger.debug("[{}] lifecycle action for index [{}] executed but index no longer exists", getKey().action(), index.getName());
            return clusterState;
        }

        LifecycleExecutionState lifecycleState = indexMetadata.getLifecycleExecutionState();

        Builder newLifecycleState = LifecycleExecutionState.builder(lifecycleState);
        String policyName = indexMetadata.getLifecyclePolicyName();
        String generatedIndexName = generateIndexName(prefix, index.getName());
        ActionRequestValidationException validationException = validateGeneratedIndexName(generatedIndexName, clusterState.projectState());
        if (validationException != null) {
            logger.warn(
                "unable to generate a valid index name as part of policy [{}] for index [{}] due to [{}]",
                policyName,
                index.getName(),
                validationException.getMessage()
            );
            throw validationException;
        }
        lifecycleStateSetter.apply(generatedIndexName, newLifecycleState);

        return LifecycleExecutionStateUtils.newClusterStateWithLifecycleState(
            clusterState,
            indexMetadata.getIndex(),
            newLifecycleState.build()
        );
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        GenerateUniqueIndexNameStep that = (GenerateUniqueIndexNameStep) o;
        return super.equals(o) && Objects.equals(prefix, that.prefix);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), prefix);
    }

    public String generateIndexName(final String prefix, final String indexName) {
        return IndexNameGenerator.generateValidIndexName(prefix, indexName);
    }
}
