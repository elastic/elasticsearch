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
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.MetadataCreateIndexService;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.index.Index;
import org.elasticsearch.indices.InvalidIndexNameException;
import org.elasticsearch.xpack.core.ilm.LifecycleExecutionState.Builder;

import java.util.Locale;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.core.ilm.LifecycleExecutionState.ILM_CUSTOM_METADATA_KEY;
import static org.elasticsearch.xpack.core.ilm.LifecycleExecutionState.fromIndexMetadata;

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
    static final String ILLEGAL_INDEXNAME_CHARS_REGEX = "[/:\"*?<>|# ,\\\\]+";
    static final int MAX_GENERATED_UUID_LENGTH = 4;

    private final String prefix;
    private final BiFunction<String, Builder, Builder> lifecycleStateSetter;

    public GenerateUniqueIndexNameStep(StepKey key, StepKey nextStepKey, String prefix,
                                       BiFunction<String, Builder, Builder> lifecycleStateSetter) {
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
        IndexMetadata indexMetadata = clusterState.metadata().index(index);
        if (indexMetadata == null) {
            // Index must have been since deleted, ignore it
            logger.debug("[{}] lifecycle action for index [{}] executed but index no longer exists", getKey().getAction(), index.getName());
            return clusterState;
        }

        ClusterState.Builder newClusterStateBuilder = ClusterState.builder(clusterState);

        LifecycleExecutionState lifecycleState = fromIndexMetadata(indexMetadata);

        Builder newCustomData = LifecycleExecutionState.builder(lifecycleState);
        String policy = indexMetadata.getSettings().get(LifecycleSettings.LIFECYCLE_NAME);
        String generatedIndexName = generateValidIndexName(prefix, index.getName());
        ActionRequestValidationException validationException = validateGeneratedIndexName(generatedIndexName, clusterState);
        if (validationException != null) {
            logger.warn("unable to generate a valid index name as part of policy [{}] for index [{}] due to [{}]",
                policy, index.getName(), validationException.getMessage());
            throw validationException;
        }
        lifecycleStateSetter.apply(generatedIndexName, newCustomData);

        IndexMetadata.Builder indexMetadataBuilder = IndexMetadata.builder(indexMetadata);
        indexMetadataBuilder.putCustom(ILM_CUSTOM_METADATA_KEY, newCustomData.build().asMap());
        newClusterStateBuilder.metadata(Metadata.builder(clusterState.getMetadata()).put(indexMetadataBuilder));
        return newClusterStateBuilder.build();
    }

    @Nullable
    static ActionRequestValidationException validateGeneratedIndexName(String generatedIndexName, ClusterState state) {
        ActionRequestValidationException err = new ActionRequestValidationException();
        try {
            MetadataCreateIndexService.validateIndexOrAliasName(generatedIndexName, InvalidIndexNameException::new);
        } catch (InvalidIndexNameException e) {
            err.addValidationError(e.getMessage());
        }
        if (state.routingTable().hasIndex(generatedIndexName) || state.metadata().hasIndex(generatedIndexName)) {
            err.addValidationError("the index name we generated [" + generatedIndexName + "] already exists");
        }
        if (state.metadata().hasAlias(generatedIndexName)) {
            err.addValidationError("the index name we generated [" + generatedIndexName + "] already exists as alias");
        }

        if (err.validationErrors().size() > 0) {
            return err;
        } else {
            return null;
        }
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

    /**
     * This generates a valid unique index name by using the provided prefix, appended with a generated UUID, and the index name.
     */
    static String generateValidIndexName(String prefix, String indexName) {
        String randomUUID = generateValidIndexSuffix(UUIDs::randomBase64UUID);
        randomUUID = randomUUID.substring(0, Math.min(randomUUID.length(), MAX_GENERATED_UUID_LENGTH));
        return prefix + randomUUID + "-" + indexName;
    }

    static String generateValidIndexSuffix(Supplier<String> randomGenerator) {
        String randomSuffix = randomGenerator.get().toLowerCase(Locale.ROOT);
        randomSuffix = randomSuffix.replaceAll(ILLEGAL_INDEXNAME_CHARS_REGEX, "");
        if (randomSuffix.length() == 0) {
            throw new IllegalArgumentException("unable to generate random index name suffix");
        }

        return randomSuffix;
    }
}
