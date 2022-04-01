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
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.LifecycleExecutionState;
import org.elasticsearch.cluster.metadata.RepositoriesMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.Index;

import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Objects;

/**
 * Generates a snapshot name for the given index and records it in the index metadata along with the provided snapshot repository.
 * <p>
 * The generated snapshot name will be in the format {day-indexName-policyName-randomUUID}
 * eg.: 2020.03.30-myindex-mypolicy-cmuce-qfvmn_dstqw-ivmjc1etsa
 */
public class GenerateSnapshotNameStep extends ClusterStateActionStep {

    public static final String NAME = "generate-snapshot-name";

    private static final Logger logger = LogManager.getLogger(GenerateSnapshotNameStep.class);

    private final String snapshotRepository;

    public GenerateSnapshotNameStep(StepKey key, StepKey nextStepKey, String snapshotRepository) {
        super(key, nextStepKey);
        this.snapshotRepository = snapshotRepository;
    }

    public String getSnapshotRepository() {
        return snapshotRepository;
    }

    @Override
    public ClusterState performAction(Index index, ClusterState clusterState) {
        IndexMetadata indexMetadata = clusterState.metadata().index(index);
        if (indexMetadata == null) {
            // Index must have been since deleted, ignore it
            logger.debug("[{}] lifecycle action for index [{}] executed but index no longer exists", getKey().getAction(), index.getName());
            return clusterState;
        }

        String policyName = indexMetadata.getLifecyclePolicyName();
        LifecycleExecutionState lifecycleState = indexMetadata.getLifecycleExecutionState();

        // validate that the snapshot repository exists -- because policies are refreshed on later retries, and because
        // this fails prior to the snapshot repository being recorded in the ilm metadata, the policy can just be corrected
        // and everything will pass on the subsequent retry
        if (clusterState.metadata().custom(RepositoriesMetadata.TYPE, RepositoriesMetadata.EMPTY).repository(snapshotRepository) == null) {
            throw new IllegalStateException(
                "repository ["
                    + snapshotRepository
                    + "] is missing. ["
                    + policyName
                    + "] policy for "
                    + "index ["
                    + index.getName()
                    + "] cannot continue until the repository is created or the policy is changed"
            );
        }

        LifecycleExecutionState.Builder newLifecycleState = LifecycleExecutionState.builder(lifecycleState);
        newLifecycleState.setSnapshotIndexName(index.getName());
        newLifecycleState.setSnapshotRepository(snapshotRepository);
        if (lifecycleState.snapshotName() == null) {
            // generate and validate the snapshotName
            String snapshotNamePrefix = ("<{now/d}-" + index.getName() + "-" + policyName + ">").toLowerCase(Locale.ROOT);
            String snapshotName = generateSnapshotName(snapshotNamePrefix);
            ActionRequestValidationException validationException = validateGeneratedSnapshotName(snapshotNamePrefix, snapshotName);
            if (validationException != null) {
                logger.warn(
                    "unable to generate a snapshot name as part of policy [{}] for index [{}] due to [{}]",
                    policyName,
                    index.getName(),
                    validationException.getMessage()
                );
                throw validationException;
            }

            newLifecycleState.setSnapshotName(snapshotName);
        }

        return LifecycleExecutionStateUtils.newClusterStateWithLifecycleState(
            clusterState,
            indexMetadata.getIndex(),
            newLifecycleState.build()
        );
    }

    @Override
    public boolean isRetryable() {
        return true;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), snapshotRepository);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        GenerateSnapshotNameStep other = (GenerateSnapshotNameStep) obj;
        return super.equals(obj) && Objects.equals(snapshotRepository, other.snapshotRepository);
    }

    /**
     * Since snapshots need to be uniquely named, this method will resolve any date math used in
     * the provided name, as well as appending a unique identifier so expressions that may overlap
     * still result in unique snapshot names.
     */
    public static String generateSnapshotName(String name) {
        return generateSnapshotName(name, new IndexNameExpressionResolver.ResolverContext());
    }

    public static String generateSnapshotName(String name, IndexNameExpressionResolver.Context context) {
        List<String> candidates = IndexNameExpressionResolver.DateMathExpressionResolver.resolve(context, Collections.singletonList(name));
        if (candidates.size() != 1) {
            throw new IllegalStateException("resolving snapshot name " + name + " generated more than one candidate: " + candidates);
        }
        // TODO: we are breaking the rules of UUIDs by lowercasing this here, find an alternative (snapshot names must be lowercase)
        return candidates.get(0) + "-" + UUIDs.randomBase64UUID().toLowerCase(Locale.ROOT);
    }

    @Nullable
    public static ActionRequestValidationException validateGeneratedSnapshotName(String snapshotPrefix, String snapshotName) {
        ActionRequestValidationException err = new ActionRequestValidationException();
        if (Strings.hasText(snapshotPrefix) == false) {
            err.addValidationError("invalid snapshot name [" + snapshotPrefix + "]: cannot be empty");
        }
        if (snapshotName.contains("#")) {
            err.addValidationError("invalid snapshot name [" + snapshotPrefix + "]: must not contain '#'");
        }
        if (snapshotName.charAt(0) == '_') {
            err.addValidationError("invalid snapshot name [" + snapshotPrefix + "]: must not start with '_'");
        }
        if (snapshotName.toLowerCase(Locale.ROOT).equals(snapshotName) == false) {
            err.addValidationError("invalid snapshot name [" + snapshotPrefix + "]: must be lowercase");
        }
        if (Strings.validFileName(snapshotName) == false) {
            err.addValidationError(
                "invalid snapshot name ["
                    + snapshotPrefix
                    + "]: must not contain contain the following characters "
                    + Strings.INVALID_FILENAME_CHARS
            );
        }

        if (err.validationErrors().size() > 0) {
            return err;
        } else {
            return null;
        }
    }

}
