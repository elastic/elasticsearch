/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ilm;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.RepositoriesMetaData;
import org.elasticsearch.cluster.metadata.RepositoryMetaData;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.Index;

import java.io.IOException;
import java.util.Locale;
import java.util.Objects;

/**
 * This step waits for a snapshot repository generation to change from the value stored in the ILM {@link LifecycleExecutionState}. This
 * is useful as a generation value change means the state of a snapshot in the repository has changed, so while the generation is the
 * same we can avoid possibly costly snapshot status checks via the
 * {@link org.elasticsearch.action.admin.cluster.snapshots.status.SnapshotsStatusAction} API.
 * <p>
 * This step expects the {@link LifecycleExecutionState} to contain the repository name and it should usually be used at a point later than
 * the {@link StoreSnapshotRepositoryGenerationStep} within an action's ILM step flow.
 */
public class WaitForRepositoryGenerationChangeStep extends ClusterStateWaitStep {

    public static final String NAME = "wait-for-repository-generation-change";

    private static final Logger logger = LogManager.getLogger(WaitForRepositoryGenerationChangeStep.class);

    WaitForRepositoryGenerationChangeStep(StepKey key, StepKey nextStepKey) {
        super(key, nextStepKey);
    }

    @Override
    public boolean isRetryable() {
        return true;
    }

    @Override
    public Result isConditionMet(Index index, ClusterState clusterState) {
        IndexMetaData indexMetaData = clusterState.metaData().index(index);

        if (indexMetaData == null) {
            String errorMessage = String.format(Locale.ROOT, "[%s] lifecycle action for index [%s] executed but index no longer exists",
                getKey().getAction(), index.getName());
            // Index must have been since deleted
            logger.debug(errorMessage);
            return new Result(false, new Info(errorMessage));
        }

        LifecycleExecutionState lifecycleState = LifecycleExecutionState.fromIndexMetadata(indexMetaData);
        String snapshotRepository = lifecycleState.getSnapshotRepository();
        String policyName = indexMetaData.getSettings().get(LifecycleSettings.LIFECYCLE_NAME);
        if (Strings.hasText(snapshotRepository) == false) {
            String errorMessage = "snapshot repository is not present for policy [" + policyName + "] and index [" + index.getName() + "]";
            logger.debug(errorMessage);
            return new Result(false, new Info(errorMessage));
        }

        if (lifecycleState.getRepositoryGeneration() == null) {
            String errorMessage =
                "found null repository generation recorded for policy [" + policyName + "] and index [" + index.getName() + "]";
            logger.warn(errorMessage);
            return new Result(false, new Info(errorMessage));
        }

        RepositoryMetaData repositoryMetadata =
            clusterState.getMetaData().<RepositoriesMetaData>custom(RepositoriesMetaData.TYPE).repository(snapshotRepository);
        if (repositoryMetadata == null) {
            String errorMessage = "repository [" + snapshotRepository + "] is missing. [" + policyName +
                "] policy for index [" + indexMetaData.getIndex().getName() + "] cannot continue until the repository is created";
            logger.warn(errorMessage);
            throw new IllegalStateException(errorMessage);
        }

        if (repositoryMetadata.generation() != lifecycleState.getRepositoryGeneration()) {
            return new Result(true, null);
        } else {
            return new Result(false,
                new Info(String.format(Locale.ROOT,
                    "generation [%s] for repository [%s] has not changed compared to the value [%s] recorded in the ILM state for " +
                        "policy [%s] and index [%s]", repositoryMetadata.generation(), snapshotRepository,
                    lifecycleState.getRepositoryGeneration(), policyName, index.getName())));
        }
    }

    static final class Info implements ToXContentObject {

        private final String message;

        static final ParseField MESSAGE = new ParseField("message");

        Info(String message) {
            this.message = message;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(MESSAGE.getPreferredName(), message);
            builder.endObject();
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Info info = (Info) o;
            return Objects.equals(message, info.message);
        }

        @Override
        public int hashCode() {
            return Objects.hash(message);
        }
    }
}
