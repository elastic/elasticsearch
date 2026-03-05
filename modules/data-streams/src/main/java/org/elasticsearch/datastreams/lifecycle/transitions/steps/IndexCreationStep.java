/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.datastreams.lifecycle.transitions.steps;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ProjectState;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.allocation.decider.ShardsLimitAllocationDecider;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.datastreams.lifecycle.transitions.DlmStep;
import org.elasticsearch.datastreams.lifecycle.transitions.DlmStepContext;
import org.elasticsearch.index.Index;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.core.searchablesnapshots.MountSearchableSnapshotAction;
import org.elasticsearch.xpack.core.searchablesnapshots.MountSearchableSnapshotRequest;

import java.util.List;
import java.util.Optional;

public class IndexCreationStep implements DlmStep {

    private static final Logger logger = LogManager.getLogger(IndexCreationStep.class);
    public static final String DLM_FROZEN_INDEX_PREFIX = "dlm-frozen-";

    /**
     * Determines if the step has been completed for the given index and project state by checking whether the searchable snapshot
     * has been mounted.
     *
     * @param index        The index to check.
     * @param projectState The current project state.
     * @return true if the step is completed, false otherwise.
     */
    @Override
    public boolean stepCompleted(Index index, ProjectState projectState) {
        ProjectMetadata projectMetadata = projectState.metadata();
        return Optional.of(getMountedIndexName(index.getName()))
            .filter(projectMetadata.indices()::containsKey)
            .map(idx -> projectState.routingTable().index(idx))
            .map(IndexRoutingTable::allPrimaryShardsActive)
            .orElse(false);
    }

    /**
     * This method determines how to execute the step and performs the necessary operations to update the index
     * so that {@link #stepCompleted(Index, ProjectState)} will return true after successful execution.
     *
     * @param dlmStepContext The context and resources for executing the step.
     */
    @Override
    public void execute(DlmStepContext dlmStepContext) {
        maybeMountSnapshot(dlmStepContext);
    }

    /**
     * A human-readable name for the step.
     *
     * @return The step name.
     */
    @Override
    public String stepName() {
        return "Index Creation";
    }

    @Override
    public List<String> possibleOutputIndexNamePatterns(String indexName) {
        return List.of(getMountedIndexName(indexName));
    }

    void maybeMountSnapshot(DlmStepContext stepContext) {
        MountSearchableSnapshotRequest mountRequest = new MountSearchableSnapshotRequest(
            TimeValue.MAX_VALUE,
            getMountedIndexName(stepContext.indexName()),
            resolveRepositoryName(stepContext),
            snapshotName(stepContext.indexName()),
            stepContext.indexName(),
            Settings.EMPTY,
            ignoredIndexSettings(),
            true,
            MountSearchableSnapshotRequest.Storage.SHARED_CACHE
        );
        stepContext.executeDeduplicatedRequest(
            MountSearchableSnapshotAction.NAME,
            mountRequest,
            "DLM service encountered an error trying to mount the frozen index for original index [" + stepContext.indexName() + "]",
            (req, reqListener) -> mountSnapshot((MountSearchableSnapshotRequest) req.v2(), reqListener, stepContext)
        );
    }

    private void mountSnapshot(MountSearchableSnapshotRequest mountRequest, ActionListener<Void> listener, DlmStepContext stepContext) {
        logger.debug(
            "DLM attempting to mount frozen index [{}] for original index [{}]",
            getMountedIndexName(stepContext.indexName()),
            stepContext.indexName()
        );
        stepContext.client()
            .projectClient(stepContext.projectId())
            .execute(MountSearchableSnapshotAction.INSTANCE, mountRequest, listener.delegateFailureAndWrap((l, response) -> {
                if (response.status() != RestStatus.OK && response.status() != RestStatus.ACCEPTED) {
                    String errorMessage = Strings.format(
                        "DLM failed to mount frozen index [%s] for original index [%s], got response [%s]",
                        getMountedIndexName(stepContext.indexName()),
                        stepContext.indexName(),
                        response.status()
                    );
                    ElasticsearchException e = new ElasticsearchException(errorMessage);
                    l.onFailure(e);
                    return;
                }
                logger.info(
                    "DLM successfully mounted frozen index [{}] for original index [{}]",
                    getMountedIndexName(stepContext.indexName()),
                    stepContext.indexName()
                );
                l.onResponse(null);
            }));
    }

    private String getMountedIndexName(String indexName) {
        return DLM_FROZEN_INDEX_PREFIX + indexName;
    }

    /**
     * This method returns the settings that need to be ignored when we mount the searchable snapshot. Currently, it returns:
     * - index.routing.allocation.total_shards_per_node: It is likely that frozen tier has fewer nodes than the hot tier. If this setting
     * is not specifically set in the frozen tier, keeping this setting runs the risk that we will not have enough nodes to
     * allocate all the shards in the frozen tier and the user does not have any way of fixing this. For this reason, we ignore this
     * setting when moving to frozen.
     */
    String[] ignoredIndexSettings() {
        return new String[] { ShardsLimitAllocationDecider.INDEX_TOTAL_SHARDS_PER_NODE_SETTING.getKey() };
    }

    // todo: remove everything below this line when #143490 merges
    private static String resolveRepositoryName(DlmStepContext stepContext) {
        return RepositoriesService.DEFAULT_REPOSITORY_SETTING.get(stepContext.projectState().cluster().metadata().settings());
    }

    static final String SNAPSHOT_NAME_PREFIX = "dlm-frozen-";

    static String snapshotName(String indexName) {
        return SNAPSHOT_NAME_PREFIX + indexName;
    }
}
