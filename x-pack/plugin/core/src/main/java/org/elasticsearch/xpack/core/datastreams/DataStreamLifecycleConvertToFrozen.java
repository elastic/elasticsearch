/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.datastreams;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.indices.readonly.AddIndexBlockRequest;
import org.elasticsearch.action.admin.indices.readonly.AddIndexBlockResponse;
import org.elasticsearch.action.admin.indices.readonly.TransportAddIndexBlockAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ProjectState;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.metadata.RepositoriesMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.license.LicenseUtils;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.repositories.RepositoriesService;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static org.elasticsearch.action.support.master.MasterNodeRequest.INFINITE_MASTER_NODE_TIMEOUT;
import static org.elasticsearch.cluster.metadata.IndexMetadata.APIBlock.WRITE;
import static org.elasticsearch.xpack.core.searchablesnapshots.SearchableSnapshotsConstants.SEARCHABLE_SNAPSHOT_FEATURE;

/**
 * This class encapsulates the steps necessary to convert a data stream backing index to frozen.
 */
public class DataStreamLifecycleConvertToFrozen implements Runnable {

    private static final Logger logger = LogManager.getLogger(DataStreamLifecycleConvertToFrozen.class);

    private final String indexName;
    private final Client client;
    private final ProjectState projectState;
    private final XPackLicenseState licenseState;

    public DataStreamLifecycleConvertToFrozen(String indexName, Client client, ProjectState projectState, XPackLicenseState licenseState) {
        this.indexName = indexName;
        this.client = client;
        this.projectState = projectState;
        this.licenseState = licenseState;
    }

    /**
     * Runs this operation.
     */
    @Override
    public void run() {
        if (isEligibleForConvertToFrozen() == false) {
            return;
        }
        // Todo: WIP - only the first step of marking the index read-only is implemented for now,
        // the rest of the steps will be implemented in follow-up PRs
        maybeMarkIndexReadOnly();
        maybeCloneIndex();
        maybeForceMergeIndex();
        maybeTakeSnapshot();
        maybeMountSearchableSnapshot();
    }

    /**
     * Marks the index as read-only by adding a WRITE block, if the block is not already present.
     * This ensures all in-flight writes are completed and flushed to segments before proceeding
     * with the subsequent convert-to-frozen steps. In the case that the index is already marked as
     * read-only, this method will simply return without performing any action.
     *
     * @throws ElasticsearchException if the attempt to add the read-only block fails due to an
     * exception or an unacknowledged response from the cluster.
     */
    public void maybeMarkIndexReadOnly() {
        if (isIndexReadOnly()) {
            logger.debug("Index [{}] is already marked as read-only, skipping to clone step", indexName);
            return;
        }
        ProjectId projectId = projectState.projectId();
        AddIndexBlockRequest addIndexBlockRequest = new AddIndexBlockRequest(WRITE, indexName).masterNodeTimeout(
            INFINITE_MASTER_NODE_TIMEOUT
        );
        // Force a flush while adding the read-only block to ensure all in-flight writes are completed and written to segments
        addIndexBlockRequest.markVerified(true);
        try {
            AddIndexBlockResponse resp = client.projectClient(projectId)
                .execute(TransportAddIndexBlockAction.TYPE, addIndexBlockRequest)
                .get();
            validateAddIndexBlockResponse(addIndexBlockRequest, resp);
            logger.debug("DLM successfully marked index [{}] as read-only", indexName);
        } catch (Exception e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new ElasticsearchException("DLM unable to mark index [{}] with read only block", e, indexName);
        }
    }

    public void maybeCloneIndex() {

    }

    public void maybeForceMergeIndex() {

    }

    public void maybeTakeSnapshot() {

    }

    public void maybeMountSearchableSnapshot() {

    }

    private boolean isIndexReadOnly() {
        return projectState.blocks().hasIndexBlock(projectState.projectId(), indexName, WRITE.getBlock());
    }

    /**
     * Public for testing only.
     * Checks whether the necessary conditions are met to proceed with the convert-to-frozen steps.
     * Throws an exception if the license does not allow searchable snapshots,
     * and returns false if the index or repository are no longer present in the project metadata.
     * @return true if the convert-to-frozen steps can proceed, false if they should be skipped due to missing index or repository
     * @throws org.elasticsearch.ElasticsearchSecurityException if the license does not allow searchable snapshots
     */
    boolean isEligibleForConvertToFrozen() {
        ProjectMetadata projectMetadata = projectState.metadata();
        if (projectMetadata.indices().containsKey(indexName) == false) {
            logger.debug(
                "Index [{}] no longer exists in project [{}], skipping convert-to-frozen steps",
                indexName,
                projectState.projectId()
            );
            return false;
        }

        String repositoryName = resolveRepositoryName(projectState);
        if (Strings.hasText(repositoryName) == false) {
            logger.debug("Default repository not configured, skipping convert-to-frozen steps for index [{}]", indexName);
            throw new ElasticsearchException(
                "Default repository is required for convert-to-frozen steps but is not configured for index " + indexName
            );
        }
        boolean repoIsRegistered = RepositoriesMetadata.get(projectState.metadata())
            .repositories()
            .stream()
            .anyMatch(repositoryMetadata -> repositoryMetadata.name().equals(repositoryName));
        if (repoIsRegistered == false) {
            logger.debug(
                "Repository [{}] required for convert-to-frozen steps is not registered in project [{}], skipping convert-to-frozen steps",
                repositoryName,
                projectState.projectId()
            );
            return false;
        }

        if (SEARCHABLE_SNAPSHOT_FEATURE.checkWithoutTracking(licenseState) == false) {
            throw LicenseUtils.newComplianceException("searchable-snapshots");
        }

        return true;
    }

    /**
     * Resolves the repository name to use for the snapshot and searchable snapshot steps.
     */
    private static String resolveRepositoryName(ProjectState projectState) {
        return RepositoriesService.DEFAULT_REPOSITORY_SETTING.get(projectState.cluster().metadata().settings());
    }

    /**
     * Validates the response from the add index block request. If the response indicates that the block was successfully added,
     * this method returns normally. If the response indicates a failure, this method throws an exception with details about the failure.
     */
    private void validateAddIndexBlockResponse(AddIndexBlockRequest addIndexBlockRequest, AddIndexBlockResponse addIndexBlockResponse) {
        String targetIndex = addIndexBlockRequest.indices()[0];
        if (addIndexBlockResponse.isAcknowledged() == false) {
            Optional<AddIndexBlockResponse.AddBlockResult> resultForTargetIndex = addIndexBlockResponse.getIndices()
                .stream()
                .filter(blockResult -> blockResult.getIndex().getName().equals(targetIndex))
                .findAny();
            if (resultForTargetIndex.isEmpty()) {
                // This really should not happen but, if it does, mark as a fail and retry next DLM run
                logger.trace(
                    "DLM received an unacknowledged response when attempting to add the "
                        + "read-only block to index [{}], but the response didn't contain an explicit result for the index.",
                    targetIndex
                );
                throw new ElasticsearchException("DLM request to mark index [" + targetIndex + "] as read-only was not acknowledged");
            } else if (resultForTargetIndex.get().hasFailures()) {
                AddIndexBlockResponse.AddBlockResult blockResult = resultForTargetIndex.get();
                if (blockResult.getException() != null) {
                    throw new ElasticsearchException(
                        "DLM received an exception when marking index [" + targetIndex + "] as read-only: " + blockResult.getException()
                    );
                } else {
                    List<AddIndexBlockResponse.AddBlockShardResult.Failure> shardFailures = new ArrayList<>(blockResult.getShards().length);
                    for (AddIndexBlockResponse.AddBlockShardResult shard : blockResult.getShards()) {
                        if (shard.hasFailures()) {
                            shardFailures.addAll(Arrays.asList(shard.getFailures()));
                        }
                    }
                    assert shardFailures.isEmpty() == false
                        : "DLM: The block response must have shard failures as the global "
                            + "exception is null. The block result is: "
                            + blockResult;
                    String errorMessage = org.elasticsearch.common.Strings.collectionToDelimitedString(
                        shardFailures.stream().map(org.elasticsearch.common.Strings::toString).toList(),
                        ","
                    );
                    throw new ElasticsearchException(errorMessage);
                }
            } else {
                throw new ElasticsearchException("DLM's request to mark index [" + targetIndex + "] as read-only was not acknowledged");
            }
        }
    }
}
