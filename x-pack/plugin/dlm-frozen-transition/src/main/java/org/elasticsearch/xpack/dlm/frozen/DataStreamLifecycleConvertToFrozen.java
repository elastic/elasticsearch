/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.dlm.frozen;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.forcemerge.ForceMergeRequest;
import org.elasticsearch.action.admin.indices.readonly.AddIndexBlockRequest;
import org.elasticsearch.action.admin.indices.readonly.AddIndexBlockResponse;
import org.elasticsearch.action.admin.indices.readonly.TransportAddIndexBlockAction;
import org.elasticsearch.action.admin.indices.segments.IndexSegments;
import org.elasticsearch.action.admin.indices.segments.IndexShardSegments;
import org.elasticsearch.action.admin.indices.segments.IndicesSegmentResponse;
import org.elasticsearch.action.admin.indices.segments.IndicesSegmentsRequest;
import org.elasticsearch.action.admin.indices.segments.ShardSegments;
import org.elasticsearch.action.admin.indices.shrink.ResizeRequest;
import org.elasticsearch.action.admin.indices.shrink.ResizeType;
import org.elasticsearch.action.admin.indices.shrink.TransportResizeAction;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.DefaultShardOperationFailedException;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.broadcast.BroadcastResponse;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ProjectState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.metadata.RepositoriesMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.datastreams.DataStreamsPlugin;
import org.elasticsearch.datastreams.lifecycle.DataStreamLifecycleService;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.license.LicenseUtils;
import org.elasticsearch.license.XPackLicenseState;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.elasticsearch.action.support.master.MasterNodeRequest.INFINITE_MASTER_NODE_TIMEOUT;
import static org.elasticsearch.cluster.metadata.IndexMetadata.APIBlock.WRITE;
import static org.elasticsearch.xpack.core.searchablesnapshots.SearchableSnapshotsConstants.SEARCHABLE_SNAPSHOT_FEATURE;

/**
 * This class encapsulates the steps necessary to convert a data stream backing index to frozen.
 */
public class DataStreamLifecycleConvertToFrozen implements DlmFrozenTransitionRunnable {

    private static final Logger logger = LogManager.getLogger(DataStreamLifecycleConvertToFrozen.class);
    public static final String CLONE_INDEX_PREFIX = "dlm-clone-";
    private static final IndicesOptions IGNORE_MISSING_OPTIONS = IndicesOptions.fromOptions(true, true, false, false);

    private final String indexName;
    private final ProjectId projectId;
    private final Client client;
    private final ClusterService clusterService;
    private final XPackLicenseState licenseState;

    public DataStreamLifecycleConvertToFrozen(
        String indexName,
        ProjectId projectId,
        Client client,
        ClusterService clusterService,
        XPackLicenseState licenseState
    ) {
        this.indexName = indexName;
        this.projectId = projectId;
        this.client = client;
        this.clusterService = clusterService;
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
        // Todo: WIP - steps will be implemented in follow-up PRs
        maybeMarkIndexReadOnly();
        String indexForForceMerge = maybeCloneIndex();
        maybeForceMergeIndex(indexForForceMerge);
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
            if (e instanceof InterruptedException || ExceptionsHelper.unwrapCause(e) instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new ElasticsearchException("DLM unable to mark index [{}] with read only block", e, indexName);
        }
    }

    /**
     * Clones the index if the original does not have 0 replicas and a clone does not already exist.
     * Returns the name of the index to be used for force merge in the next step, which will be either the existing clone,
     * the original index (if it has 0 replicas), or a newly created clone.
     */
    String maybeCloneIndex() {
        if (isCloneNeeded() == false) {
            return getIndexForForceMerge();
        }

        String cloneIndexName = getDLMCloneIndexName();

        ResizeRequest resizeReq = getCloneRequest();
        logger.trace("DLM issuing request to clone index [{}] to index [{}]", indexName, cloneIndexName);
        try {
            CreateIndexResponse resp = client.projectClient(projectId).execute(TransportResizeAction.TYPE, resizeReq).get();
            if (resp.isAcknowledged() == false) {
                throw new ElasticsearchException(
                    Strings.format("DLM failed to acknowledge clone of index [%s] to index [%s]", indexName, cloneIndexName)
                );
            }
            logger.info("DLM successfully cloned index [{}] to index [{}]", indexName, cloneIndexName);
            return cloneIndexName;
        } catch (Exception e) {
            if (e instanceof InterruptedException || ExceptionsHelper.unwrapCause(e) instanceof IndexNotFoundException) {
                Thread.currentThread().interrupt();
            }
            try {
                deleteIndex(cloneIndexName);
            } catch (Exception deleteException) {
                e.addSuppressed(deleteException);
            }
            throw e instanceof ElasticsearchException
                ? (ElasticsearchException) e
                : new ElasticsearchException(
                    "DLM failed to clone index [{}] to index [{}]. " + "[{}] has been cleaned up by DLM.",
                    e,
                    indexName,
                    cloneIndexName,
                    cloneIndexName
                );
        }
    }

    public void maybeForceMergeIndex(String indexForForceMerge) {
        boolean indexMissing = Optional.ofNullable(getProjectState())
            .map(ProjectState::metadata)
            .map(metadata -> metadata.index(indexForForceMerge))
            .isEmpty();
        if (indexMissing) {
            logger.warn("Index [{}] not found in project metadata, skipping force merge step", indexForForceMerge);
            return;
        }
        if (isForceMergeComplete()) {
            logger.debug("Index [{}] has already been force merged by DLM, skipping force merge step", indexForForceMerge);
            return;
        }

        ForceMergeRequest req = new ForceMergeRequest(indexForForceMerge);
        req.maxNumSegments(1);
        req.timeout(TimeValue.MAX_VALUE);
        logger.info("DLM is issuing a request to force merge index [{}] to a single segment", indexForForceMerge);
        try {
            BroadcastResponse forceMergeResponse = client.projectClient(projectId).admin().indices().forceMerge(req).get();
            if (forceMergeResponse.getFailedShards() > 0) {
                DefaultShardOperationFailedException[] failures = forceMergeResponse.getShardFailures();
                String message = Strings.format(
                    "DLM failed to force merge %d shards for index [%s] due to failures [%s]",
                    forceMergeResponse.getFailedShards(),
                    indexForForceMerge,
                    failures == null
                        ? "unknown"
                        : Arrays.stream(failures).map(DefaultShardOperationFailedException::toString).collect(Collectors.joining(","))
                );
                throw new ElasticsearchException(message);
            } else if (forceMergeResponse.getUnavailableShards() > 0) {
                String message = Strings.format(
                    "DLM could not complete force merge for index [%s] because [%d] shards were unavailable."
                        + " This will be retried in the next cycle.",
                    indexForForceMerge,
                    forceMergeResponse.getUnavailableShards()
                );
                throw new ElasticsearchException(message);
            } else {
                logger.info("DLM successfully force merged index [{}]", indexForForceMerge);
            }
        } catch (Exception e) {
            if (e instanceof InterruptedException || ExceptionsHelper.unwrapCause(e) instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw e instanceof ElasticsearchException
                ? (ElasticsearchException) e
                : new ElasticsearchException("DLM failed to force merge index [{}]", e, indexForForceMerge);
        }
    }

    public void maybeTakeSnapshot() {

    }

    public void maybeMountSearchableSnapshot() {

    }

    private boolean isIndexReadOnly() {
        return getProjectState().blocks().hasIndexBlock(projectId, indexName, WRITE.getBlock());
    }

    /**
     * Determines if force merge is complete based on if the index has been successfully
     * force merged down to a single segment.
     */
    private boolean isForceMergeComplete() {
        try {
            IndicesSegmentResponse response = client.projectClient(projectId)
                .admin()
                .indices()
                .segments(new IndicesSegmentsRequest(indexName))
                .get();
            IndexSegments indexSegments = response.getIndices().get(indexName);
            if (indexSegments == null || indexSegments.getShards().isEmpty()) {
                logger.debug("No segment information found for index [{}], DLM force merge is not complete", indexName);
                return false;
            }
            for (IndexShardSegments indexShardSegments : indexSegments) {
                for (ShardSegments shardSegments : indexShardSegments) {
                    if (shardSegments.getShardRouting().primary() && shardSegments.getSegments().size() > 1) {
                        logger.debug(
                            "Shard [{}] of index [{}] has [{}] segments, DLM force merge is not complete",
                            shardSegments.getShardRouting().shardId(),
                            indexName,
                            shardSegments.getSegments().size()
                        );
                        return false;
                    }
                }
            }
            logger.debug("All primary shards of index [{}] have been force merged to a single segment", indexName);
            return true;
        } catch (Exception e) {
            if (e instanceof InterruptedException || ExceptionsHelper.unwrapCause(e) instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new ElasticsearchException("DLM unable to check segment count for index [{}]", e, indexName);
        }
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
        ProjectMetadata projectMetadata = getProjectState().metadata();
        if (projectMetadata.indices().containsKey(indexName) == false) {
            logger.debug("Index [{}] no longer exists in project [{}], skipping convert-to-frozen steps", indexName, projectId);
            return false;
        }

        final String repositoryName = getRepositoryForFrozen(projectMetadata, indexName);
        if (Strings.hasText(repositoryName) == false) {
            logger.debug("Default repository not configured, skipping convert-to-frozen steps for index [{}]", indexName);
            throw new ElasticsearchException(
                "Default repository is required for convert-to-frozen steps but is not configured for index " + indexName
            );
        }
        boolean repoIsRegistered = RepositoriesMetadata.get(getProjectState().metadata())
            .repositories()
            .stream()
            .anyMatch(repositoryMetadata -> repositoryMetadata.name().equals(repositoryName));
        if (repoIsRegistered == false) {
            logger.debug(
                "Repository [{}] required for convert-to-frozen steps is not registered in project [{}], skipping convert-to-frozen steps",
                repositoryName,
                projectId
            );
            return false;
        }

        if (SEARCHABLE_SNAPSHOT_FEATURE.checkWithoutTracking(licenseState) == false) {
            throw LicenseUtils.newComplianceException("searchable-snapshots");
        }

        return true;
    }

    /**
     * Return the repository name to use for converting this index to a searchable snapshot, or else null if it is not set.
     */
    @Nullable
    private static String getRepositoryForFrozen(ProjectMetadata projectMetadata, String indexName) {
        return Optional.ofNullable(projectMetadata.index(indexName))
            .map(im -> im.getCustomData(DataStreamsPlugin.LIFECYCLE_CUSTOM_INDEX_METADATA_KEY))
            .map(custom -> custom.get(DataStreamLifecycleService.FROZEN_CANDIDATE_REPOSITORY_METADATA_KEY))
            .orElse(null);
    }

    private ResizeRequest getCloneRequest() {
        String cloneIndexName = getDLMCloneIndexName();
        CreateIndexRequest createReq = new CreateIndexRequest(cloneIndexName);
        createReq.waitForActiveShards(ActiveShardCount.ALL);
        ResizeRequest resizeReq = new ResizeRequest(
            MasterNodeRequest.INFINITE_MASTER_NODE_TIMEOUT,
            AcknowledgedRequest.DEFAULT_ACK_TIMEOUT,
            ResizeType.CLONE,
            indexName,
            cloneIndexName
        );
        resizeReq.setTargetIndex(createReq);
        resizeReq.setTargetIndexSettings(
            Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0).putNull(IndexMetadata.SETTING_AUTO_EXPAND_REPLICAS)
        );
        return resizeReq;
    }

    /**
     * Checks whether a clone of the index is needed for the force merge step.
     * A clone is needed if the original index has more than 0 replicas and
     * a clone does not already exist.
     */
    boolean isCloneNeeded() {
        ProjectMetadata projectMetadata = getProjectState().metadata();
        IndexMetadata indexMetadata = projectMetadata.index(indexName);
        String cloneIndexName = getDLMCloneIndexName();
        boolean cloneExists = projectMetadata.indices().containsKey(cloneIndexName);
        if (cloneExists) {
            return false;
        }
        return indexMetadata.getNumberOfReplicas() != 0;
    }

    /**
     * Determines the appropriate index to use for the force merge step. If a clone index already exists and
     * is fully active, it will be returned. If no clone index exists but the original index has 0 replicas,
     * returns the original index. Otherwise, returns the clone index name.
     */
    String getIndexForForceMerge() {
        ProjectMetadata projectMetadata = getProjectState().metadata();
        String cloneIndexName = getDLMCloneIndexName();
        if (isCloneNeeded()) {
            return cloneIndexName;
        }

        boolean cloneExists = projectMetadata.indices().containsKey(cloneIndexName);
        if (cloneExists) {
            logger.debug("DLM has already cloned index [{}] in index [{}]", indexName, cloneIndexName);
            boolean cloneIsActive = Optional.ofNullable(getProjectState().routingTable())
                .map(routingTable -> routingTable.index(cloneIndexName).allPrimaryShardsActive())
                .orElse(false);
            if (cloneIsActive == false) {
                waitForCloneToBeActive();
            }
            return cloneIndexName;
        }
        // if we reach here, then it means the original index has 0 replicas, and we can skip the clone step and proceed
        // with the original index for force merge
        logger.debug(
            "Skipping DLM clone step for index [{}] as it already has 0 replicas and can be used for force merge directly",
            indexName
        );
        return indexName;
    }

    /**
     * Waits for the clone index to be fully active by issuing a cluster health request that waits for green status
     * on the clone index.
     */
    void waitForCloneToBeActive() {
        String cloneIndex = getDLMCloneIndexName();
        logger.debug(
            "DLM clone index [{}] already exists but is not fully active yet, waiting until it is active before proceeding",
            cloneIndex
        );
        ClusterHealthRequest healthRequest = new ClusterHealthRequest(INFINITE_MASTER_NODE_TIMEOUT, cloneIndex).waitForGreenStatus()
            .timeout(TimeValue.timeValueHours(12));
        try {
            ClusterHealthResponse response = client.projectClient(projectId).admin().cluster().health(healthRequest).get();
            if (response.isTimedOut()) {
                throw new ElasticsearchException("DLM timed out waiting for clone index [{}] to become active", cloneIndex);
            }
            logger.debug("DLM clone index [{}] is now fully active", cloneIndex);
        } catch (IndexNotFoundException e) {
            throw new ElasticsearchException("DLM failed waiting for clone index [{}] to become active", e, cloneIndex);
        } catch (Exception e) {
            try {
                deleteIndex(cloneIndex);
            } catch (Exception deleteException) {
                e.addSuppressed(deleteException);
            }
            if (e instanceof InterruptedException || ExceptionsHelper.unwrapCause(e) instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw e instanceof ElasticsearchException
                ? (ElasticsearchException) e
                : new ElasticsearchException("DLM failed waiting for clone index [{}] to become active", e, cloneIndex);
        }
    }

    /**
     * Gets a prefixed name for the clone index based on the original index name
     *
     * @return a prefixed clone index name
     */
    String getDLMCloneIndexName() {
        return CLONE_INDEX_PREFIX + indexName;
    }

    /**
     * Deletes the index if it exists.
     */
    void deleteIndex(String indexToDelete) {
        DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest(indexToDelete).indicesOptions(IGNORE_MISSING_OPTIONS)
            .masterNodeTimeout(TimeValue.MAX_VALUE);
        logger.debug("DLM issuing request to delete index [{}]", indexToDelete);
        try {
            AcknowledgedResponse resp = client.projectClient(projectId).admin().indices().delete(deleteIndexRequest).get();
            if (resp.isAcknowledged()) {
                logger.debug("DLM successfully deleted index [{}]", indexToDelete);
            } else {
                logger.warn("DLM failed to acknowledge deletion of index [{}]", indexToDelete);
                throw new ElasticsearchException(Strings.format("Failed to acknowledge delete of index [%s]", indexToDelete));
            }
        } catch (IndexNotFoundException e) {
            logger.debug("Index [{}] was not found during DLM delete attempt, it may have already been deleted", indexToDelete);
        } catch (Exception e) {
            logger.warn(Strings.format("DLM failed to delete index [%s]", indexToDelete), e);
            if (e instanceof InterruptedException || ExceptionsHelper.unwrapCause(e) instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new ElasticsearchException("DLM unable to delete index [{}]", e, indexToDelete);
        }
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

    @Override
    public String getIndexName() {
        return indexName;
    }

    @Override
    public ProjectId getProjectId() {
        return projectId;
    }

    private ProjectState getProjectState() {
        return clusterService.state().projectState(projectId);
    }
}
