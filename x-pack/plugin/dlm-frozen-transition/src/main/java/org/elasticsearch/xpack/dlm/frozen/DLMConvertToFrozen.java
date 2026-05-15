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
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotRequest;
import org.elasticsearch.action.admin.cluster.snapshots.delete.DeleteSnapshotRequest;
import org.elasticsearch.action.admin.cluster.snapshots.delete.TransportDeleteSnapshotAction;
import org.elasticsearch.action.admin.cluster.snapshots.get.GetSnapshotsRequest;
import org.elasticsearch.action.admin.cluster.snapshots.get.GetSnapshotsResponse;
import org.elasticsearch.action.admin.cluster.snapshots.get.TransportGetSnapshotsAction;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
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
import org.elasticsearch.action.datastreams.ModifyDataStreamsAction;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.DefaultShardOperationFailedException;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.broadcast.BroadcastResponse;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.ProjectState;
import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.DataStreamAction;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.metadata.RepositoriesMetadata;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.cluster.routing.allocation.DataTier;
import org.elasticsearch.cluster.routing.allocation.decider.ShardsLimitAllocationDecider;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.datastreams.DataStreamsPlugin;
import org.elasticsearch.datastreams.lifecycle.DataStreamLifecycleService;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.license.LicenseUtils;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.snapshots.RestoreInfo;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.elasticsearch.snapshots.SnapshotMissingException;
import org.elasticsearch.snapshots.SnapshotState;
import org.elasticsearch.xpack.core.ilm.LifecycleSettings;
import org.elasticsearch.xpack.core.searchablesnapshots.MountSearchableSnapshotAction;
import org.elasticsearch.xpack.core.searchablesnapshots.MountSearchableSnapshotRequest;

import java.time.Clock;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.elasticsearch.action.support.master.MasterNodeRequest.INFINITE_MASTER_NODE_TIMEOUT;
import static org.elasticsearch.cluster.metadata.IndexMetadata.APIBlock.WRITE;
import static org.elasticsearch.xpack.core.searchablesnapshots.SearchableSnapshotsConstants.SEARCHABLE_SNAPSHOT_FEATURE;

/**
 * This class encapsulates the steps necessary to convert a data stream backing index to frozen.
 */
public class DLMConvertToFrozen implements DLMFrozenTransitionRunnable {

    public static final String DLM_CREATED_SETTING_KEY = IndexMetadata.INDEX_SETTING_PREFIX + "dlm.frozen.created";
    public static final Setting<Boolean> DLM_CREATED_SETTING = Setting.boolSetting(
        DLM_CREATED_SETTING_KEY,
        false,
        Setting.Property.IndexScope,
        Setting.Property.InternalIndex
    );

    public static final String CLONE_INDEX_PREFIX = "dlm-clone-";
    static final String SNAPSHOT_NAME_PREFIX = "dlm-frozen-";
    static final IndicesOptions IGNORE_MISSING_OPTIONS = IndicesOptions.fromOptions(true, true, false, false);
    static final String DLM_CREATED_METADATA_KEY = "dlm-created";
    private static final Logger logger = LogManager.getLogger(DLMConvertToFrozen.class);
    private static final TimeValue SNAPSHOT_TIMEOUT = TimeValue.timeValueHours(12);

    private final String indexName;
    private final ProjectId projectId;
    private final Client client;
    private final ClusterService clusterService;
    private final Supplier<XPackLicenseState> licenseStateSupplier;
    private final Clock clock;

    public DLMConvertToFrozen(
        String indexName,
        ProjectId projectId,
        Client client,
        ClusterService clusterService,
        Supplier<XPackLicenseState> licenseStateSupplier,
        Clock clock
    ) {
        this.indexName = indexName;
        this.projectId = projectId;
        this.client = client;
        this.clusterService = clusterService;
        this.licenseStateSupplier = licenseStateSupplier;
        this.clock = clock;
    }

    /**
     * Runs this operation.
     */
    @Override
    public void run() {
        try {
            maybeMarkIndexReadOnly();
            String forceMergeIndex = maybeCloneIndex();
            maybeForceMergeIndex(forceMergeIndex);
            maybeTakeSnapshot(forceMergeIndex);
            maybeMountSearchableSnapshot(forceMergeIndex);
            maybeCleanup(forceMergeIndex);
        } catch (IndexNotFoundException e) {
            if (e.getIndex().getName().equals(indexName)) {
                // if the original index was not found, then we can assume
                // it was deleted after the eligibility check, and we should
                // skip the remaining steps
                logger.warn("Index [{}] was not found during DLM convert-to-frozen operation, skipping this index", indexName);
            } else {
                throw e;
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
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

    /**
     * Checks if the current thread has been interrupted and, if so, throws an {@link InterruptedException}.
     * This allows long-running multi-step operations to detect interrupts quickly at the beginning
     * of each step rather than waiting for a blocking call to fail.
     */
    private static void checkIfThreadInterrupted() throws InterruptedException {
        if (Thread.currentThread().isInterrupted()) {
            throw new InterruptedException("DLM frozen conversion was interrupted");
        }
    }

    /**
     * Public for testing only.
     * Checks whether the necessary conditions are met to proceed with the convert-to-frozen steps.
     * @throws IndexNotFoundException if the index to be converted to frozen no longer exists in the project metadata
     * @throws DLMUnrecoverableException if the snapshot repository is not configured or no longer registered
     * @throws org.elasticsearch.ElasticsearchSecurityException if the license does not allow searchable snapshots
     */
    void checkIfEligibleForConvertToFrozen() {
        ProjectMetadata projectMetadata = getProjectState().metadata();
        if (projectMetadata.indices().containsKey(indexName) == false) {
            throw new IndexNotFoundException(indexName);
        }

        final String repositoryName = getRepositoryForFrozen(projectMetadata, indexName);
        if (Strings.hasText(repositoryName) == false) {
            throw new DLMUnrecoverableException(
                indexName,
                "Default repository is required for convert-to-frozen steps but was not configured for index [{}]",
                indexName
            );
        }
        RepositoryMetadata registeredRepo = RepositoriesMetadata.get(getProjectState().metadata())
            .repositories()
            .stream()
            .filter(repositoryMetadata -> repositoryMetadata.name().equals(repositoryName))
            .findFirst()
            .orElse(null);
        if (registeredRepo == null) {
            throw new DLMUnrecoverableException(
                indexName,
                "Repository [{}] required for convert-to-frozen steps is no longer registered in project [{}]",
                repositoryName,
                projectId
            );
        }
        if (RepositoriesService.isReadOnly(registeredRepo.settings())) {
            throw new DLMUnrecoverableException(
                "Repository [{}] required for convert-to-frozen steps is configured as read-only in project [{}]",
                repositoryName,
                projectId
            );
        }

        if (SEARCHABLE_SNAPSHOT_FEATURE.checkWithoutTracking(licenseStateSupplier.get()) == false) {
            throw LicenseUtils.newComplianceException("searchable-snapshots");
        }
    }

    /**
     * Checks whether the index exists in the project metadata. Throws IndexNotFoundException if not, or
     * ElasticsearchException if the project state or metadata cannot be retrieved for some reason.
     */
    private void checkIndexExists(String index) {
        ProjectState projectState = getProjectState();
        if (projectState == null || projectState.metadata() == null) {
            throw new ElasticsearchException("Project state not found for project [{}] during DLM run", projectId);
        }
        if (projectState.metadata().index(index) == null) {
            throw new IndexNotFoundException(index);
        }
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
    public void maybeMarkIndexReadOnly() throws InterruptedException {
        checkIfThreadInterrupted();
        checkIfEligibleForConvertToFrozen();

        if (isIndexReadOnly()) {
            logger.debug("Index [{}] is already marked as read-only, skipping to clone step", indexName);
            return;
        }

        waitForIndexYellowStatus(indexName);

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
    String maybeCloneIndex() throws InterruptedException {
        checkIfThreadInterrupted();
        checkIfEligibleForConvertToFrozen();

        if (isCloneNeeded() == false) {
            return getIndexForForceMerge();
        }

        waitForIndexYellowStatus(indexName);

        String cloneIndexName = getDLMCloneIndexName();

        ResizeRequest resizeReq = getCloneRequest();
        logger.trace("DLM issuing request to clone index [{}] to index [{}]", indexName, cloneIndexName);
        try {
            CreateIndexResponse resp = client.projectClient(projectId).execute(TransportResizeAction.TYPE, resizeReq).get();
            if (resp.isAcknowledged() == false) {
                throw new ElasticsearchException("DLM failed to acknowledge clone of index [{}] to index [{}]", indexName, cloneIndexName);
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

    public void maybeForceMergeIndex(String forceMergeIndex) throws InterruptedException {
        checkIfThreadInterrupted();
        checkIfEligibleForConvertToFrozen();
        checkIndexExists(forceMergeIndex);

        if (isForceMergeComplete()) {
            logger.debug("Index [{}] has already been force merged by DLM, skipping force merge step", forceMergeIndex);
            return;
        }

        waitForIndexYellowStatus(forceMergeIndex);

        ForceMergeRequest req = new ForceMergeRequest(forceMergeIndex);
        req.maxNumSegments(1);
        req.timeout(TimeValue.MAX_VALUE);
        logger.info("DLM is issuing a request to force merge index [{}] to a single segment", forceMergeIndex);
        try {
            BroadcastResponse forceMergeResponse = client.projectClient(projectId).admin().indices().forceMerge(req).get();
            if (forceMergeResponse.getFailedShards() > 0) {
                DefaultShardOperationFailedException[] failures = forceMergeResponse.getShardFailures();
                String message = Strings.format(
                    "DLM failed to force merge %d shards for index [%s] due to failures [%s]",
                    forceMergeResponse.getFailedShards(),
                    forceMergeIndex,
                    failures == null
                        ? "unknown"
                        : Arrays.stream(failures).map(DefaultShardOperationFailedException::toString).collect(Collectors.joining(","))
                );
                throw new ElasticsearchException(message);
            } else if (forceMergeResponse.getUnavailableShards() > 0) {
                String message = Strings.format(
                    "DLM could not complete force merge for index [%s] because [%d] shards were unavailable."
                        + " This will be retried in the next cycle.",
                    forceMergeIndex,
                    forceMergeResponse.getUnavailableShards()
                );
                throw new ElasticsearchException(message);
            } else {
                logger.info("DLM successfully force merged index [{}]", forceMergeIndex);
            }
        } catch (Exception e) {
            if (e instanceof InterruptedException || ExceptionsHelper.unwrapCause(e) instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw e instanceof ElasticsearchException
                ? (ElasticsearchException) e
                : new ElasticsearchException("DLM failed to force merge index [{}]", e, forceMergeIndex);
        }
    }

    /**
     * Takes a snapshot of the index and waits for completion. If a snapshot with the expected name is already
     * in progress, checks how long it has been running. If it has been running for longer than the configured
     * timeout, cancels the snapshot and starts a new one; otherwise leaves it alone to complete. If no snapshot
     * is currently in progress, checks whether a completed snapshot with the expected name already exists in
     * the repository. If a valid completed snapshot exists, skips re-taking the snapshot. If an invalid completed snapshot
     * exists (e.g. failed or partial), deletes it and starts a new one. If no completed snapshot exists, starts a new one.
     */
    void maybeTakeSnapshot(String forceMergeIndex) throws InterruptedException {
        checkIfThreadInterrupted();
        checkIfEligibleForConvertToFrozen();
        checkIndexExists(forceMergeIndex);

        ProjectState projectState = getProjectState();
        ProjectMetadata projectMetadata = projectState.metadata();
        // Use the original index name for repository lookup since that's where the repository is configured,
        // even if we are force merging a clone index.
        final String repositoryName = getRepositoryForFrozen(projectMetadata, indexName);
        String snapshotName = snapshotName(forceMergeIndex);

        SnapshotsInProgress snapshotsInProgress = SnapshotsInProgress.get(projectState.cluster());
        OptionalLong snapshotStartTime = findSnapshotStartTime(snapshotsInProgress, projectId, repositoryName, snapshotName);

        if (snapshotStartTime.isPresent()) {
            handleInProgressSnapshot(forceMergeIndex, repositoryName, snapshotName, snapshotStartTime.getAsLong());
        } else {
            checkForOrphanedSnapshotAndStart(forceMergeIndex, repositoryName, snapshotName);
        }
    }

    public void maybeMountSearchableSnapshot(String forceMergeIndex) throws InterruptedException {
        checkIfThreadInterrupted();
        checkIfEligibleForConvertToFrozen();

        if (isSnapshotMounted()) {
            logger.debug("Snapshot [{}] is already mounted, skipping DLM mount searchable snapshot step", snapshotName(forceMergeIndex));
            return;
        }

        ProjectState projectState = getProjectState();
        ProjectMetadata projectMetadata = projectState.metadata();
        String snapshotName = snapshotName(forceMergeIndex);
        String mountedIndexName = snapshotName(indexName);

        // We ignore these settings when mounting the snapshot to frozen:
        // - ShardsLimitAllocationDecider.INDEX_TOTAL_SHARDS_PER_NODE_SETTING:
        // It is likely that frozen tier has fewer nodes than the hot tier. If this setting
        // is not specifically set in the frozen tier, keeping this setting runs the risk that we will not have enough nodes to
        // allocate all the shards in the frozen tier and the user does not have any way of
        // fixing this. For this reason, we ignore this setting when moving to frozen.
        // - LifecycleSettings.LIFECYCLE_NAME:
        // Avoids potential conflicts with ILM.
        // - DataTier.TIER_PREFERENCE:
        // Since we are moving to frozen, we want to ensure that any existing tier preferences
        // do not interfere with the allocation of the mounted index to the frozen tier.
        String[] ignoredIndexSettings = new String[] {
            ShardsLimitAllocationDecider.INDEX_TOTAL_SHARDS_PER_NODE_SETTING.getKey(),
            LifecycleSettings.LIFECYCLE_NAME,
            DataTier.TIER_PREFERENCE };

        MountSearchableSnapshotRequest mountRequest = new MountSearchableSnapshotRequest(
            TimeValue.MAX_VALUE,
            mountedIndexName,
            getRepositoryForFrozen(projectMetadata, indexName),
            snapshotName,
            forceMergeIndex,
            Settings.builder().put(DLM_CREATED_SETTING_KEY, true).build(),
            ignoredIndexSettings,
            true,
            MountSearchableSnapshotRequest.Storage.SHARED_CACHE
        );

        logger.debug("DLM attempting to mount frozen index [{}]", snapshotName);
        try {
            RestoreSnapshotResponse resp = client.projectClient(projectId)
                .execute(MountSearchableSnapshotAction.INSTANCE, mountRequest)
                .get();
            RestoreInfo restoreInfo = resp.getRestoreInfo();
            if (restoreInfo == null) {
                throw new ElasticsearchException("DLM failed to mount snapshot [{}] because the restore info was missing", snapshotName);
            }
            if (restoreInfo.failedShards() > 0 || restoreInfo.successfulShards() == 0) {
                throw new ElasticsearchException(
                    "DLM failed to mount snapshot [{}] because there were failed shards or no successful shards. Restore info: [{}]",
                    snapshotName,
                    restoreInfo
                );
            }
            logger.info("DLM successfully mounted snapshot [{}]", snapshotName);
        } catch (Exception e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw ExceptionsHelper.convertToElastic(e, "DLM failed while mounting snapshot [{}]", snapshotName);
        }

    }

    void maybeCleanup(String forceMergeIndex) throws InterruptedException {
        if (isCleanUpComplete(forceMergeIndex)) {
            logger.debug("DLM cleanup is already complete for index [{}], skipping cleanup step.", indexName);
            return;
        }

        checkIfThreadInterrupted();
        checkIfEligibleForConvertToFrozen();

        ProjectMetadata projectMetadata = getProjectState().metadata();
        // Check if the old index is still part of a datastream, swap if so
        String dataStreamName = resolveDataStreamName(indexName, projectMetadata);
        if (dataStreamName != null) {
            swapIndicesInDataStream(dataStreamName);
        }
        // Delete the force merge index if it is different from the original.
        if (indexName.equals(forceMergeIndex) == false) {
            deleteIndex(forceMergeIndex);
        }
        deleteIndex(indexName);
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
     * The cleanup is complete when neither the old index nor its clone exist and the frozen index is part of a data stream.
     */
    private boolean isCleanUpComplete(String forceMergeIndex) {
        // return false if original or clone indices still exist
        ProjectMetadata projectMetadata = getProjectState().metadata();
        if (projectMetadata.indices().containsKey(indexName)) {
            return false;
        }
        if (projectMetadata.indices().containsKey(forceMergeIndex)) {
            return false;
        }
        // return false if frozen index is not in a datastream (swap hasn't occurred yet)
        return resolveDataStreamName(snapshotName(indexName), projectMetadata) != null;
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
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .putNull(IndexMetadata.SETTING_AUTO_EXPAND_REPLICAS)
                .put(DLM_CREATED_SETTING_KEY, true)
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
     * Waits up to 1 minute for the given index to reach yellow status (all primary shards allocated).
     * Throws an {@link ElasticsearchException} if the timeout is breached.
     */
    private void waitForIndexYellowStatus(String index) {
        TimeValue timeout = TimeValue.timeValueMinutes(1);
        ClusterHealthRequest healthRequest = new ClusterHealthRequest(INFINITE_MASTER_NODE_TIMEOUT, index).waitForYellowStatus()
            .timeout(timeout);
        ClusterHealthResponse response;
        try {
            response = client.projectClient(projectId).admin().cluster().health(healthRequest).get();
        } catch (Exception e) {
            if (e instanceof InterruptedException || ExceptionsHelper.unwrapCause(e) instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new ElasticsearchException("DLM failed while waiting for index [{}] shards to be allocated", e, index);
        }
        if (response.isTimedOut()) {
            throw new ElasticsearchException(
                "DLM timed out after [{}]m waiting for index [{}] shards to be allocated",
                timeout.getMinutes(),
                index
            );
        }
        logger.debug("DLM index [{}] has reached yellow status, proceeding", index);
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
                throw new ElasticsearchException("Failed to acknowledge delete of index [{}]", indexToDelete);
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

    /**
     * A snapshot for this index is currently running in the cluster. If it has been running longer
     * than {@link #SNAPSHOT_TIMEOUT}, delete it and start again; otherwise wait for it to complete.
     */
    void handleInProgressSnapshot(String indexName, String repositoryName, String snapshotName, long snapshotStartTime) {
        if ((clock.millis() - snapshotStartTime) > SNAPSHOT_TIMEOUT.millis()) {
            logger.warn(
                "DLM snapshot [{}] for index [{}] has been running for over [{}], cancelling and restarting",
                snapshotName,
                indexName,
                SNAPSHOT_TIMEOUT
            );
            deleteSnapshotIfExists(repositoryName, snapshotName, indexName);
            createSnapshot(indexName, repositoryName, snapshotName);
        } else {
            logger.info(
                "DLM snapshot [{}] for index [{}] is currently in progress and has been running for [{}], waiting for completion",
                snapshotName,
                indexName,
                TimeValue.timeValueMillis(clock.millis() - snapshotStartTime)
            );
            waitForSnapshotCompletion(indexName, repositoryName, snapshotName, snapshotStartTime);
        }
    }

    /**
     * Waits for the snapshot to complete by observing cluster state changes via {@link ClusterStateObserver}.
     * The observer watches for the snapshot to be removed from {@link SnapshotsInProgress}, which indicates
     * that it has completed (successfully or otherwise). If the observer times out (i.e. the remaining time until {@link #SNAPSHOT_TIMEOUT}
     * elapses) or the cluster service closes, an exception is thrown.
     */
    void waitForSnapshotCompletion(String indexName, String repositoryName, String snapshotName, long snapshotStartTime) {
        TimeValue timeout = TimeValue.timeValueMillis(SNAPSHOT_TIMEOUT.millis() - (clock.millis() - snapshotStartTime));

        // Use a latch so that the observer listener (invoked on the ClusterApplierService thread)
        // does no heavy/blocking work
        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicReference<Exception> observerError = new AtomicReference<>();

        ClusterStateObserver.waitForState(
            clusterService,
            clusterService.threadPool().getThreadContext(),
            new ClusterStateObserver.Listener() {
                @Override
                public void onNewClusterState(ClusterState state) {
                    // Snapshot is no longer in progress – signal the waiting thread.
                    latch.countDown();
                }

                @Override
                public void onClusterServiceClose() {
                    observerError.set(
                        new ElasticsearchException(
                            "Cluster service closed while waiting for DLM snapshot [{}] for index [{}]",
                            snapshotName,
                            indexName
                        )
                    );
                    latch.countDown();
                }

                @Override
                public void onTimeout(TimeValue timeout) {
                    observerError.set(
                        new ElasticsearchException(
                            "DLM snapshot [{}] for index [{}] has exceeded timeout of [{}]",
                            snapshotName,
                            indexName,
                            SNAPSHOT_TIMEOUT
                        )
                    );
                    latch.countDown();
                }
            },
            state -> isSnapshotNoLongerInProgress(state, repositoryName, snapshotName),
            timeout,
            logger
        );

        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ElasticsearchException("Interrupted while waiting for DLM snapshot [{}] for index [{}]", e, snapshotName, indexName);
        }

        Exception error = observerError.get();
        if (error != null) {
            throw ExceptionsHelper.convertToElastic(error);
        }

        SnapshotInfo snapshot = getSnapshot(repositoryName, snapshotName, indexName);
        if (snapshot == null) {
            throw new ElasticsearchException(
                "DLM snapshot [{}] for index [{}] disappeared while waiting for completion",
                snapshotName,
                indexName
            );
        }
        checkSnapshotInfoSuccess(indexName, snapshotName, snapshot);
    }

    /**
     * Returns {@code true} if the snapshot with the given name is either no longer listed in
     * {@link SnapshotsInProgress} for the specified repository, or is still listed but has
     * reached a completed (non-running) state such as {@code SUCCESS} or {@code FAILED}.
     */
    private boolean isSnapshotNoLongerInProgress(ClusterState state, String repositoryName, String snapshotName) {
        SnapshotsInProgress snapshotsInProgress = SnapshotsInProgress.get(state);
        return snapshotsInProgress.forRepo(projectId, repositoryName)
            .stream()
            .noneMatch(entry -> entry.snapshot().getSnapshotId().getName().equals(snapshotName) && entry.state().completed() == false);
    }

    /**
     * No snapshot is currently running for this index. Check whether a completed snapshot already
     * exists in the repository. If a valid successful snapshot exists, returns.
     * If the snapshot exists but is invalid (e.g. partial or failed), delete and recreate it.
     * Otherwise, start a fresh snapshot.
     */
    void checkForOrphanedSnapshotAndStart(String indexName, String repositoryName, String snapshotName) {
        SnapshotInfo existingSnapshot = getSnapshot(repositoryName, snapshotName, indexName);
        if (existingSnapshot == null) {
            createSnapshot(indexName, repositoryName, snapshotName);
            return;
        }

        if (existingSnapshot.state() == SnapshotState.SUCCESS && existingSnapshot.failedShards() == 0) {
            logger.info("DLM found valid snapshot [{}] for index [{}]", snapshotName, indexName);
        } else {
            logger.info(
                "DLM found invalid orphaned snapshot [{}] for index [{}] (state [{}], failed shards [{}]), deleting and recreating",
                snapshotName,
                indexName,
                existingSnapshot.state(),
                existingSnapshot.failedShards()
            );
            deleteSnapshotIfExists(repositoryName, snapshotName, indexName);
            createSnapshot(indexName, repositoryName, snapshotName);
        }
    }

    /**
     * Attempts to delete a snapshot. If the snapshot is already missing, logs and returns.
     * Throws on any other failure.
     */
    void deleteSnapshotIfExists(String repositoryName, String snapshotName, String indexName) {
        DeleteSnapshotRequest deleteRequest = new DeleteSnapshotRequest(INFINITE_MASTER_NODE_TIMEOUT, repositoryName, snapshotName);
        try {
            AcknowledgedResponse resp = client.projectClient(projectId).execute(TransportDeleteSnapshotAction.TYPE, deleteRequest).get();
            if (resp.isAcknowledged() == false) {
                throw new ElasticsearchException("Failed to acknowledge delete of snapshot [{}] for index [{}]", snapshotName, indexName);
            }
            logger.info("DLM successfully deleted stale snapshot [{}] for index [{}]", snapshotName, indexName);
        } catch (Exception e) {
            Exception cause = unwrapExecutionException(e);
            if (cause instanceof SnapshotMissingException) {
                logger.debug("DLM snapshot [{}] for index [{}] already missing, proceeding to create", snapshotName, indexName);
            } else {
                if (e instanceof InterruptedException) {
                    Thread.currentThread().interrupt();
                }
                throw ExceptionsHelper.convertToElastic(
                    cause,
                    "DLM failed to delete stale snapshot [{}] for index [{}]",
                    snapshotName,
                    indexName
                );
            }
        }
    }

    /**
     * Fetches snapshot info for a single snapshot from the repository. Returns {@code null} if the snapshot does not exist.
     */
    @Nullable
    SnapshotInfo getSnapshot(String repositoryName, String snapshotName, String indexName) {
        GetSnapshotsRequest getRequest = new GetSnapshotsRequest(INFINITE_MASTER_NODE_TIMEOUT, repositoryName);
        getRequest.snapshots(new String[] { snapshotName });
        getRequest.ignoreUnavailable(true);
        try {
            GetSnapshotsResponse response = client.projectClient(projectId).execute(TransportGetSnapshotsAction.TYPE, getRequest).get();
            List<SnapshotInfo> snapshots = response.getSnapshots();
            return snapshots.isEmpty() ? null : snapshots.getFirst();
        } catch (Exception e) {
            Exception cause = unwrapExecutionException(e);
            if (cause instanceof SnapshotMissingException) {
                return null;
            }
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw ExceptionsHelper.convertToElastic(cause, "DLM failed while checking snapshots for index [{}]", indexName);
        }
    }

    /**
     * Unwraps an {@link ExecutionException} to its cause, since {@code Future.get()} wraps
     * failures. Returns the original exception if it is not an {@code ExecutionException}.
     */
    private static Exception unwrapExecutionException(Exception e) {
        return e instanceof ExecutionException && e.getCause() != null ? (Exception) ExceptionsHelper.unwrapCause(e.getCause()) : e;
    }

    /**
     * Creates a snapshot for the given index and waits for completion.
     * Throws an exception if the snapshot fails to complete successfully for any reason.
     */
    void createSnapshot(String indexName, String repositoryName, String snapshotName) {
        waitForIndexYellowStatus(indexName);
        CreateSnapshotRequest createRequest = buildCreateSnapshotRequest(repositoryName, indexName, snapshotName);
        try {
            var response = client.projectClient(projectId)
                .admin()
                .cluster()
                .createSnapshot(createRequest)
                .get(SNAPSHOT_TIMEOUT.millis(), TimeUnit.MILLISECONDS);
            checkSnapshotInfoSuccess(indexName, snapshotName, response.getSnapshotInfo());
        } catch (TimeoutException e) {
            throw new ElasticsearchException(
                "DLM timed out after [{}] waiting for snapshot [{}] for index [{}]",
                e,
                SNAPSHOT_TIMEOUT,
                snapshotName,
                indexName
            );
        } catch (Exception e) {
            final Exception unwrapped = unwrapExecutionException(e);
            if (unwrapped instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw ExceptionsHelper.convertToElastic(unwrapped, "DLM failed to start snapshot [{}] for index [{}]", snapshotName, indexName);
        }
    }

    /**
     * Checks the snapshot info to determine whether the snapshot completed successfully. If so, logs and returns.
     * If not, throws an exception with details about the failure.
     */
    static void checkSnapshotInfoSuccess(String indexName, String snapshotName, SnapshotInfo snapshotInfo) {
        if (snapshotInfo == null) {
            throw new ElasticsearchException("DLM snapshot [{}] for index [{}] did not return snapshot info", snapshotName, indexName);
        }

        if (snapshotInfo.state() == SnapshotState.SUCCESS && snapshotInfo.failedShards() == 0) {
            logger.info("DLM successfully created snapshot [{}] for index [{}]", snapshotName, indexName);
            return;
        }

        int failedShards = snapshotInfo.failedShards();
        String state = snapshotInfo.state() == null ? "unknown" : snapshotInfo.state().name();
        String reason = snapshotInfo.reason();
        if (Strings.hasText(reason)) {
            throw new ElasticsearchException(
                "DLM snapshot [{}] for index [{}] finished with [{}] failed shards, state [{}], reason [{}]",
                snapshotName,
                indexName,
                failedShards,
                state,
                reason
            );
        } else {
            throw new ElasticsearchException(
                "DLM snapshot [{}] for index [{}] finished with [{}] failed shards, state [{}]",
                snapshotName,
                indexName,
                failedShards,
                state
            );
        }
    }

    /**
     * Finds the start time of a running snapshot with the given name. Returns empty if not found.
     */
    private static OptionalLong findSnapshotStartTime(
        SnapshotsInProgress snapshotsInProgress,
        ProjectId projectId,
        String repositoryName,
        String snapshotName
    ) {
        return snapshotsInProgress.forRepo(projectId, repositoryName)
            .stream()
            .filter(entry -> entry.snapshot().getSnapshotId().getName().equals(snapshotName))
            .mapToLong(SnapshotsInProgress.Entry::startTime)
            .findFirst();
    }

    private static CreateSnapshotRequest buildCreateSnapshotRequest(String repositoryName, String indexName, String snapshotName) {
        CreateSnapshotRequest request = new CreateSnapshotRequest(INFINITE_MASTER_NODE_TIMEOUT, repositoryName, snapshotName);
        request.indices(indexName);
        request.waitForCompletion(true);
        request.includeGlobalState(false);
        request.userMetadata(Map.of(DLM_CREATED_METADATA_KEY, true));
        return request;
    }

    static String snapshotName(String indexName) {
        return SNAPSHOT_NAME_PREFIX + indexName;
    }

    /**
     * Checks whether the snapshot for the index is already mounted by
     * looking for an index with the expected mounted name in the project metadata.
     */
    boolean isSnapshotMounted() {
        ProjectMetadata projectMetadata = getProjectState().metadata();
        return projectMetadata.indices().containsKey(snapshotName(indexName));
    }

    /**
     * Resolves the parent data stream name for the given index.
     *
     * @param indexName       the index name to look up
     * @param projectMetadata the project metadata containing the indices lookup
     * @return the data stream name, or {@code null} if the index is not part of a data stream
     */
    static String resolveDataStreamName(String indexName, ProjectMetadata projectMetadata) {
        IndexAbstraction indexAbstraction = projectMetadata.getIndicesLookup().get(indexName);
        if (indexAbstraction == null) {
            return null;
        }
        DataStream parentDataStream = indexAbstraction.getParentDataStream();
        return parentDataStream != null ? parentDataStream.getName() : null;
    }

    /**
     * Swaps a backing index in a data stream by issuing a {@link ModifyDataStreamsAction} request
     * with a remove action for the old index and an add action for the new frozen index.
     * @param dataStreamName the name of the data stream
     */
    void swapIndicesInDataStream(String dataStreamName) {
        ProjectState projectState = getProjectState();

        ModifyDataStreamsAction.Request request = new ModifyDataStreamsAction.Request(
            TimeValue.MAX_VALUE,
            TimeValue.MAX_VALUE,
            List.of(
                DataStreamAction.removeBackingIndex(dataStreamName, indexName),
                DataStreamAction.addBackingIndex(dataStreamName, snapshotName(indexName))
            )
        );

        AcknowledgedResponse resp;
        try {
            resp = client.projectClient(projectState.projectId()).execute(ModifyDataStreamsAction.INSTANCE, request).get();
        } catch (Exception e) {
            if (e instanceof InterruptedException || ExceptionsHelper.unwrapCause(e) instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw ExceptionsHelper.convertToElastic(e);
        }
        if (resp.isAcknowledged()) {
            logger.info(
                "DLM successfully swapped backing index [{}] with [{}] in data stream [{}]",
                indexName,
                snapshotName(indexName),
                dataStreamName
            );
        } else {
            throw new ElasticsearchException(
                "DLM failed to acknowledge swap of backing index [{}] with [{}] in data stream [{}]",
                indexName,
                snapshotName(indexName),
                dataStreamName
            );
        }
    }
}
