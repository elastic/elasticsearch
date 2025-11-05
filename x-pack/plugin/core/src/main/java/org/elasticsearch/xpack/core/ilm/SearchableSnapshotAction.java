/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ilm;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.admin.indices.shrink.ResizeType;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.LifecycleExecutionState;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.license.LicenseUtils;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ilm.Step.StepKey;
import org.elasticsearch.xpack.core.searchablesnapshots.MountSearchableSnapshotRequest;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.Function;

import static org.elasticsearch.snapshots.SearchableSnapshotsSettings.SEARCHABLE_SNAPSHOTS_REPOSITORY_NAME_SETTING_KEY;
import static org.elasticsearch.snapshots.SearchableSnapshotsSettings.SEARCHABLE_SNAPSHOTS_SNAPSHOT_NAME_SETTING_KEY;
import static org.elasticsearch.snapshots.SearchableSnapshotsSettings.SEARCHABLE_SNAPSHOT_PARTIAL_SETTING_KEY;
import static org.elasticsearch.xpack.core.searchablesnapshots.SearchableSnapshotsConstants.SEARCHABLE_SNAPSHOT_FEATURE;

/**
 * A {@link LifecycleAction} that will convert the index into a searchable snapshot, by taking a snapshot of the index, creating a
 * searchable snapshot and the corresponding "searchable snapshot index", deleting the original index and swapping its aliases to the
 * newly created searchable snapshot backed index.
 */
public class SearchableSnapshotAction implements LifecycleAction {
    private static final Logger logger = LogManager.getLogger(SearchableSnapshotAction.class);

    public static final String NAME = "searchable_snapshot";

    public static final ParseField SNAPSHOT_REPOSITORY = new ParseField("snapshot_repository");
    public static final ParseField FORCE_MERGE_INDEX = new ParseField("force_merge_index");
    public static final ParseField TOTAL_SHARDS_PER_NODE = new ParseField("total_shards_per_node");
    public static final ParseField REPLICATE_FOR = new ParseField("replicate_for");
    public static final ParseField FORCE_MERGE_ON_CLONE = new ParseField("force_merge_on_clone");

    private static final TransportVersion FORCE_MERGE_ON_CLONE_TRANSPORT_VERSION = TransportVersion.fromName(
        "ilm_searchable_snapshot_opt_out_clone"
    );

    public static final String CONDITIONAL_SKIP_ACTION_STEP = BranchingStep.NAME + "-check-prerequisites";
    public static final String CONDITIONAL_SKIP_GENERATE_AND_CLEAN = BranchingStep.NAME + "-check-existing-snapshot";
    public static final String CONDITIONAL_SKIP_CLONE_STEP = BranchingStep.NAME + "-skip-clone-check";
    public static final String WAIT_FOR_CLONED_INDEX_GREEN = WaitForIndexColorStep.NAME + "-cloned-index";
    public static final String CONDITIONAL_DATASTREAM_CHECK_KEY = BranchingStep.NAME + "-on-datastream-check";
    public static final String CONDITIONAL_DELETE_FORCE_MERGED_INDEX_KEY = BranchingStep.NAME + "-delete-force-merged-index";
    public static final String DELETE_FORCE_MERGED_INDEX_KEY = DeleteStep.NAME + "-force-merged-index";

    public static final String FULL_RESTORED_INDEX_PREFIX = "restored-";
    public static final String PARTIAL_RESTORED_INDEX_PREFIX = "partial-";
    public static final String FORCE_MERGE_CLONE_INDEX_PREFIX = "fm-clone-";
    /** An index name supplier that always returns the force merge index name (possibly null). */
    public static final BiFunction<String, LifecycleExecutionState, String> FORCE_MERGE_CLONE_INDEX_NAME_SUPPLIER = (
        indexName,
        state) -> state.forceMergeCloneIndexName();
    /** An index name supplier that returns the force merge index name if it exists, or the original index name if not. */
    public static final BiFunction<String, LifecycleExecutionState, String> FORCE_MERGE_CLONE_INDEX_NAME_FALLBACK_SUPPLIER = (
        indexName,
        state) -> state.forceMergeCloneIndexName() != null ? state.forceMergeCloneIndexName() : indexName;

    /** The cloned index should have 0 replicas, so we also need to remove the auto_expand_replicas setting if present. */
    private static final Settings CLONE_SETTINGS = Settings.builder()
        .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
        .put(IndexMetadata.SETTING_AUTO_EXPAND_REPLICAS, (String) null)
        .build();
    private static final Function<IndexMetadata, Settings> CLONE_SETTINGS_SUPPLIER = indexMetadata -> CLONE_SETTINGS;

    private static final ConstructingObjectParser<SearchableSnapshotAction, Void> PARSER = new ConstructingObjectParser<>(
        NAME,
        a -> new SearchableSnapshotAction((String) a[0], a[1] == null || (boolean) a[1], (Integer) a[2], (TimeValue) a[3], (Boolean) a[4])
    );

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), SNAPSHOT_REPOSITORY);
        PARSER.declareBoolean(ConstructingObjectParser.optionalConstructorArg(), FORCE_MERGE_INDEX);
        PARSER.declareInt(ConstructingObjectParser.optionalConstructorArg(), TOTAL_SHARDS_PER_NODE);
        PARSER.declareField(
            ConstructingObjectParser.optionalConstructorArg(),
            p -> TimeValue.parseTimeValue(p.textOrNull(), REPLICATE_FOR.getPreferredName()),
            REPLICATE_FOR,
            ObjectParser.ValueType.STRING
        );
        PARSER.declareBoolean(ConstructingObjectParser.optionalConstructorArg(), FORCE_MERGE_ON_CLONE);
    }

    public static SearchableSnapshotAction parse(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    private final String snapshotRepository;
    private final boolean forceMergeIndex;
    @Nullable
    private final Integer totalShardsPerNode;
    @Nullable
    private final TimeValue replicateFor;
    /** Opt-out field for forcing the force-merge step to run on the source index instead of a cloned version with 0 replicas. */
    @Nullable
    private final Boolean forceMergeOnClone;

    public SearchableSnapshotAction(
        String snapshotRepository,
        boolean forceMergeIndex,
        @Nullable Integer totalShardsPerNode,
        @Nullable TimeValue replicateFor,
        @Nullable Boolean forceMergeOnClone
    ) {
        if (Strings.hasText(snapshotRepository) == false) {
            throw new IllegalArgumentException("the snapshot repository must be specified");
        }
        this.snapshotRepository = snapshotRepository;
        this.forceMergeIndex = forceMergeIndex;

        if (totalShardsPerNode != null && totalShardsPerNode < 1) {
            throw new IllegalArgumentException("[" + TOTAL_SHARDS_PER_NODE.getPreferredName() + "] must be >= 1");
        }
        this.totalShardsPerNode = totalShardsPerNode;

        if (replicateFor != null && replicateFor.millis() <= 0) {
            throw new IllegalArgumentException(
                "[" + REPLICATE_FOR.getPreferredName() + "] must be positive [" + replicateFor.getStringRep() + "]"
            );
        }
        this.replicateFor = replicateFor;

        if (forceMergeIndex == false && forceMergeOnClone != null) {
            throw new IllegalArgumentException(
                Strings.format(
                    "[%s] is not allowed when [%s] is [false]",
                    FORCE_MERGE_ON_CLONE.getPreferredName(),
                    FORCE_MERGE_INDEX.getPreferredName()
                )
            );
        }
        this.forceMergeOnClone = forceMergeOnClone;
    }

    public SearchableSnapshotAction(String snapshotRepository, boolean forceMergeIndex) {
        this(snapshotRepository, forceMergeIndex, null, null, null);
    }

    public SearchableSnapshotAction(String snapshotRepository) {
        this(snapshotRepository, true, null, null, null);
    }

    public SearchableSnapshotAction(StreamInput in) throws IOException {
        this.snapshotRepository = in.readString();
        this.forceMergeIndex = in.readBoolean();
        this.totalShardsPerNode = in.getTransportVersion().onOrAfter(TransportVersions.V_8_16_0) ? in.readOptionalInt() : null;
        this.replicateFor = in.getTransportVersion().supports(TransportVersions.V_8_18_0) ? in.readOptionalTimeValue() : null;
        this.forceMergeOnClone = in.getTransportVersion().supports(FORCE_MERGE_ON_CLONE_TRANSPORT_VERSION)
            ? in.readOptionalBoolean()
            : null;
    }

    public boolean isForceMergeIndex() {
        return forceMergeIndex;
    }

    public String getSnapshotRepository() {
        return snapshotRepository;
    }

    @Nullable
    public Integer getTotalShardsPerNode() {
        return totalShardsPerNode;
    }

    @Nullable
    public TimeValue getReplicateFor() {
        return replicateFor;
    }

    @Nullable
    public Boolean isForceMergeOnClone() {
        return forceMergeOnClone;
    }

    @Override
    public List<Step> toSteps(Client client, String phase, StepKey nextStepKey) {
        assert false;
        throw new UnsupportedOperationException();
    }

    @Override
    public List<Step> toSteps(Client client, String phase, StepKey nextStepKey, XPackLicenseState licenseState) {
        StepKey preActionBranchingKey = new StepKey(phase, NAME, CONDITIONAL_SKIP_ACTION_STEP);
        StepKey checkNoWriteIndex = new StepKey(phase, NAME, CheckNotDataStreamWriteIndexStep.NAME);
        StepKey waitForNoFollowerStepKey = new StepKey(phase, NAME, WaitForNoFollowersStep.NAME);
        StepKey waitTimeSeriesEndTimePassesKey = new StepKey(phase, NAME, WaitUntilTimeSeriesEndTimePassesStep.NAME);
        StepKey skipGeneratingSnapshotKey = new StepKey(phase, NAME, CONDITIONAL_SKIP_GENERATE_AND_CLEAN);

        StepKey conditionalSkipCloneKey = new StepKey(phase, NAME, CONDITIONAL_SKIP_CLONE_STEP);
        StepKey readOnlyKey = new StepKey(phase, NAME, ReadOnlyStep.NAME);
        StepKey cleanupClonedIndexKey = new StepKey(phase, NAME, CleanupGeneratedIndexStep.NAME);
        StepKey generateCloneIndexNameKey = new StepKey(phase, NAME, GenerateUniqueIndexNameStep.NAME);
        StepKey cloneIndexKey = new StepKey(phase, NAME, ResizeIndexStep.CLONE);
        StepKey waitForClonedIndexGreenKey = new StepKey(phase, NAME, WAIT_FOR_CLONED_INDEX_GREEN);
        StepKey forceMergeStepKey = new StepKey(phase, NAME, ForceMergeStep.NAME);
        StepKey waitForSegmentCountKey = new StepKey(phase, NAME, SegmentCountStep.NAME);

        StepKey generateSnapshotNameKey = new StepKey(phase, NAME, GenerateSnapshotNameStep.NAME);
        StepKey cleanSnapshotKey = new StepKey(phase, NAME, CleanupSnapshotStep.NAME);
        StepKey createSnapshotKey = new StepKey(phase, NAME, CreateSnapshotStep.NAME);
        StepKey waitForDataTierKey = new StepKey(phase, NAME, WaitForDataTierStep.NAME);
        StepKey mountSnapshotKey = new StepKey(phase, NAME, MountSnapshotStep.NAME);
        StepKey waitForGreenRestoredIndexKey = new StepKey(phase, NAME, WaitForIndexColorStep.NAME);
        StepKey copyMetadataKey = new StepKey(phase, NAME, CopyExecutionStateStep.NAME);
        StepKey dataStreamCheckBranchingKey = new StepKey(phase, NAME, CONDITIONAL_DATASTREAM_CHECK_KEY);
        StepKey copyLifecyclePolicySettingKey = new StepKey(phase, NAME, CopySettingsStep.NAME);
        StepKey swapAliasesKey = new StepKey(phase, NAME, SwapAliasesAndDeleteSourceIndexStep.NAME);
        StepKey replaceDataStreamIndexKey = new StepKey(phase, NAME, ReplaceDataStreamBackingIndexStep.NAME);
        StepKey deleteSourceIndexKey = new StepKey(phase, NAME, DeleteStep.NAME);
        StepKey conditionalDeleteForceMergedIndexKey = new StepKey(phase, NAME, CONDITIONAL_DELETE_FORCE_MERGED_INDEX_KEY);
        StepKey deleteForceMergedIndexKey = new StepKey(phase, NAME, DELETE_FORCE_MERGED_INDEX_KEY);
        StepKey replicateForKey = new StepKey(phase, NAME, WaitUntilReplicateForTimePassesStep.NAME);
        StepKey dropReplicasKey = new StepKey(phase, NAME, UpdateSettingsStep.NAME);

        // Before going through all these steps, first check if we need to do them at all. For example, the index could already be
        // a searchable snapshot of the same type and repository, in which case we don't need to do anything. If that is detected,
        // this branching step jumps right to the end, skipping the searchable snapshot action entirely. We also check the license
        // here before generating snapshots that can't be used if the user doesn't have the right license level.
        BranchingStep conditionalSkipActionStep = new BranchingStep(
            preActionBranchingKey,
            checkNoWriteIndex,
            nextStepKey,
            (index, project) -> {
                if (SEARCHABLE_SNAPSHOT_FEATURE.checkWithoutTracking(licenseState) == false) {
                    logger.error("[{}] action is not available in the current license", SearchableSnapshotAction.NAME);
                    throw LicenseUtils.newComplianceException("searchable-snapshots");
                }

                IndexMetadata indexMetadata = project.index(index);
                assert indexMetadata != null : "index " + index.getName() + " must exist in the cluster state";
                String policyName = indexMetadata.getLifecyclePolicyName();
                SearchableSnapshotMetadata searchableSnapshotMetadata = extractSearchableSnapshotFromSettings(indexMetadata);
                if (searchableSnapshotMetadata != null) {
                    // TODO: allow this behavior instead of returning false, in this case the index is already a searchable a snapshot
                    // so the most graceful way of recovery might be to use this repo
                    // The index is already a searchable snapshot, let's see if the repository matches
                    if (this.snapshotRepository.equals(searchableSnapshotMetadata.repositoryName) == false) {
                        // Okay, different repo, we need to go ahead with the searchable snapshot
                        logger.debug(
                            "[{}] action is configured for index [{}] in policy [{}] which is already mounted as a searchable "
                                + "snapshot, but with a different repository (existing: [{}] vs new: [{}]), a new snapshot and "
                                + "index will be created",
                            SearchableSnapshotAction.NAME,
                            index.getName(),
                            policyName,
                            searchableSnapshotMetadata.repositoryName,
                            this.snapshotRepository
                        );
                        return false;
                    }

                    // Check to the storage type to see if we need to convert between full <-> partial
                    MountSearchableSnapshotRequest.Storage existingType = searchableSnapshotMetadata.partial
                        ? MountSearchableSnapshotRequest.Storage.SHARED_CACHE
                        : MountSearchableSnapshotRequest.Storage.FULL_COPY;
                    MountSearchableSnapshotRequest.Storage type = getConcreteStorageType(preActionBranchingKey);
                    if (existingType == type) {
                        logger.debug(
                            "[{}] action is configured for index [{}] in policy [{}] which is already mounted "
                                + "as a searchable snapshot with the same repository [{}] and storage type [{}], skipping this action",
                            SearchableSnapshotAction.NAME,
                            index.getName(),
                            policyName,
                            searchableSnapshotMetadata.repositoryName,
                            type
                        );
                        return true;
                    }

                    logger.debug(
                        "[{}] action is configured for index [{}] in policy [{}] which is already mounted "
                            + "as a searchable snapshot in repository [{}], however, the storage type ([{}] vs [{}]) "
                            + "differs, so a new index will be created",
                        SearchableSnapshotAction.NAME,
                        index.getName(),
                        policyName,
                        this.snapshotRepository,
                        existingType,
                        type
                    );
                    // Perform the searchable snapshot
                    return false;
                }
                // Perform the searchable snapshot, as the index is not currently a searchable snapshot
                return false;
            }
        );
        CheckNotDataStreamWriteIndexStep checkNoWriteIndexStep = new CheckNotDataStreamWriteIndexStep(
            checkNoWriteIndex,
            waitForNoFollowerStepKey
        );
        WaitForNoFollowersStep waitForNoFollowersStep = new WaitForNoFollowersStep(
            waitForNoFollowerStepKey,
            waitTimeSeriesEndTimePassesKey,
            client
        );
        WaitUntilTimeSeriesEndTimePassesStep waitUntilTimeSeriesEndTimeStep = new WaitUntilTimeSeriesEndTimePassesStep(
            waitTimeSeriesEndTimePassesKey,
            skipGeneratingSnapshotKey,
            Instant::now
        );

        // We force-merge on the clone by default, but allow the user to opt-out of this behavior if there is any reason why they don't want
        // to clone the index (e.g. if something is preventing the cloned index shards from being assigned).
        StepKey keyForForceMerge = shouldForceMergeOnClone() ? conditionalSkipCloneKey : forceMergeStepKey;
        // When generating a snapshot, we either jump to the force merge section, or we skip the
        // forcemerge and go straight to steps for creating the snapshot
        StepKey keyForSnapshotGeneration = forceMergeIndex ? keyForForceMerge : generateSnapshotNameKey;
        // Branch, deciding whether there is an existing searchable snapshot that can be used for mounting the index
        // (in which case, skip generating a new name and the snapshot cleanup), or if we need to generate a new snapshot
        BranchingStep skipGeneratingSnapshotStep = new BranchingStep(
            skipGeneratingSnapshotKey,
            keyForSnapshotGeneration,
            waitForDataTierKey,
            (index, project) -> {
                IndexMetadata indexMetadata = project.index(index);
                String policyName = indexMetadata.getLifecyclePolicyName();
                LifecycleExecutionState lifecycleExecutionState = indexMetadata.getLifecycleExecutionState();
                SearchableSnapshotMetadata searchableSnapshotMetadata = extractSearchableSnapshotFromSettings(indexMetadata);
                if (lifecycleExecutionState.snapshotName() == null && searchableSnapshotMetadata == null) {
                    // No name exists, so it must be generated
                    logger.trace(
                        "no snapshot name for index [{}] in policy [{}] exists, so one will be generated",
                        index.getName(),
                        policyName
                    );
                    return false;
                }
                String snapshotIndexName;
                String snapshotName;
                String repoName;
                if (lifecycleExecutionState.snapshotName() != null) {
                    snapshotIndexName = lifecycleExecutionState.snapshotIndexName();
                    snapshotName = lifecycleExecutionState.snapshotName();
                    repoName = lifecycleExecutionState.snapshotRepository();
                } else {
                    snapshotIndexName = searchableSnapshotMetadata.sourceIndex;
                    snapshotName = searchableSnapshotMetadata.snapshotName;
                    repoName = searchableSnapshotMetadata.repositoryName;
                }

                if (this.snapshotRepository.equals(repoName) == false) {
                    // A different repository is being used
                    // TODO: allow this behavior instead of throwing an exception
                    throw new IllegalArgumentException("searchable snapshot indices may be converted only within the same repository");
                }

                // We can skip the generate, initial cleanup, and snapshot taking for this index, as we already have a generated snapshot.
                // This will jump ahead directly to the "mount snapshot" step
                logger.debug(
                    "Policy [{}] will use an existing snapshot [{}] in repository [{}] (index name: [{}]) "
                        + "to mount [{}] as a searchable snapshot. This snapshot was found in the {}.",
                    policyName,
                    snapshotName,
                    snapshotRepository,
                    snapshotIndexName,
                    index.getName(),
                    lifecycleExecutionState.snapshotName() != null ? "lifecycle execution state" : "metadata of " + index.getName()
                );
                return true;
            }
        );

        // If a new snapshot is needed, these steps are executed
        // If the index has replicas, we need to clone the index first with 0 replicas and perform the force-merge on that index.
        // That avoids us having to force-merge the replica shards too, which is a waste, as the snapshot will only be taken from the
        // primary shards. If the index already has 0 replicas, we can skip the clone steps.
        BranchingStep conditionalSkipCloneStep = new BranchingStep(
            conditionalSkipCloneKey,
            readOnlyKey,
            forceMergeStepKey,
            (index, project) -> {
                IndexMetadata indexMetadata = project.index(index);
                assert indexMetadata != null : "index " + index.getName() + " must exist in the cluster state";
                return indexMetadata.getNumberOfReplicas() == 0;
            }
        );
        ReadOnlyStep readOnlyStep = new ReadOnlyStep(readOnlyKey, cleanupClonedIndexKey, client, false);
        // If a previous step created a clone index but the action did not complete, we need to clean up the old clone index.
        CleanupGeneratedIndexStep cleanupClonedIndexStep = new CleanupGeneratedIndexStep(
            cleanupClonedIndexKey,
            generateCloneIndexNameKey,
            client,
            FORCE_MERGE_CLONE_INDEX_NAME_SUPPLIER
        );
        GenerateUniqueIndexNameStep generateCloneIndexNameStep = new GenerateUniqueIndexNameStep(
            generateCloneIndexNameKey,
            cloneIndexKey,
            FORCE_MERGE_CLONE_INDEX_PREFIX,
            (generatedIndexName, lifecycleStateBuilder) -> lifecycleStateBuilder.setForceMergeCloneIndexName(generatedIndexName)
        );
        // Clone the index with 0 replicas.
        ResizeIndexStep cloneIndexStep = new ResizeIndexStep(
            cloneIndexKey,
            waitForClonedIndexGreenKey,
            client,
            ResizeType.CLONE,
            FORCE_MERGE_CLONE_INDEX_NAME_SUPPLIER,
            CLONE_SETTINGS_SUPPLIER,
            null
        );
        // Wait for the cloned index to be green before proceeding with the force-merge. We wrap this with a
        // ClusterStateWaitUntilThresholdStep to avoid waiting forever if the index cannot be started for some reason.
        // On timeout, ILM will move back to the cleanup step, remove the cloned index, and retry the clone.
        ClusterStateWaitUntilThresholdStep waitForClonedIndexGreenStep = new ClusterStateWaitUntilThresholdStep(
            new WaitForIndexColorStep(
                waitForClonedIndexGreenKey,
                forceMergeStepKey,
                ClusterHealthStatus.GREEN,
                FORCE_MERGE_CLONE_INDEX_NAME_SUPPLIER
            ),
            cleanupClonedIndexKey
        );
        ForceMergeStep forceMergeStep = new ForceMergeStep(
            forceMergeStepKey,
            waitForSegmentCountKey,
            client,
            1,
            FORCE_MERGE_CLONE_INDEX_NAME_FALLBACK_SUPPLIER
        );
        SegmentCountStep segmentCountStep = new SegmentCountStep(
            waitForSegmentCountKey,
            generateSnapshotNameKey,
            client,
            1,
            FORCE_MERGE_CLONE_INDEX_NAME_FALLBACK_SUPPLIER
        );
        GenerateSnapshotNameStep generateSnapshotNameStep = new GenerateSnapshotNameStep(
            generateSnapshotNameKey,
            cleanSnapshotKey,
            snapshotRepository,
            FORCE_MERGE_CLONE_INDEX_NAME_FALLBACK_SUPPLIER
        );
        CleanupSnapshotStep cleanupSnapshotStep = new CleanupSnapshotStep(cleanSnapshotKey, createSnapshotKey, client);
        CreateSnapshotStep createSnapshotStep = new CreateSnapshotStep(createSnapshotKey, waitForDataTierKey, cleanSnapshotKey, client);

        MountSearchableSnapshotRequest.Storage storageType = getConcreteStorageType(mountSnapshotKey);

        // If the skipGeneratingSnapshotStep determined a snapshot already existed that
        // can be used, it jumps directly here, skipping the snapshot generation steps above.
        WaitForDataTierStep waitForDataTierStep = new WaitForDataTierStep(
            waitForDataTierKey,
            mountSnapshotKey,
            MountSnapshotStep.overrideTierPreference(phase).orElse(storageType.defaultDataTiersPreference())
        );
        MountSnapshotStep mountSnapshotStep = new MountSnapshotStep(
            mountSnapshotKey,
            waitForGreenRestoredIndexKey,
            client,
            getRestoredIndexPrefix(mountSnapshotKey),
            storageType,
            totalShardsPerNode,
            replicateFor != null ? 1 : 0 // if the 'replicate_for' option is set, then have a replica, otherwise don't
        );
        WaitForIndexColorStep waitForGreenIndexHealthStep = new WaitForIndexColorStep(
            waitForGreenRestoredIndexKey,
            copyMetadataKey,
            ClusterHealthStatus.GREEN,
            getRestoredIndexPrefix(waitForGreenRestoredIndexKey)
        );
        StepKey keyForReplicateForOrContinue = replicateFor != null ? replicateForKey : nextStepKey;
        CopyExecutionStateStep copyMetadataStep = new CopyExecutionStateStep(
            copyMetadataKey,
            copyLifecyclePolicySettingKey,
            (index, executionState) -> getRestoredIndexPrefix(copyMetadataKey) + index,
            keyForReplicateForOrContinue
        );
        CopySettingsStep copySettingsStep = new CopySettingsStep(
            copyLifecyclePolicySettingKey,
            forceMergeIndex ? conditionalDeleteForceMergedIndexKey : dataStreamCheckBranchingKey,
            (index, lifecycleState) -> getRestoredIndexPrefix(copyLifecyclePolicySettingKey) + index,
            LifecycleSettings.LIFECYCLE_NAME
        );
        // If we cloned the index, we need to delete it before we swap the mounted snapshot in place of the original index.
        // If we did not clone the index, there's nothing else for us to do.
        BranchingStep conditionalDeleteForceMergedIndexStep = new BranchingStep(
            conditionalDeleteForceMergedIndexKey,
            dataStreamCheckBranchingKey,
            deleteForceMergedIndexKey,
            (index, project) -> {
                IndexMetadata indexMetadata = project.index(index);
                assert indexMetadata != null : "index " + index.getName() + " must exist in the cluster state";
                String cloneIndexName = indexMetadata.getLifecycleExecutionState().forceMergeCloneIndexName();
                return cloneIndexName != null && project.index(cloneIndexName) != null;
            }
        );
        DeleteStep deleteForceMergedIndexStep = new DeleteStep(
            deleteForceMergedIndexKey,
            dataStreamCheckBranchingKey,
            client,
            FORCE_MERGE_CLONE_INDEX_NAME_SUPPLIER,
            true
        );
        BranchingStep isDataStreamBranchingStep = new BranchingStep(
            dataStreamCheckBranchingKey,
            swapAliasesKey,
            replaceDataStreamIndexKey,
            (index, project) -> {
                IndexAbstraction indexAbstraction = project.getIndicesLookup().get(index.getName());
                assert indexAbstraction != null : "invalid cluster metadata. index [" + index.getName() + "] was not found";
                return indexAbstraction.getParentDataStream() != null;
            }
        );
        ReplaceDataStreamBackingIndexStep replaceDataStreamBackingIndex = new ReplaceDataStreamBackingIndexStep(
            replaceDataStreamIndexKey,
            deleteSourceIndexKey,
            (index, executionState) -> getRestoredIndexPrefix(replaceDataStreamIndexKey) + index
        );
        DeleteStep deleteSourceIndexStep = new DeleteStep(deleteSourceIndexKey, null, client);
        // sending this step to null as the restored index (which will after this step essentially be the source index) was sent to the next
        // key after we restored the lifecycle execution state
        SwapAliasesAndDeleteSourceIndexStep swapAliasesAndDeleteSourceIndexStep = new SwapAliasesAndDeleteSourceIndexStep(
            swapAliasesKey,
            null,
            client,
            getRestoredIndexPrefix(swapAliasesKey)
        );

        // note that the replicateForStep and dropReplicasStep will only be used if replicateFor != null, see the construction of
        // the list of steps below
        Step replicateForStep = new WaitUntilReplicateForTimePassesStep(replicateForKey, dropReplicasKey, replicateFor);
        UpdateSettingsStep dropReplicasStep = new UpdateSettingsStep(
            dropReplicasKey,
            nextStepKey,
            client,
            Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0).build()
        );

        List<Step> steps = new ArrayList<>();
        steps.add(conditionalSkipActionStep);
        steps.add(checkNoWriteIndexStep);
        steps.add(waitForNoFollowersStep);
        steps.add(waitUntilTimeSeriesEndTimeStep);
        steps.add(skipGeneratingSnapshotStep);
        if (forceMergeIndex) {
            if (shouldForceMergeOnClone()) {
                steps.add(conditionalSkipCloneStep);
                steps.add(readOnlyStep);
                steps.add(cleanupClonedIndexStep);
                steps.add(generateCloneIndexNameStep);
                steps.add(cloneIndexStep);
                steps.add(waitForClonedIndexGreenStep);
            }
            steps.add(forceMergeStep);
            steps.add(segmentCountStep);
        }
        steps.add(generateSnapshotNameStep);
        steps.add(cleanupSnapshotStep);
        steps.add(createSnapshotStep);
        steps.add(waitForDataTierStep);
        steps.add(mountSnapshotStep);
        steps.add(waitForGreenIndexHealthStep);
        steps.add(copyMetadataStep);
        steps.add(copySettingsStep);
        if (replicateFor != null) {
            steps.add(replicateForStep);
            steps.add(dropReplicasStep);
        }
        if (forceMergeIndex) {
            steps.add(conditionalDeleteForceMergedIndexStep);
            steps.add(deleteForceMergedIndexStep);
        }
        steps.add(isDataStreamBranchingStep);
        steps.add(replaceDataStreamBackingIndex);
        steps.add(deleteSourceIndexStep);
        steps.add(swapAliasesAndDeleteSourceIndexStep);
        return steps;
    }

    /**
     * Resolves the prefix to be used for the mounted index depending on the provided key
     */
    static String getRestoredIndexPrefix(StepKey currentKey) {
        if (currentKey.phase().equals(TimeseriesLifecycleType.FROZEN_PHASE)) {
            return PARTIAL_RESTORED_INDEX_PREFIX;
        } else {
            return FULL_RESTORED_INDEX_PREFIX;
        }
    }

    // Resolves the storage type depending on which phase the index is in
    static MountSearchableSnapshotRequest.Storage getConcreteStorageType(StepKey currentKey) {
        if (currentKey.phase().equals(TimeseriesLifecycleType.FROZEN_PHASE)) {
            return MountSearchableSnapshotRequest.Storage.SHARED_CACHE;
        } else {
            return MountSearchableSnapshotRequest.Storage.FULL_COPY;
        }
    }

    /**
     * Returns whether we should first clone the index and perform the force-merge on that cloned index (true) or force-merge on the
     * original index (false). Defaults to true when {@link #forceMergeOnClone} is null/unspecified. Note that this value is ignored when
     * {@link #forceMergeIndex} is false.
     */
    private boolean shouldForceMergeOnClone() {
        return forceMergeOnClone == null || forceMergeOnClone;
    }

    @Override
    public boolean isSafeAction() {
        return true;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(snapshotRepository);
        out.writeBoolean(forceMergeIndex);
        if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_16_0)) {
            out.writeOptionalInt(totalShardsPerNode);
        }
        if (out.getTransportVersion().supports(TransportVersions.V_8_18_0)) {
            out.writeOptionalTimeValue(replicateFor);
        }
        if (out.getTransportVersion().supports(FORCE_MERGE_ON_CLONE_TRANSPORT_VERSION)) {
            out.writeOptionalBoolean(forceMergeOnClone);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(SNAPSHOT_REPOSITORY.getPreferredName(), snapshotRepository);
        builder.field(FORCE_MERGE_INDEX.getPreferredName(), forceMergeIndex);
        if (totalShardsPerNode != null) {
            builder.field(TOTAL_SHARDS_PER_NODE.getPreferredName(), totalShardsPerNode);
        }
        if (replicateFor != null) {
            builder.field(REPLICATE_FOR.getPreferredName(), replicateFor);
        }
        if (forceMergeOnClone != null) {
            builder.field(FORCE_MERGE_ON_CLONE.getPreferredName(), forceMergeOnClone);
        }
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
        SearchableSnapshotAction that = (SearchableSnapshotAction) o;
        return Objects.equals(snapshotRepository, that.snapshotRepository)
            && Objects.equals(forceMergeIndex, that.forceMergeIndex)
            && Objects.equals(totalShardsPerNode, that.totalShardsPerNode)
            && Objects.equals(replicateFor, that.replicateFor)
            && Objects.equals(forceMergeOnClone, that.forceMergeOnClone);
    }

    @Override
    public int hashCode() {
        return Objects.hash(snapshotRepository, forceMergeIndex, totalShardsPerNode, replicateFor, forceMergeOnClone);
    }

    @Nullable
    static SearchableSnapshotMetadata extractSearchableSnapshotFromSettings(IndexMetadata indexMetadata) {
        String indexName = indexMetadata.getSettings().get(LifecycleSettings.SNAPSHOT_INDEX_NAME);
        if (indexName == null) {
            return null;
        }
        String snapshotName = indexMetadata.getSettings().get(SEARCHABLE_SNAPSHOTS_SNAPSHOT_NAME_SETTING_KEY);
        String repo = indexMetadata.getSettings().get(SEARCHABLE_SNAPSHOTS_REPOSITORY_NAME_SETTING_KEY);
        final boolean partial = indexMetadata.getSettings().getAsBoolean(SEARCHABLE_SNAPSHOT_PARTIAL_SETTING_KEY, false);
        return new SearchableSnapshotMetadata(indexName, repo, snapshotName, partial);
    }

    record SearchableSnapshotMetadata(String sourceIndex, String repositoryName, String snapshotName, boolean partial) {}
}
