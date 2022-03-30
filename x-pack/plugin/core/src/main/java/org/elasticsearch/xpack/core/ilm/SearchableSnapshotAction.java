/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ilm;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.LifecycleExecutionState;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.license.LicenseUtils;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ilm.Step.StepKey;
import org.elasticsearch.xpack.core.searchablesnapshots.MountSearchableSnapshotRequest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.snapshots.SearchableSnapshotsSettings.SEARCHABLE_SNAPSHOTS_REPOSITORY_NAME_SETTING_KEY;
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
    public static final String CONDITIONAL_DATASTREAM_CHECK_KEY = BranchingStep.NAME + "-on-datastream-check";
    public static final String CONDITIONAL_SKIP_ACTION_STEP = BranchingStep.NAME + "-check-prerequisites";
    public static final String CONDITIONAL_SKIP_GENERATE_AND_CLEAN = BranchingStep.NAME + "-check-existing-snapshot";

    public static final String FULL_RESTORED_INDEX_PREFIX = "restored-";
    public static final String PARTIAL_RESTORED_INDEX_PREFIX = "partial-";

    private static final ConstructingObjectParser<SearchableSnapshotAction, Void> PARSER = new ConstructingObjectParser<>(
        NAME,
        a -> new SearchableSnapshotAction((String) a[0], a[1] == null || (boolean) a[1])
    );

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), SNAPSHOT_REPOSITORY);
        PARSER.declareBoolean(ConstructingObjectParser.optionalConstructorArg(), FORCE_MERGE_INDEX);
    }

    public static SearchableSnapshotAction parse(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    private final String snapshotRepository;
    private final boolean forceMergeIndex;

    public SearchableSnapshotAction(String snapshotRepository, boolean forceMergeIndex) {
        if (Strings.hasText(snapshotRepository) == false) {
            throw new IllegalArgumentException("the snapshot repository must be specified");
        }
        this.snapshotRepository = snapshotRepository;
        this.forceMergeIndex = forceMergeIndex;
    }

    public SearchableSnapshotAction(String snapshotRepository) {
        this(snapshotRepository, true);
    }

    public SearchableSnapshotAction(StreamInput in) throws IOException {
        this.snapshotRepository = in.readString();
        this.forceMergeIndex = in.readBoolean();
    }

    boolean isForceMergeIndex() {
        return forceMergeIndex;
    }

    public String getSnapshotRepository() {
        return snapshotRepository;
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
        StepKey forceMergeStepKey = new StepKey(phase, NAME, ForceMergeStep.NAME);
        StepKey waitForSegmentCountKey = new StepKey(phase, NAME, SegmentCountStep.NAME);
        StepKey skipGeneratingSnapshotKey = new StepKey(phase, NAME, CONDITIONAL_SKIP_GENERATE_AND_CLEAN);
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
        StepKey deleteIndexKey = new StepKey(phase, NAME, DeleteStep.NAME);

        // Before going through all these steps, first check if we need to do them at all. For example, the index could already be
        // a searchable snapshot of the same type and repository, in which case we don't need to do anything. If that is detected,
        // this branching step jumps right to the end, skipping the searchable snapshot action entirely. We also check the license
        // here before generating snapshots that can't be used if the user doesn't have the right license level.
        BranchingStep conditionalSkipActionStep = new BranchingStep(
            preActionBranchingKey,
            checkNoWriteIndex,
            nextStepKey,
            (index, clusterState) -> {
                if (SEARCHABLE_SNAPSHOT_FEATURE.checkWithoutTracking(licenseState) == false) {
                    logger.error("[{}] action is not available in the current license", SearchableSnapshotAction.NAME);
                    throw LicenseUtils.newComplianceException("searchable-snapshots");
                }

                IndexMetadata indexMetadata = clusterState.getMetadata().index(index);
                assert indexMetadata != null : "index " + index.getName() + " must exist in the cluster state";
                String policyName = indexMetadata.getLifecyclePolicyName();
                if (indexMetadata.getSettings().get(LifecycleSettings.SNAPSHOT_INDEX_NAME) != null) {
                    // The index is already a searchable snapshot, let's see if the repository matches
                    String repo = indexMetadata.getSettings().get(SEARCHABLE_SNAPSHOTS_REPOSITORY_NAME_SETTING_KEY);
                    if (this.snapshotRepository.equals(repo) == false) {
                        // Okay, different repo, we need to go ahead with the searchable snapshot
                        logger.debug(
                            "[{}] action is configured for index [{}] in policy [{}] which is already mounted as a searchable "
                                + "snapshot, but with a different repository (existing: [{}] vs new: [{}]), a new snapshot and "
                                + "index will be created",
                            SearchableSnapshotAction.NAME,
                            index.getName(),
                            policyName,
                            repo,
                            this.snapshotRepository
                        );
                        return false;
                    }

                    // Check to the storage type to see if we need to convert between full <-> partial
                    final boolean partial = indexMetadata.getSettings().getAsBoolean(SEARCHABLE_SNAPSHOT_PARTIAL_SETTING_KEY, false);
                    MountSearchableSnapshotRequest.Storage existingType = partial
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
                            repo,
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
            skipGeneratingSnapshotKey,
            client
        );

        // When generating a snapshot, we either jump to the force merge step, or we skip the
        // forcemerge and go straight to steps for creating the snapshot
        StepKey keyForSnapshotGeneration = forceMergeIndex ? forceMergeStepKey : generateSnapshotNameKey;
        // Branch, deciding whether there is an existing searchable snapshot snapshot that can be used for mounting the index
        // (in which case, skip generating a new name and the snapshot cleanup), or if we need to generate a new snapshot
        BranchingStep skipGeneratingSnapshotStep = new BranchingStep(
            skipGeneratingSnapshotKey,
            keyForSnapshotGeneration,
            waitForDataTierKey,
            (index, clusterState) -> {
                IndexMetadata indexMetadata = clusterState.getMetadata().index(index);
                String policyName = indexMetadata.getLifecyclePolicyName();
                LifecycleExecutionState lifecycleExecutionState = indexMetadata.getLifecycleExecutionState();
                if (lifecycleExecutionState.snapshotName() == null) {
                    // No name exists, so it must be generated
                    logger.trace(
                        "no snapshot name for index [{}] in policy [{}] exists, so one will be generated",
                        index.getName(),
                        policyName
                    );
                    return false;
                }

                if (this.snapshotRepository.equals(lifecycleExecutionState.snapshotRepository()) == false) {
                    // A different repository is being used
                    // TODO: allow this behavior instead of throwing an exception
                    throw new IllegalArgumentException("searchable snapshot indices may be converted only within the same repository");
                }

                // We can skip the generate, initial cleanup, and snapshot taking for this index, as we already have a generated snapshot.
                // This will jump ahead directly to the "mount snapshot" step
                logger.debug(
                    "an existing snapshot [{}] in repository [{}] (index name: [{}]) "
                        + "will be used for mounting [{}] as a searchable snapshot",
                    lifecycleExecutionState.snapshotName(),
                    lifecycleExecutionState.snapshotRepository(),
                    lifecycleExecutionState.snapshotIndexName(),
                    index.getName()
                );
                return true;
            }
        );

        // If a new snapshot is needed, these steps are executed
        ForceMergeStep forceMergeStep = new ForceMergeStep(forceMergeStepKey, waitForSegmentCountKey, client, 1);
        SegmentCountStep segmentCountStep = new SegmentCountStep(waitForSegmentCountKey, generateSnapshotNameKey, client, 1);
        GenerateSnapshotNameStep generateSnapshotNameStep = new GenerateSnapshotNameStep(
            generateSnapshotNameKey,
            cleanSnapshotKey,
            snapshotRepository
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
            storageType
        );
        WaitForIndexColorStep waitForGreenIndexHealthStep = new WaitForIndexColorStep(
            waitForGreenRestoredIndexKey,
            copyMetadataKey,
            ClusterHealthStatus.GREEN,
            getRestoredIndexPrefix(waitForGreenRestoredIndexKey)
        );
        CopyExecutionStateStep copyMetadataStep = new CopyExecutionStateStep(
            copyMetadataKey,
            copyLifecyclePolicySettingKey,
            (index, executionState) -> getRestoredIndexPrefix(copyMetadataKey) + index,
            nextStepKey
        );
        CopySettingsStep copySettingsStep = new CopySettingsStep(
            copyLifecyclePolicySettingKey,
            dataStreamCheckBranchingKey,
            getRestoredIndexPrefix(copyLifecyclePolicySettingKey),
            LifecycleSettings.LIFECYCLE_NAME
        );
        BranchingStep isDataStreamBranchingStep = new BranchingStep(
            dataStreamCheckBranchingKey,
            swapAliasesKey,
            replaceDataStreamIndexKey,
            (index, clusterState) -> {
                IndexAbstraction indexAbstraction = clusterState.metadata().getIndicesLookup().get(index.getName());
                assert indexAbstraction != null : "invalid cluster metadata. index [" + index.getName() + "] was not found";
                return indexAbstraction.getParentDataStream() != null;
            }
        );
        ReplaceDataStreamBackingIndexStep replaceDataStreamBackingIndex = new ReplaceDataStreamBackingIndexStep(
            replaceDataStreamIndexKey,
            deleteIndexKey,
            (index, executionState) -> getRestoredIndexPrefix(replaceDataStreamIndexKey) + index
        );
        DeleteStep deleteSourceIndexStep = new DeleteStep(deleteIndexKey, null, client);
        // sending this step to null as the restored index (which will after this step essentially be the source index) was sent to the next
        // key after we restored the lifecycle execution state
        SwapAliasesAndDeleteSourceIndexStep swapAliasesAndDeleteSourceIndexStep = new SwapAliasesAndDeleteSourceIndexStep(
            swapAliasesKey,
            null,
            client,
            getRestoredIndexPrefix(swapAliasesKey)
        );

        List<Step> steps = new ArrayList<>();
        steps.add(conditionalSkipActionStep);
        steps.add(checkNoWriteIndexStep);
        steps.add(waitForNoFollowersStep);
        steps.add(skipGeneratingSnapshotStep);
        if (forceMergeIndex) {
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
        if (currentKey.getPhase().equals(TimeseriesLifecycleType.FROZEN_PHASE)) {
            return PARTIAL_RESTORED_INDEX_PREFIX;
        } else {
            return FULL_RESTORED_INDEX_PREFIX;
        }
    }

    // Resolves the storage type depending on which phase the index is in
    static MountSearchableSnapshotRequest.Storage getConcreteStorageType(StepKey currentKey) {
        if (currentKey.getPhase().equals(TimeseriesLifecycleType.FROZEN_PHASE)) {
            return MountSearchableSnapshotRequest.Storage.SHARED_CACHE;
        } else {
            return MountSearchableSnapshotRequest.Storage.FULL_COPY;
        }
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
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(SNAPSHOT_REPOSITORY.getPreferredName(), snapshotRepository);
        builder.field(FORCE_MERGE_INDEX.getPreferredName(), forceMergeIndex);
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
        return Objects.equals(snapshotRepository, that.snapshotRepository);
    }

    @Override
    public int hashCode() {
        return Objects.hash(snapshotRepository);
    }
}
