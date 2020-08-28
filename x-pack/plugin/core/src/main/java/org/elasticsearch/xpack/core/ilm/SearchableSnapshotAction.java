/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.Version;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ilm.Step.StepKey;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * A {@link LifecycleAction} that will convert the index into a searchable snapshot, by taking a snapshot of the index, creating a
 * searchable snapshot and the corresponding "searchable snapshot index", deleting the original index and swapping its aliases to the
 * newly created searchable snapshot backed index.
 */
public class SearchableSnapshotAction implements LifecycleAction {
    public static final String NAME = "searchable_snapshot";

    public static final ParseField SNAPSHOT_REPOSITORY = new ParseField("snapshot_repository");
    public static final ParseField FORCE_MERGE_INDEX = new ParseField("force_merge_index");
    public static final String CONDITIONAL_DATASTREAM_CHECK_KEY = BranchingStep.NAME + "-on-datastream-check";

    public static final String RESTORED_INDEX_PREFIX = "restored-";

    private static final ConstructingObjectParser<SearchableSnapshotAction, Void> PARSER = new ConstructingObjectParser<>(NAME,
        a -> new SearchableSnapshotAction((String) a[0], a[1] == null || (boolean) a[1]));

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
        this(in.readString(), in.getVersion().onOrAfter(Version.V_7_10_0) ? in.readBoolean() : true);
    }

    boolean isForceMergeIndex() {
        return forceMergeIndex;
    }

    @Override
    public List<Step> toSteps(Client client, String phase, StepKey nextStepKey) {
        StepKey checkNoWriteIndex = new StepKey(phase, NAME, CheckNotDataStreamWriteIndexStep.NAME);
        StepKey waitForNoFollowerStepKey = new StepKey(phase, NAME, WaitForNoFollowersStep.NAME);
        StepKey forceMergeStepKey = new StepKey(phase, NAME, ForceMergeStep.NAME);
        StepKey waitForSegmentCountKey = new StepKey(phase, NAME, SegmentCountStep.NAME);
        StepKey generateSnapshotNameKey = new StepKey(phase, NAME, GenerateSnapshotNameStep.NAME);
        StepKey cleanSnapshotKey = new StepKey(phase, NAME, CleanupSnapshotStep.NAME);
        StepKey createSnapshotKey = new StepKey(phase, NAME, CreateSnapshotStep.NAME);
        StepKey mountSnapshotKey = new StepKey(phase, NAME, MountSnapshotStep.NAME);
        StepKey waitForGreenRestoredIndexKey = new StepKey(phase, NAME, WaitForIndexColorStep.NAME);
        StepKey copyMetadataKey = new StepKey(phase, NAME, CopyExecutionStateStep.NAME);
        StepKey dataStreamCheckBranchingKey = new StepKey(phase, NAME, CONDITIONAL_DATASTREAM_CHECK_KEY);
        StepKey copyLifecyclePolicySettingKey = new StepKey(phase, NAME, CopySettingsStep.NAME);
        StepKey swapAliasesKey = new StepKey(phase, NAME, SwapAliasesAndDeleteSourceIndexStep.NAME);
        StepKey replaceDataStreamIndexKey = new StepKey(phase, NAME, ReplaceDataStreamBackingIndexStep.NAME);
        StepKey deleteIndexKey = new StepKey(phase, NAME, DeleteStep.NAME);

        CheckNotDataStreamWriteIndexStep checkNoWriteIndexStep = new CheckNotDataStreamWriteIndexStep(checkNoWriteIndex,
            waitForNoFollowerStepKey);
        final WaitForNoFollowersStep waitForNoFollowersStep;
        if (forceMergeIndex) {
            waitForNoFollowersStep = new WaitForNoFollowersStep(waitForNoFollowerStepKey, forceMergeStepKey, client);
        } else {
            waitForNoFollowersStep = new WaitForNoFollowersStep(waitForNoFollowerStepKey, generateSnapshotNameKey, client);
        }
        ForceMergeStep forceMergeStep = new ForceMergeStep(forceMergeStepKey, waitForSegmentCountKey, client, 1);
        SegmentCountStep segmentCountStep = new SegmentCountStep(waitForSegmentCountKey, generateSnapshotNameKey, client, 1);
        GenerateSnapshotNameStep generateSnapshotNameStep = new GenerateSnapshotNameStep(generateSnapshotNameKey, cleanSnapshotKey,
            snapshotRepository);
        CleanupSnapshotStep cleanupSnapshotStep = new CleanupSnapshotStep(cleanSnapshotKey, createSnapshotKey, client);
        AsyncActionBranchingStep createSnapshotBranchingStep = new AsyncActionBranchingStep(
            new CreateSnapshotStep(createSnapshotKey, mountSnapshotKey, client), cleanSnapshotKey, client);
        MountSnapshotStep mountSnapshotStep = new MountSnapshotStep(mountSnapshotKey, waitForGreenRestoredIndexKey,
            client, RESTORED_INDEX_PREFIX);
        WaitForIndexColorStep waitForGreenIndexHealthStep = new WaitForIndexColorStep(waitForGreenRestoredIndexKey,
            copyMetadataKey, ClusterHealthStatus.GREEN, RESTORED_INDEX_PREFIX);
        // a policy with only the cold phase will have a null "nextStepKey", hence the "null" nextStepKey passed in below when that's the
        // case
        CopyExecutionStateStep copyMetadataStep = new CopyExecutionStateStep(copyMetadataKey, copyLifecyclePolicySettingKey,
            RESTORED_INDEX_PREFIX, nextStepKey != null ? nextStepKey.getName() : "null");
        CopySettingsStep copySettingsStep = new CopySettingsStep(copyLifecyclePolicySettingKey, dataStreamCheckBranchingKey,
            RESTORED_INDEX_PREFIX, LifecycleSettings.LIFECYCLE_NAME);
        BranchingStep isDataStreamBranchingStep = new BranchingStep(dataStreamCheckBranchingKey, swapAliasesKey, replaceDataStreamIndexKey,
            (index, clusterState) -> {
                IndexAbstraction indexAbstraction = clusterState.metadata().getIndicesLookup().get(index.getName());
                assert indexAbstraction != null : "invalid cluster metadata. index [" + index.getName() + "] was not found";
                return indexAbstraction.getParentDataStream() != null;
            });
        ReplaceDataStreamBackingIndexStep replaceDataStreamBackingIndex = new ReplaceDataStreamBackingIndexStep(replaceDataStreamIndexKey,
            deleteIndexKey, RESTORED_INDEX_PREFIX);
        DeleteStep deleteSourceIndexStep = new DeleteStep(deleteIndexKey, null, client);
        // sending this step to null as the restored index (which will after this step essentially be the source index) was sent to the next
        // key after we restored the lifecycle execution state
        SwapAliasesAndDeleteSourceIndexStep swapAliasesAndDeleteSourceIndexStep = new SwapAliasesAndDeleteSourceIndexStep(swapAliasesKey,
            null, client, RESTORED_INDEX_PREFIX);

        List<Step> steps = new ArrayList<>();
        steps.add(checkNoWriteIndexStep);
        steps.add(waitForNoFollowersStep);
        if (forceMergeIndex) {
            steps.add(forceMergeStep);
            steps.add(segmentCountStep);
        }
        steps.add(generateSnapshotNameStep);
        steps.add(cleanupSnapshotStep);
        steps.add(createSnapshotBranchingStep);
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
        if (out.getVersion().onOrAfter(Version.V_7_10_0)) {
            out.writeBoolean(forceMergeIndex);
        }
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
