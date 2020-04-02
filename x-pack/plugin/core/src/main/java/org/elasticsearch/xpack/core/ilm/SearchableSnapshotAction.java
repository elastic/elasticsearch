/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.snapshots.status.SnapshotStatus;
import org.elasticsearch.action.admin.cluster.snapshots.status.SnapshotsStatusRequest;
import org.elasticsearch.action.admin.cluster.snapshots.status.SnapshotsStatusResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.TriConsumer;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ilm.OnAsyncWaitBranchingStep.BranchingStepListener;
import org.elasticsearch.xpack.core.ilm.Step.StepKey;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.cluster.SnapshotsInProgress.State.SUCCESS;
import static org.elasticsearch.snapshots.SnapshotState.IN_PROGRESS;

/**
 * A {@link LifecycleAction} that will convert the index into a searchable snapshot, by taking a snapshot of the index, creating a
 * searchable snapshot and the corresponding "searchable snapshot index", deleting the original index and swapping its aliases to the
 * newly created searchable snapshot backed index.
 */
public class SearchableSnapshotAction implements LifecycleAction {
    public static final String NAME = "searchable_snapshot";

    public static final ParseField SNAPSHOT_REPOSITORY = new ParseField("snapshot_repository");

    public static final String RESTORED_INDEX_PREFIX = "restored-";

    private static final ConstructingObjectParser<SearchableSnapshotAction, Void> PARSER = new ConstructingObjectParser<>(NAME,
        a -> new SearchableSnapshotAction((String) a[0]));

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), SNAPSHOT_REPOSITORY);
    }

    public static SearchableSnapshotAction parse(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    private final String snapshotRepository;

    public SearchableSnapshotAction(String snapshotRepository) {
        if (Strings.hasText(snapshotRepository) == false) {
            throw new IllegalArgumentException("the snapshot repository must be specified");
        }
        this.snapshotRepository = snapshotRepository;
    }

    public SearchableSnapshotAction(StreamInput in) throws IOException {
        this(in.readString());
    }

    @Override
    public List<Step> toSteps(Client client, String phase, StepKey nextStepKey) {
        StepKey waitForNoFollowerStepKey = new StepKey(phase, NAME, WaitForNoFollowersStep.NAME);
        StepKey generateSnapshotNameKey = new StepKey(phase, NAME, GenerateSnapshotNameStep.NAME);
        StepKey cleanSnapshotKey = new StepKey(phase, NAME, CleanupSnapshotStep.NAME);
        StepKey storeRepoGenKey = new StepKey(phase, NAME, StoreSnapshotRepositoryGenerationStep.NAME);
        StepKey createSnapshotKey = new StepKey(phase, NAME, CreateSnapshotStep.NAME);
        StepKey waitForRepoGenChangeKey = new StepKey(phase, NAME, WaitForRepositoryGenerationChangeStep.NAME);
        StepKey verifySnapshotStatusBranchingKey = new StepKey(phase, NAME, OnAsyncWaitBranchingStep.NAME);
        StepKey mountSnapshotKey = new StepKey(phase, NAME, MountSnapshotStep.NAME);
        StepKey waitForGreenRestoredIndexKey = new StepKey(phase, NAME, WaitForIndexColorStep.NAME);
        StepKey copyMetadataKey = new StepKey(phase, NAME, CopyExecutionStateStep.NAME);
        StepKey copyLifecyclePolicySettingKey = new StepKey(phase, NAME, CopySettingsStep.NAME);
        StepKey swapAliasesKey = new StepKey(phase, NAME, SwapAliasesAndDeleteSourceIndexStep.NAME);

        WaitForNoFollowersStep waitForNoFollowersStep = new WaitForNoFollowersStep(waitForNoFollowerStepKey, generateSnapshotNameKey,
            client);
        GenerateSnapshotNameStep generateSnapshotNameStep = new GenerateSnapshotNameStep(generateSnapshotNameKey, cleanSnapshotKey,
            snapshotRepository);
        CleanupSnapshotStep cleanupSnapshotStep = new CleanupSnapshotStep(cleanSnapshotKey, storeRepoGenKey, client);
        StoreSnapshotRepositoryGenerationStep storeRepoGenStep = new StoreSnapshotRepositoryGenerationStep(storeRepoGenKey,
            createSnapshotKey, snapshotRepository);
        CreateSnapshotStep createSnapshotStep = new CreateSnapshotStep(createSnapshotKey, waitForRepoGenChangeKey, client);
        WaitForRepositoryGenerationChangeStep waitForRepoGenChangeStep =
            new WaitForRepositoryGenerationChangeStep(waitForRepoGenChangeKey, verifySnapshotStatusBranchingKey);
        OnAsyncWaitBranchingStep onAsyncWaitBranchingStep = new OnAsyncWaitBranchingStep(verifySnapshotStatusBranchingKey,
            cleanSnapshotKey, mountSnapshotKey, client, getCheckSnapshotStatusAsyncAction());
        MountSnapshotStep mountSnapshotStep = new MountSnapshotStep(mountSnapshotKey, waitForGreenRestoredIndexKey,
            client, RESTORED_INDEX_PREFIX);
        WaitForIndexColorStep waitForGreenIndexHealthStep = new WaitForIndexColorStep(waitForGreenRestoredIndexKey,
            copyMetadataKey, ClusterHealthStatus.GREEN, RESTORED_INDEX_PREFIX);
        // a policy with only the cold phase will have a null "nextStepKey", hence the "null" nextStepKey passed in below when that's the
        // case
        CopyExecutionStateStep copyMetadataStep = new CopyExecutionStateStep(copyMetadataKey, copyLifecyclePolicySettingKey,
            RESTORED_INDEX_PREFIX, nextStepKey != null ? nextStepKey.getName() : "null");
        CopySettingsStep copySettingsStep = new CopySettingsStep(copyLifecyclePolicySettingKey, swapAliasesKey, RESTORED_INDEX_PREFIX,
            LifecycleSettings.LIFECYCLE_NAME);
        // sending this step to null as the restored index (which will after this step essentially be the source index) was sent to the next
        // key after we restored the lifecycle execution state
        SwapAliasesAndDeleteSourceIndexStep swapAliasesAndDeleteSourceIndexStep = new SwapAliasesAndDeleteSourceIndexStep(swapAliasesKey,
            null, client, RESTORED_INDEX_PREFIX);

        return Arrays.asList(waitForNoFollowersStep, generateSnapshotNameStep, cleanupSnapshotStep, storeRepoGenStep, createSnapshotStep,
            waitForRepoGenChangeStep, onAsyncWaitBranchingStep, mountSnapshotStep, waitForGreenIndexHealthStep, copyMetadataStep,
            copySettingsStep, swapAliasesAndDeleteSourceIndexStep);
    }

    /**
     * Creates a consumer to evaluate the ILM generated snapshot status in the provided snapshotRepository in an async way, akin to an
     * equivalent {@link AsyncWaitStep} implementation.
     */
    static TriConsumer<Client, IndexMetadata, BranchingStepListener> getCheckSnapshotStatusAsyncAction() {
        return (client, indexMetadata, branchingStepListener) -> {

            LifecycleExecutionState executionState = LifecycleExecutionState.fromIndexMetadata(indexMetadata);

            String snapshotName = executionState.getSnapshotName();
            String policyName = indexMetadata.getSettings().get(LifecycleSettings.LIFECYCLE_NAME);
            final String indexName = indexMetadata.getIndex().getName();
            final String repositoryName = executionState.getSnapshotRepository();
            if (Strings.hasText(repositoryName) == false) {
                branchingStepListener.onFailure(
                    new IllegalStateException("snapshot repository is not present for policy [" + policyName + "] and index [" + indexName +
                        "]"));
                return;
            }
            if (Strings.hasText(snapshotName) == false) {
                branchingStepListener.onFailure(new IllegalStateException("snapshot name was not generated for policy [" + policyName +
                    "] and index [" + indexName + "]"));
                return;
            }
            SnapshotsStatusRequest snapshotsStatusRequest = new SnapshotsStatusRequest(repositoryName, new String[]{snapshotName});
            client.admin().cluster().snapshotsStatus(snapshotsStatusRequest, new ActionListener<>() {
                @Override
                public void onResponse(SnapshotsStatusResponse snapshotsStatusResponse) {
                    List<SnapshotStatus> statuses = snapshotsStatusResponse.getSnapshots();
                    assert statuses.size() == 1 : "we only requested the status info for one snapshot";
                    SnapshotStatus snapshotStatus = statuses.get(0);
                    SnapshotsInProgress.State snapshotState = snapshotStatus.getState();
                    if (snapshotState.equals(SUCCESS)) {
                        branchingStepListener.onResponse(true, null);
                    } else if (snapshotState.equals(IN_PROGRESS)) {
                        branchingStepListener.onResponse(false, new Info(
                            "snapshot [" + snapshotName + "] for index [ " + indexName + "] as part of policy [" + policyName + "] is " +
                                "in state [" + snapshotState + "]. waiting for SUCCESS"));
                    } else {
                        branchingStepListener.onStopWaitingAndMoveToNextKey(new Info(
                            "snapshot [" + snapshotName + "] for index [ " + indexName + "] as part of policy [" + policyName + "] " +
                                "cannot complete as it is in state [" + snapshotState + "]"));
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    branchingStepListener.onFailure(e);
                }

                final class Info implements ToXContentObject {

                    final ParseField MESSAGE_FIELD = new ParseField("message");

                    private final String message;

                    Info(String message) {
                        this.message = message;
                    }

                    String getMessage() {
                        return message;
                    }

                    @Override
                    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
                        builder.startObject();
                        builder.field(MESSAGE_FIELD.getPreferredName(), message);
                        builder.endObject();
                        return builder;
                    }

                    @Override
                    public boolean equals(Object o) {
                        if (o == null) {
                            return false;
                        }
                        if (getClass() != o.getClass()) {
                            return false;
                        }
                        Info info = (Info) o;
                        return Objects.equals(getMessage(), info.getMessage());
                    }

                    @Override
                    public int hashCode() {
                        return Objects.hash(getMessage());
                    }
                }
            });
        };
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
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(SNAPSHOT_REPOSITORY.getPreferredName(), snapshotRepository);
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
