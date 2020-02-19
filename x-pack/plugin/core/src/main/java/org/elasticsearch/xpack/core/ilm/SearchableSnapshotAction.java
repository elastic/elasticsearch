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
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.TriConsumer;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.snapshots.SnapshotException;
import org.elasticsearch.xpack.core.ilm.Step.StepKey;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.cluster.SnapshotsInProgress.State.ABORTED;
import static org.elasticsearch.cluster.SnapshotsInProgress.State.FAILED;
import static org.elasticsearch.cluster.SnapshotsInProgress.State.MISSING;
import static org.elasticsearch.cluster.SnapshotsInProgress.State.SUCCESS;

/**
 * A {@link LifecycleAction} that will convert the index into a searchable snapshot, by taking a snapshot of the index, creating a
 * searchable snapshot and the corresponding "searchable snapshot index", deleting the original index and swapping its aliases to the
 * newly created searchable snapshot backed index.
 */
public class SearchableSnapshotAction implements LifecycleAction {
    public static final String NAME = "searchable_snapshot";

    public static final ParseField SNAPSHOT_REPOSITORY = new ParseField("snapshot_repository");
    public static final ParseField SEARCHABLE_REPOSITORY = new ParseField("searchable_repository");

    public static final String RESTORED_INDEX_PREFIX = "ilm-restored-";

    private static final ConstructingObjectParser<SearchableSnapshotAction, Void> PARSER = new ConstructingObjectParser<>(NAME,
        a -> new SearchableSnapshotAction((String) a[0], (String) a[1]));

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), SNAPSHOT_REPOSITORY);
        PARSER.declareString(ConstructingObjectParser.constructorArg(), SEARCHABLE_REPOSITORY);
    }

    public static SearchableSnapshotAction parse(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    private final String snapshotRepository;
    private final String searchableRepository;

    public SearchableSnapshotAction(String snapshotRepository, String searchableRepository) {
        if (Strings.hasText(snapshotRepository) == false) {
            throw new IllegalArgumentException("the snapshot repository must be specified");
        }
        if (Strings.hasText(searchableRepository) == false) {
            throw new IllegalArgumentException("the searchable repository must be specified");
        }
        this.snapshotRepository = snapshotRepository;
        this.searchableRepository = searchableRepository;
    }

    public SearchableSnapshotAction(StreamInput in) throws IOException {
        this(in.readString(), in.readString());
    }

    @Override
    public List<Step> toSteps(Client client, String phase, StepKey nextStepKey) {
        StepKey waitForNoFollowerStepKey = new StepKey(phase, NAME, WaitForNoFollowersStep.NAME);
        StepKey generateSnapshotNameKey = new StepKey(phase, NAME, GenerateSnapshotNameStep.NAME);
        StepKey cleanSnapshotKey = new StepKey(phase, NAME, CleanupSnapshotStep.NAME);
        StepKey takeSnapshotKey = new StepKey(phase, NAME, TakeSnapshotStep.NAME);
        StepKey waitForSnapshotInProgressKey = new StepKey(phase, NAME, WaitForSnapshotInProgressStep.NAME);
        StepKey verifySnapshotStatusBranchingKey = new StepKey(phase, NAME, OnAsyncWaitBranchingStep.NAME);
        StepKey restoreFromSearchableRepoKey = new StepKey(phase, NAME, RestoreSnapshotStep.NAME);
        StepKey waitForGreenRestoredIndexKey = new StepKey(phase, NAME, WaitForGreenIndexHealthStep.NAME);
        StepKey copyMetadataKey = new StepKey(phase, NAME, CopyExecutionStateStep.NAME);
        StepKey copyLifecyclePolicySettingKey = new StepKey(phase, NAME, CopySettingsStep.NAME);
        StepKey swapAliasesKey = new StepKey(phase, NAME, SwapAliasesAndDeleteSourceIndexStep.NAME);

        WaitForNoFollowersStep waitForNoFollowersStep = new WaitForNoFollowersStep(waitForNoFollowerStepKey, generateSnapshotNameKey,
            client);
        GenerateSnapshotNameStep generateSnapshotNameStep = new GenerateSnapshotNameStep(generateSnapshotNameKey, cleanSnapshotKey);
        CleanupSnapshotStep cleanupSnapshotStep = new CleanupSnapshotStep(cleanSnapshotKey, takeSnapshotKey, client, snapshotRepository);
        TakeSnapshotStep takeSnapshotStep = new TakeSnapshotStep(takeSnapshotKey, waitForSnapshotInProgressKey, client, snapshotRepository);
        WaitForSnapshotInProgressStep waitForSnapshotInProgressStep = new WaitForSnapshotInProgressStep(waitForSnapshotInProgressKey,
            verifySnapshotStatusBranchingKey, snapshotRepository);
        OnAsyncWaitBranchingStep onAsyncWaitBranchingStep = new OnAsyncWaitBranchingStep(verifySnapshotStatusBranchingKey,
            cleanSnapshotKey, restoreFromSearchableRepoKey, client, getCheckSnapshotStatusAsyncAction(snapshotRepository));
        RestoreSnapshotStep restoreSnapshotStep = new RestoreSnapshotStep(restoreFromSearchableRepoKey, waitForGreenRestoredIndexKey,
            client, searchableRepository, RESTORED_INDEX_PREFIX);
        WaitForGreenIndexHealthStep waitForGreenIndexHealthStep = new WaitForGreenIndexHealthStep(waitForGreenRestoredIndexKey,
            copyMetadataKey, RESTORED_INDEX_PREFIX);
        CopyExecutionStateStep copyMetadataStep = new CopyExecutionStateStep(copyMetadataKey, copyLifecyclePolicySettingKey,
            RESTORED_INDEX_PREFIX, nextStepKey.getName());
        CopySettingsStep copySettingsStep = new CopySettingsStep(copyLifecyclePolicySettingKey, swapAliasesKey, client,
            RESTORED_INDEX_PREFIX, LifecycleSettings.LIFECYCLE_NAME);
        // sending this step to null as the restored index (which will after this step essentially be the source index) was sent to the next
        // key after we restored the lifecycle execution state
        SwapAliasesAndDeleteSourceIndexStep swapAliasesAndDeleteSourceIndexStep = new SwapAliasesAndDeleteSourceIndexStep(swapAliasesKey,
            null, client, RESTORED_INDEX_PREFIX);

        return Arrays.asList(waitForNoFollowersStep, generateSnapshotNameStep, cleanupSnapshotStep, takeSnapshotStep,
            waitForSnapshotInProgressStep, onAsyncWaitBranchingStep, restoreSnapshotStep, waitForGreenIndexHealthStep,
            copyMetadataStep, copySettingsStep, swapAliasesAndDeleteSourceIndexStep);
    }

    private TriConsumer<Client, IndexMetaData, AsyncWaitStep.Listener> getCheckSnapshotStatusAsyncAction(String snapshotRepository) {
        return (client, indexMetaData, listener) -> {

            LifecycleExecutionState executionState = LifecycleExecutionState.fromIndexMetadata(indexMetaData);

            String snapshotName = executionState.getSnapshotName();
            String policyName = indexMetaData.getSettings().get(LifecycleSettings.LIFECYCLE_NAME);
            final String indexName = indexMetaData.getIndex().getName();
            if (Strings.hasText(snapshotName) == false) {
                listener.onFailure(new IllegalStateException("snapshot name was not generated for policy [" + policyName + "] and index " +
                    "[" + indexName + "]"));
                return;
            }
            SnapshotsStatusRequest snapshotsStatusRequest = new SnapshotsStatusRequest(snapshotRepository, new String[]{snapshotName});
            client.admin().cluster().snapshotsStatus(snapshotsStatusRequest, new ActionListener<>() {
                @Override
                public void onResponse(SnapshotsStatusResponse snapshotsStatusResponse) {
                    List<SnapshotStatus> statuses = snapshotsStatusResponse.getSnapshots();
                    assert statuses.size() == 1 : "we only requested the status info for one snapshot";
                    SnapshotStatus snapshotStatus = statuses.get(0);
                    SnapshotsInProgress.State snapshotState = snapshotStatus.getState();
                    if (snapshotState.equals(SUCCESS)) {
                        listener.onResponse(true, null);
                    } else if (snapshotState.equals(ABORTED) || snapshotState.equals(FAILED) || snapshotState.equals(MISSING)) {
                        listener.onFailure(new SnapshotException(snapshotStatus.getSnapshot(),
                            "unable to create snapshot [" + snapshotName + "] for index [ " + indexName + "] as part of policy [" +
                                policyName + "] execution"));
                    } else {
                        // TODO add info context with status
                        listener.onResponse(false, null);
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    listener.onFailure(e);
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
        out.writeString(searchableRepository);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(SNAPSHOT_REPOSITORY.getPreferredName(), snapshotRepository);
        builder.field(SEARCHABLE_REPOSITORY.getPreferredName(), searchableRepository);
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
        return Objects.equals(snapshotRepository, that.snapshotRepository) &&
            Objects.equals(searchableRepository, that.searchableRepository);
    }

    @Override
    public int hashCode() {
        return Objects.hash(snapshotRepository, searchableRepository);
    }
}
