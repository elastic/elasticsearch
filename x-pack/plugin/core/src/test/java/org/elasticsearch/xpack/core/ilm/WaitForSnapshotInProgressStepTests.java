/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.snapshots.Snapshot;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.test.VersionUtils;
import org.elasticsearch.xpack.core.ilm.ClusterStateWaitStep.Result;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.elasticsearch.xpack.core.ilm.AbstractStepMasterTimeoutTestCase.emptyClusterState;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.containsStringIgnoringCase;

public class WaitForSnapshotInProgressStepTests extends AbstractStepTestCase<WaitForSnapshotInProgressStep> {

    @Override
    protected WaitForSnapshotInProgressStep createRandomInstance() {
        return new WaitForSnapshotInProgressStep(randomStepKey(), randomStepKey(), randomAlphaOfLengthBetween(1, 10));
    }

    @Override
    protected WaitForSnapshotInProgressStep mutateInstance(WaitForSnapshotInProgressStep instance) {
        Step.StepKey key = instance.getKey();
        Step.StepKey nextKey = instance.getNextStepKey();
        String snapshotRepository = instance.getSnapshotRepository();

        switch (between(0, 2)) {
            case 0:
                key = new Step.StepKey(key.getPhase(), key.getAction(), key.getName() + randomAlphaOfLength(5));
                break;
            case 1:
                nextKey = new Step.StepKey(key.getPhase(), key.getAction(), key.getName() + randomAlphaOfLength(5));
                break;
            case 2:
                snapshotRepository = randomValueOtherThan(snapshotRepository, () -> randomAlphaOfLengthBetween(1, 10));
                break;
            default:
                throw new AssertionError("Illegal randomisation branch");
        }

        return new WaitForSnapshotInProgressStep(key, nextKey, snapshotRepository);
    }

    @Override
    protected WaitForSnapshotInProgressStep copyInstance(WaitForSnapshotInProgressStep instance) {
        return new WaitForSnapshotInProgressStep(instance.getKey(), instance.getNextStepKey(), instance.getSnapshotRepository());
    }

    public void testNoSnapshotNameAvailable() {
        String indexName = randomAlphaOfLength(10);
        String policyName = "test-ilm-policy";
        IndexMetaData.Builder indexMetadataBuilder =
            IndexMetaData.builder(indexName).settings(settings(Version.CURRENT).put(LifecycleSettings.LIFECYCLE_NAME, policyName))
                .numberOfShards(randomIntBetween(1, 5)).numberOfReplicas(randomIntBetween(0, 5));
        IndexMetaData indexMetaData = indexMetadataBuilder.build();

        ClusterState clusterState =
            ClusterState.builder(emptyClusterState()).metaData(MetaData.builder().put(indexMetaData, true).build()).build();

        WaitForSnapshotInProgressStep waitForSnapshotInProgressStep = createRandomInstance();
        Result result = waitForSnapshotInProgressStep.isConditionMet(indexMetaData.getIndex(), clusterState);
        assertThat(getMessage(result), containsStringIgnoringCase("snapshot name was not generated for policy [" + policyName + "] and " +
            "index [" + indexName + "]"));
    }

    public void testSnapshotInProgress() {
        String indexName = randomAlphaOfLength(10);
        String policyName = "test-ilm-policy";
        Map<String, String> ilmCustom = new HashMap<>();
        String snapshotName = indexName + "-" + policyName;
        ilmCustom.put("snapshot_name", snapshotName);

        IndexMetaData.Builder indexMetadataBuilder =
            IndexMetaData.builder(indexName).settings(settings(Version.CURRENT).put(LifecycleSettings.LIFECYCLE_NAME, policyName))
                .putCustom(LifecycleExecutionState.ILM_CUSTOM_METADATA_KEY, ilmCustom)
                .numberOfShards(randomIntBetween(1, 5)).numberOfReplicas(randomIntBetween(0, 5));
        IndexMetaData indexMetaData = indexMetadataBuilder.build();

        WaitForSnapshotInProgressStep waitForSnapshotInProgressStep = createRandomInstance();
        Snapshot snapshot = new Snapshot(waitForSnapshotInProgressStep.getSnapshotRepository(), new SnapshotId(snapshotName,
            UUID.randomUUID().toString()));
        SnapshotsInProgress inProgress = new SnapshotsInProgress(
            new SnapshotsInProgress.Entry(
                snapshot, false, false, SnapshotsInProgress.State.INIT,
                Collections.singletonList(new IndexId(indexName, "id")), 0, 0,
                ImmutableOpenMap.<ShardId, SnapshotsInProgress.ShardSnapshotStatus>builder().build(), Collections.emptyMap(),
                VersionUtils.randomVersion(random())));

        ClusterState clusterState =
            ClusterState.builder(emptyClusterState()).metaData(MetaData.builder().put(indexMetaData, true).build())
                .putCustom(SnapshotsInProgress.TYPE, inProgress)
                .build();

        Result result = waitForSnapshotInProgressStep.isConditionMet(indexMetaData.getIndex(), clusterState);
        assertThat(result.isComplete(), is(false));
        assertThat(getMessage(result), containsStringIgnoringCase("snapshot [" + snapshotName + "] generated by policy [" + policyName +
            "] for index [" + indexName + "] is still in progress"));
    }

    public void testNoSnapshotInProgress() {
        String indexName = randomAlphaOfLength(10);
        String policyName = "test-ilm-policy";
        Map<String, String> ilmCustom = new HashMap<>();
        String snapshotName = indexName + "-" + policyName;
        ilmCustom.put("snapshot_name", snapshotName);

        IndexMetaData.Builder indexMetadataBuilder =
            IndexMetaData.builder(indexName).settings(settings(Version.CURRENT).put(LifecycleSettings.LIFECYCLE_NAME, policyName))
                .putCustom(LifecycleExecutionState.ILM_CUSTOM_METADATA_KEY, ilmCustom)
                .numberOfShards(randomIntBetween(1, 5)).numberOfReplicas(randomIntBetween(0, 5));
        IndexMetaData indexMetaData = indexMetadataBuilder.build();

        ClusterState clusterState =
            ClusterState.builder(emptyClusterState()).metaData(MetaData.builder().put(indexMetaData, true).build())
                .build();

        WaitForSnapshotInProgressStep waitForSnapshotInProgressStep = createRandomInstance();
        Result result = waitForSnapshotInProgressStep.isConditionMet(indexMetaData.getIndex(), clusterState);
        assertThat(result.isComplete(), is(true));
    }

    private String getMessage(Result result) {
        return Strings.toString(result.getInfomationContext());
    }
}
