/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.snapshots.RestoreInfo;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.xpack.core.ilm.Step.StepKey;
import org.elasticsearch.xpack.core.searchablesnapshots.MountSearchableSnapshotAction;
import org.elasticsearch.xpack.core.searchablesnapshots.MountSearchableSnapshotRequest;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class MountSnapshotStepTests extends AbstractStepTestCase<MountSnapshotStep> {

    private static final String RESTORED_INDEX_PREFIX = "restored-";

    @Override
    public MountSnapshotStep createRandomInstance() {
        StepKey stepKey = randomStepKey();
        StepKey nextStepKey = randomStepKey();
        String restoredIndexPrefix = randomAlphaOfLength(10);
        MountSearchableSnapshotRequest.Storage storage = randomStorageType();
        return new MountSnapshotStep(stepKey, nextStepKey, client, restoredIndexPrefix, storage);
    }

    public static MountSearchableSnapshotRequest.Storage randomStorageType() {
        if (randomBoolean()) {
            return MountSearchableSnapshotRequest.Storage.FULL_COPY;
        } else {
            return MountSearchableSnapshotRequest.Storage.SHARED_CACHE;
        }
    }

    @Override
    protected MountSnapshotStep copyInstance(MountSnapshotStep instance) {
        return new MountSnapshotStep(instance.getKey(), instance.getNextStepKey(), instance.getClient(), instance.getRestoredIndexPrefix(),
            instance.getStorage());
    }

    @Override
    public MountSnapshotStep mutateInstance(MountSnapshotStep instance) {
        StepKey key = instance.getKey();
        StepKey nextKey = instance.getNextStepKey();
        String restoredIndexPrefix = instance.getRestoredIndexPrefix();
        MountSearchableSnapshotRequest.Storage storage = instance.getStorage();
        switch (between(0, 3)) {
            case 0:
                key = new StepKey(key.getPhase(), key.getAction(), key.getName() + randomAlphaOfLength(5));
                break;
            case 1:
                nextKey = new StepKey(key.getPhase(), key.getAction(), key.getName() + randomAlphaOfLength(5));
                break;
            case 2:
                restoredIndexPrefix = randomValueOtherThan(restoredIndexPrefix, () -> randomAlphaOfLengthBetween(1, 10));
                break;
            case 3:
                if (storage == MountSearchableSnapshotRequest.Storage.FULL_COPY) {
                    storage = MountSearchableSnapshotRequest.Storage.SHARED_CACHE;
                } else if (storage == MountSearchableSnapshotRequest.Storage.SHARED_CACHE) {
                    storage = MountSearchableSnapshotRequest.Storage.FULL_COPY;
                } else {
                    throw new AssertionError("unknown storage type: " + storage);
                }
                break;
            default:
                throw new AssertionError("Illegal randomisation branch");
        }
        return new MountSnapshotStep(key, nextKey, instance.getClient(), restoredIndexPrefix, storage);
    }

    public void testPerformActionFailure() {
        String indexName = randomAlphaOfLength(10);
        String policyName = "test-ilm-policy";

        {
            IndexMetadata.Builder indexMetadataBuilder =
                IndexMetadata.builder(indexName).settings(settings(Version.CURRENT).put(LifecycleSettings.LIFECYCLE_NAME, policyName))
                    .numberOfShards(randomIntBetween(1, 5)).numberOfReplicas(randomIntBetween(0, 5));
            IndexMetadata indexMetadata = indexMetadataBuilder.build();

            ClusterState clusterState =
                ClusterState.builder(emptyClusterState()).metadata(Metadata.builder().put(indexMetadata, true).build()).build();

            MountSnapshotStep mountSnapshotStep = createRandomInstance();
            Exception e = expectThrows(IllegalStateException.class, () -> PlainActionFuture.<Boolean, Exception>get(
                f -> mountSnapshotStep.performAction(indexMetadata, clusterState, null, f)));
            assertThat(e.getMessage(),
                is("snapshot repository is not present for policy [" + policyName + "] and index [" + indexName + "]"));
        }

        {
            IndexMetadata.Builder indexMetadataBuilder =
                IndexMetadata.builder(indexName).settings(settings(Version.CURRENT).put(LifecycleSettings.LIFECYCLE_NAME, policyName))
                    .numberOfShards(randomIntBetween(1, 5)).numberOfReplicas(randomIntBetween(0, 5));
            Map<String, String> ilmCustom = new HashMap<>();
            String repository = "repository";
            ilmCustom.put("snapshot_repository", repository);
            indexMetadataBuilder.putCustom(LifecycleExecutionState.ILM_CUSTOM_METADATA_KEY, ilmCustom);
            IndexMetadata indexMetadata = indexMetadataBuilder.build();

            ClusterState clusterState =
                ClusterState.builder(emptyClusterState()).metadata(Metadata.builder().put(indexMetadata, true).build()).build();

            MountSnapshotStep mountSnapshotStep = createRandomInstance();
            Exception e = expectThrows(IllegalStateException.class, () -> PlainActionFuture.<Boolean, Exception>get(
                f -> mountSnapshotStep.performAction(indexMetadata, clusterState, null, f)));
            assertThat(e.getMessage(),
                is("snapshot name was not generated for policy [" + policyName + "] and index [" + indexName + "]"));
        }
    }

    public void testPerformAction() {
        String indexName = randomAlphaOfLength(10);
        String policyName = "test-ilm-policy";
        Map<String, String> ilmCustom = new HashMap<>();
        String snapshotName = indexName + "-" + policyName;
        ilmCustom.put("snapshot_name", snapshotName);
        String repository = "repository";
        ilmCustom.put("snapshot_repository", repository);

        IndexMetadata.Builder indexMetadataBuilder =
            IndexMetadata.builder(indexName).settings(settings(Version.CURRENT).put(LifecycleSettings.LIFECYCLE_NAME, policyName))
                .putCustom(LifecycleExecutionState.ILM_CUSTOM_METADATA_KEY, ilmCustom)
                .numberOfShards(randomIntBetween(1, 5)).numberOfReplicas(randomIntBetween(0, 5));
        IndexMetadata indexMetadata = indexMetadataBuilder.build();

        ClusterState clusterState =
            ClusterState.builder(emptyClusterState()).metadata(Metadata.builder().put(indexMetadata, true).build()).build();

        try (NoOpClient client =
                 getRestoreSnapshotRequestAssertingClient(repository, snapshotName, indexName, RESTORED_INDEX_PREFIX, indexName)) {
            MountSnapshotStep step =
                new MountSnapshotStep(randomStepKey(), randomStepKey(), client, RESTORED_INDEX_PREFIX, randomStorageType());
            assertTrue(PlainActionFuture.get(f -> step.performAction(indexMetadata, clusterState, null, f)));
        }
    }

    public void testResponseStatusHandling() {
        String indexName = randomAlphaOfLength(10);
        String policyName = "test-ilm-policy";
        Map<String, String> ilmCustom = new HashMap<>();
        String snapshotName = indexName + "-" + policyName;
        ilmCustom.put("snapshot_name", snapshotName);
        String repository = "repository";
        ilmCustom.put("snapshot_repository", repository);

        IndexMetadata.Builder indexMetadataBuilder =
            IndexMetadata.builder(indexName).settings(settings(Version.CURRENT).put(LifecycleSettings.LIFECYCLE_NAME, policyName))
                .putCustom(LifecycleExecutionState.ILM_CUSTOM_METADATA_KEY, ilmCustom)
                .numberOfShards(randomIntBetween(1, 5)).numberOfReplicas(randomIntBetween(0, 5));
        IndexMetadata indexMetadata = indexMetadataBuilder.build();

        ClusterState clusterState =
            ClusterState.builder(emptyClusterState()).metadata(Metadata.builder().put(indexMetadata, true).build()).build();

        {
            RestoreSnapshotResponse responseWithOKStatus = new RestoreSnapshotResponse(new RestoreInfo("test", List.of(), 1, 1));
            try (NoOpClient clientPropagatingOKResponse = getClientTriggeringResponse(responseWithOKStatus)) {
                MountSnapshotStep step = new MountSnapshotStep(randomStepKey(), randomStepKey(), clientPropagatingOKResponse,
                    RESTORED_INDEX_PREFIX, randomStorageType());
                assertTrue(PlainActionFuture.get(f -> step.performAction(indexMetadata, clusterState, null, f)));
            }
        }

        {
            RestoreSnapshotResponse responseWithACCEPTEDStatus = new RestoreSnapshotResponse((RestoreInfo) null);
            try (NoOpClient clientPropagatingACCEPTEDResponse = getClientTriggeringResponse(responseWithACCEPTEDStatus)) {
                MountSnapshotStep step = new MountSnapshotStep(randomStepKey(), randomStepKey(), clientPropagatingACCEPTEDResponse,
                    RESTORED_INDEX_PREFIX, randomStorageType());
                assertTrue(PlainActionFuture.get(f -> step.performAction(indexMetadata, clusterState, null, f)));
            }
        }
    }

    public void testBestEffortNameResolution() {
        assertThat(MountSnapshotStep.bestEffortIndexNameResolution("potato"), equalTo("potato"));
        assertThat(MountSnapshotStep.bestEffortIndexNameResolution("restored-potato"), equalTo("potato"));
        assertThat(MountSnapshotStep.bestEffortIndexNameResolution("partial-potato"), equalTo("potato"));
        assertThat(MountSnapshotStep.bestEffortIndexNameResolution("partial-restored-potato"), equalTo("potato"));
        assertThat(MountSnapshotStep.bestEffortIndexNameResolution("restored-partial-potato"), equalTo("partial-potato"));
        assertThat(MountSnapshotStep.bestEffortIndexNameResolution("my-restored-potato"), equalTo("my-restored-potato"));
        assertThat(MountSnapshotStep.bestEffortIndexNameResolution("my-partial-potato"), equalTo("my-partial-potato"));
        assertThat(MountSnapshotStep.bestEffortIndexNameResolution("my-partial-restored-potato"), equalTo("my-partial-restored-potato"));
        assertThat(MountSnapshotStep.bestEffortIndexNameResolution("my-restored-partial-potato"), equalTo("my-restored-partial-potato"));
    }

    public void testMountWithNoPrefix() {
        doTestMountWithoutSnapshotIndexNameInState("");
    }

    public void testMountWithRestorePrefix() {
        doTestMountWithoutSnapshotIndexNameInState(SearchableSnapshotAction.FULL_RESTORED_INDEX_PREFIX);
    }

    public void testMountWithPartialPrefix() {
        doTestMountWithoutSnapshotIndexNameInState(SearchableSnapshotAction.PARTIAL_RESTORED_INDEX_PREFIX);
    }

    public void testMountWithPartialAndRestoredPrefix() {
        doTestMountWithoutSnapshotIndexNameInState(SearchableSnapshotAction.PARTIAL_RESTORED_INDEX_PREFIX +
            SearchableSnapshotAction.FULL_RESTORED_INDEX_PREFIX);
    }

    public void doTestMountWithoutSnapshotIndexNameInState(String prefix) {
        {
            String indexNameSnippet = randomAlphaOfLength(10);
            String indexName = prefix + indexNameSnippet;
            String policyName = "test-ilm-policy";
            Map<String, String> ilmCustom = new HashMap<>();
            String snapshotName = indexName + "-" + policyName;
            ilmCustom.put("snapshot_name", snapshotName);
            String repository = "repository";
            ilmCustom.put("snapshot_repository", repository);

            IndexMetadata.Builder indexMetadataBuilder =
                IndexMetadata.builder(indexName).settings(settings(Version.CURRENT).put(LifecycleSettings.LIFECYCLE_NAME, policyName))
                    .putCustom(LifecycleExecutionState.ILM_CUSTOM_METADATA_KEY, ilmCustom)
                    .numberOfShards(randomIntBetween(1, 5)).numberOfReplicas(randomIntBetween(0, 5));
            IndexMetadata indexMetadata = indexMetadataBuilder.build();

            ClusterState clusterState =
                ClusterState.builder(emptyClusterState()).metadata(Metadata.builder().put(indexMetadata, true).build()).build();

            try (NoOpClient client =
                     getRestoreSnapshotRequestAssertingClient(repository, snapshotName,
                         indexName, RESTORED_INDEX_PREFIX, indexNameSnippet)) {
                MountSnapshotStep step =
                    new MountSnapshotStep(randomStepKey(), randomStepKey(), client, RESTORED_INDEX_PREFIX, randomStorageType());
                assertTrue(PlainActionFuture.get(f -> step.performAction(indexMetadata, clusterState, null, f)));
            }
        }
    }

    @SuppressWarnings("unchecked")
    private NoOpClient getClientTriggeringResponse(RestoreSnapshotResponse response) {
        return new NoOpClient(getTestName()) {
            @Override
            protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(ActionType<Response> action,
                                                                                                      Request request,
                                                                                                      ActionListener<Response> listener) {
                listener.onResponse((Response) response);
            }
        };
    }

    @SuppressWarnings("unchecked")
    private NoOpClient getRestoreSnapshotRequestAssertingClient(String expectedRepoName, String expectedSnapshotName, String indexName,
                                                                String restoredIndexPrefix, String expectedSnapshotIndexName) {
        return new NoOpClient(getTestName()) {
            @Override
            protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(ActionType<Response> action,
                                                                                                      Request request,
                                                                                                      ActionListener<Response> listener) {
                assertThat(action.name(), is(MountSearchableSnapshotAction.NAME));
                assertTrue(request instanceof MountSearchableSnapshotRequest);
                MountSearchableSnapshotRequest mountSearchableSnapshotRequest = (MountSearchableSnapshotRequest) request;
                assertThat(mountSearchableSnapshotRequest.repositoryName(), is(expectedRepoName));
                assertThat(mountSearchableSnapshotRequest.snapshotName(), is(expectedSnapshotName));
                assertThat("another ILM step will wait for the restore to complete. the " + MountSnapshotStep.NAME + " step should not",
                    mountSearchableSnapshotRequest.waitForCompletion(), is(false));
                assertThat(mountSearchableSnapshotRequest.ignoreIndexSettings(), is(notNullValue()));
                assertThat(mountSearchableSnapshotRequest.ignoreIndexSettings()[0], is(LifecycleSettings.LIFECYCLE_NAME));
                assertThat(mountSearchableSnapshotRequest.mountedIndexName(), is(restoredIndexPrefix + indexName));
                assertThat(mountSearchableSnapshotRequest.snapshotIndexName(), is(expectedSnapshotIndexName));

                // invoke the awaiting listener with a very generic 'response', just to fulfill the contract
                listener.onResponse((Response) new RestoreSnapshotResponse((RestoreInfo) null));
            }
        };
    }
}
