/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ilm.actions;

import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.elasticsearch.action.admin.indices.rollover.RolloverConditions;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.WarningFailureException;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.cluster.routing.allocation.DataTier;
import org.elasticsearch.cluster.routing.allocation.decider.EnableAllocationDecider;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.rest.ObjectPath;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.IlmESRestTestCase;
import org.elasticsearch.xpack.TimeSeriesRestDriver;
import org.elasticsearch.xpack.core.ilm.AllocateAction;
import org.elasticsearch.xpack.core.ilm.DeleteAction;
import org.elasticsearch.xpack.core.ilm.ForceMergeAction;
import org.elasticsearch.xpack.core.ilm.FreezeAction;
import org.elasticsearch.xpack.core.ilm.LifecycleAction;
import org.elasticsearch.xpack.core.ilm.LifecyclePolicy;
import org.elasticsearch.xpack.core.ilm.LifecycleSettings;
import org.elasticsearch.xpack.core.ilm.Phase;
import org.elasticsearch.xpack.core.ilm.PhaseCompleteStep;
import org.elasticsearch.xpack.core.ilm.RolloverAction;
import org.elasticsearch.xpack.core.ilm.SearchableSnapshotAction;
import org.elasticsearch.xpack.core.ilm.SetPriorityAction;
import org.elasticsearch.xpack.core.ilm.ShrinkAction;
import org.elasticsearch.xpack.core.ilm.Step;
import org.elasticsearch.xpack.core.ilm.WaitUntilReplicateForTimePassesStep;
import org.junit.Before;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.cluster.metadata.IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING;
import static org.elasticsearch.cluster.routing.allocation.decider.ShardsLimitAllocationDecider.INDEX_TOTAL_SHARDS_PER_NODE_SETTING;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.xpack.TimeSeriesRestDriver.createComposableTemplate;
import static org.elasticsearch.xpack.TimeSeriesRestDriver.createNewSingletonPolicy;
import static org.elasticsearch.xpack.TimeSeriesRestDriver.createPolicy;
import static org.elasticsearch.xpack.TimeSeriesRestDriver.createSnapshotRepo;
import static org.elasticsearch.xpack.TimeSeriesRestDriver.explainIndex;
import static org.elasticsearch.xpack.TimeSeriesRestDriver.getNumberOfPrimarySegments;
import static org.elasticsearch.xpack.TimeSeriesRestDriver.getStepKeyForIndex;
import static org.elasticsearch.xpack.TimeSeriesRestDriver.indexDocument;
import static org.elasticsearch.xpack.TimeSeriesRestDriver.rolloverMaxOneDocCondition;
import static org.elasticsearch.xpack.core.ilm.DeleteAction.WITH_SNAPSHOT_DELETE;
import static org.elasticsearch.xpack.core.ilm.SearchableSnapshotAction.FORCE_MERGE_CLONE_INDEX_PREFIX;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.startsWith;

public class SearchableSnapshotActionIT extends IlmESRestTestCase {

    private String policy;
    private String dataStream;
    private String snapshotRepo;

    @Before
    public void refreshIndex() {
        dataStream = "logs-" + randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        policy = "policy-" + randomAlphaOfLength(5);
        snapshotRepo = randomAlphaOfLengthBetween(10, 20);
        logger.info(
            "--> running [{}] with data stream [{}], snapshot repo [{}] and policy [{}]",
            getTestName(),
            dataStream,
            snapshotRepo,
            policy
        );
    }

    public void testSearchableSnapshotAction() throws Exception {
        createSnapshotRepo(client(), snapshotRepo, randomBoolean());
        createNewSingletonPolicy(client(), policy, "cold", new SearchableSnapshotAction(snapshotRepo, true));

        createComposableTemplate(
            client(),
            randomAlphaOfLengthBetween(5, 10).toLowerCase(Locale.ROOT),
            dataStream,
            new Template(Settings.builder().put(LifecycleSettings.LIFECYCLE_NAME, policy).build(), null, null)
        );

        indexDocument(client(), dataStream, true);

        // rolling over the data stream so we can apply the searchable snapshot policy to a backing index that's not the write index
        rolloverMaxOneDocCondition(client(), dataStream);

        List<String> backingIndices = getDataStreamBackingIndexNames(dataStream);
        assertThat(backingIndices.size(), equalTo(2));
        String backingIndexName = backingIndices.getFirst();
        String restoredIndexName = SearchableSnapshotAction.FULL_RESTORED_INDEX_PREFIX + backingIndexName;
        awaitIndexExists(restoredIndexName, TimeValue.timeValueSeconds(20));

        TimeSeriesRestDriver.awaitStepKey(client(), restoredIndexName, null, null, PhaseCompleteStep.NAME);
        // Wait for the original index to be deleted, to ensure ILM has finished
        awaitIndexDoesNotExist(backingIndexName);
    }

    /**
     * Test that when we have a searchable snapshot action with force merge enabled and the source index has _at least one_ replica,
     * we perform the force merge on _the cloned index_ with 0 replicas and then snapshot the clone.
     * We also sometimes artificially "pause" ILM on the clone step to allow us to assert that the cloned index has the correct settings.
     * We do this by disabling allocation in the whole cluster, which means the clone step cannot complete as none of its shards can
     * allocate. After re-enabling allocation, we check that the remainder of the action completes as expected.
     */
    public void testSearchableSnapshotForceMergesClonedIndex() throws Exception {
        final int numberOfPrimaries = randomIntBetween(1, 3);
        // The test suite runs with 4 nodes, so we can have up to 3 (allocated) replicas.
        final int numberOfReplicas = randomIntBetween(1, 3);
        final String phase = randomBoolean() ? "cold" : "frozen";
        final String backingIndexName = prepareDataStreamWithDocs(phase, numberOfPrimaries, numberOfReplicas);

        final boolean pauseOnClone = randomBoolean();
        if (pauseOnClone) {
            logger.info("--> pausing test on index clone step");
            configureClusterAllocation(false);
        }
        try {
            // Enable/start ILM on the data stream.
            updateIndexSettings(dataStream, Settings.builder().put(LifecycleSettings.LIFECYCLE_NAME, policy));

            if (pauseOnClone) {
                assertForceMergeCloneIndexSettings(backingIndexName, numberOfPrimaries);
                configureClusterAllocation(true);
            }

            assertForceMergedSnapshotDone(phase, backingIndexName, numberOfPrimaries, true);
        } catch (Exception | AssertionError e) {
            // Make sure we re-enable allocation in case of failure so that the remaining tests in the suite are not affected.
            configureClusterAllocation(true);
            throw e;
        }
    }

    /**
     * Test that when we have a searchable snapshot action with force merge enabled and the source index has _zero_ replicas,
     * we perform the force merge on the _source_ index and snapshot the source index.
     */
    public void testSearchableSnapshotForceMergesSourceIndex() throws Exception {
        final String phase = randomBoolean() ? "cold" : "frozen";
        final int numberOfPrimaries = randomIntBetween(1, 3);
        final String backingIndexName = prepareDataStreamWithDocs(phase, numberOfPrimaries, 0);

        // Enable/start ILM on the data stream.
        updateIndexSettings(dataStream, Settings.builder().put(LifecycleSettings.LIFECYCLE_NAME, policy));

        assertForceMergedSnapshotDone(phase, backingIndexName, numberOfPrimaries, false);
    }

    /**
     * Test that when we have a searchable snapshot action with force merge enabled, the source index has _at least one_ replica,
     * and we opt out of performing the force-merge on a zero-replica clone (through {@link SearchableSnapshotAction#forceMergeOnClone}),
     * we perform the force merge on the _source_ index and snapshot the source index.
     */
    public void testSearchableSnapshotForceMergeOnCloneOptOut() throws Exception {
        final String phase = randomBoolean() ? "cold" : "frozen";
        final int numberOfPrimaries = randomIntBetween(1, 3);
        // The test suite runs with 4 nodes, so we can have up to 3 (allocated) replicas.
        final int numberOfReplicas = randomIntBetween(1, 3);
        final String backingIndexName = prepareDataStreamWithDocs(phase, numberOfPrimaries, numberOfReplicas);
        // Update the policy to set `forceMergeOnClone` false in the SearchableSnapshotAction.
        createNewSingletonPolicy(client(), policy, phase, new SearchableSnapshotAction(snapshotRepo, true, null, null, false));

        // Enable/start ILM on the data stream.
        updateIndexSettings(dataStream, Settings.builder().put(LifecycleSettings.LIFECYCLE_NAME, policy));

        assertForceMergedSnapshotDone(phase, backingIndexName, numberOfPrimaries, false);
    }

    /**
     * Test that when we have a searchable snapshot action with force merge enabled and the source index has _at least one_ replica,
     * we perform the force merge on _the cloned index_ with 0 replicas and then snapshot the clone.
     * This test simulates a failure during the clone step by pausing ILM on the clone step and then moving the index back to the cleanup
     * step. We need to resort to simulation, as triggering the threshold in the ClusterStateWaitUntilThresholdStep is not feasible in a
     * Java REST test. After re-enabling allocation, we check that the remainder of the action completes as expected.
     */
    public void testSearchableSnapshotForceMergesClonedIndexAfterRetry() throws Exception {
        final int numberOfPrimaries = randomIntBetween(1, 3);
        final int numberOfReplicas = randomIntBetween(1, 3);
        final String phase = randomBoolean() ? "cold" : "frozen";
        final String backingIndexName = prepareDataStreamWithDocs(phase, numberOfPrimaries, numberOfReplicas);

        configureClusterAllocation(false);
        try {
            // Enable/start ILM on the data stream.
            updateIndexSettings(dataStream, Settings.builder().put(LifecycleSettings.LIFECYCLE_NAME, policy));

            assertForceMergeCloneIndexSettings(backingIndexName, numberOfPrimaries);

            TimeSeriesRestDriver.moveIndexToStep(
                client(),
                backingIndexName,
                new Step.StepKey(phase, "searchable_snapshot", "clone"),
                new Step.StepKey(phase, "searchable_snapshot", "cleanup-generated-index")
            );

            configureClusterAllocation(true);

            assertForceMergedSnapshotDone(phase, backingIndexName, numberOfPrimaries, true);
        } catch (Exception | AssertionError e) {
            // Make sure we re-enable allocation in case of failure so that the remaining tests in the suite are not affected.
            configureClusterAllocation(true);
            throw e;
        }
    }

    public void testDeleteActionDeletesSearchableSnapshot() throws Exception {
        createSnapshotRepo(client(), snapshotRepo, randomBoolean());

        // create policy with cold and delete phases
        Map<String, LifecycleAction> coldActions = Map.of(SearchableSnapshotAction.NAME, new SearchableSnapshotAction(snapshotRepo));
        Map<String, Phase> phases = new HashMap<>();
        phases.put("cold", new Phase("cold", TimeValue.ZERO, coldActions));
        phases.put("delete", new Phase("delete", TimeValue.ZERO, Map.of(DeleteAction.NAME, WITH_SNAPSHOT_DELETE)));
        LifecyclePolicy lifecyclePolicy = new LifecyclePolicy(policy, phases);
        // PUT policy
        XContentBuilder builder = jsonBuilder();
        lifecyclePolicy.toXContent(builder, null);
        final StringEntity entity = new StringEntity("{ \"policy\":" + Strings.toString(builder) + "}", ContentType.APPLICATION_JSON);
        Request createPolicyRequest = new Request("PUT", "_ilm/policy/" + policy);
        createPolicyRequest.setEntity(entity);
        assertOK(client().performRequest(createPolicyRequest));

        createComposableTemplate(
            client(),
            randomAlphaOfLengthBetween(5, 10).toLowerCase(Locale.ROOT),
            dataStream,
            new Template(Settings.builder().put(LifecycleSettings.LIFECYCLE_NAME, policy).build(), null, null)
        );

        indexDocument(client(), dataStream, true);

        // rolling over the data stream so we can apply the searchable snapshot policy to a backing index that's not the write index
        rolloverMaxOneDocCondition(client(), dataStream);

        List<String> backingIndices = getDataStreamBackingIndexNames(dataStream);
        assertThat(backingIndices.size(), equalTo(2));
        String backingIndexName = backingIndices.getFirst();
        String restoredIndexName = SearchableSnapshotAction.FULL_RESTORED_INDEX_PREFIX + backingIndexName;

        // let's wait for ILM to finish
        awaitIndexDoesNotExist(backingIndexName, TimeValue.timeValueSeconds(20));
        awaitIndexDoesNotExist(restoredIndexName);

        List<Map<String, Object>> snapshots = getSnapshots();
        assertThat(
            "the snapshot we generate in the cold phase should be deleted by the delete phase, but got snapshot: " + snapshots,
            snapshots.size(),
            equalTo(0)
        );
    }

    public void testCreateInvalidPolicy() {
        ResponseException exception = expectThrows(
            ResponseException.class,
            () -> createPolicy(
                client(),
                policy,
                new Phase(
                    "hot",
                    TimeValue.ZERO,
                    Map.of(
                        RolloverAction.NAME,
                        new RolloverAction(null, null, null, 1L, null, null, null, null, null, null),
                        SearchableSnapshotAction.NAME,
                        new SearchableSnapshotAction(randomAlphaOfLengthBetween(4, 10))
                    )
                ),
                new Phase("warm", TimeValue.ZERO, Map.of(ForceMergeAction.NAME, new ForceMergeAction(1, null))),
                new Phase("cold", TimeValue.ZERO, Map.of(FreezeAction.NAME, FreezeAction.INSTANCE)),
                null,
                null
            )
        );

        assertThat(
            exception.getMessage(),
            containsString(
                "phases [warm,cold] define one or more of [forcemerge, freeze, shrink, downsample]"
                    + " actions which are not allowed after a managed index is mounted as a searchable snapshot"
            )
        );
    }

    public void testUpdatePolicyToAddPhasesYieldsInvalidActionsToBeSkipped() throws Exception {
        createSnapshotRepo(client(), snapshotRepo, randomBoolean());
        createPolicy(
            client(),
            policy,
            new Phase(
                "hot",
                TimeValue.ZERO,
                Map.of(
                    RolloverAction.NAME,
                    // We create the policy with maxDocs 2 since we're required to have a rollover action if we're creating a searchable
                    // snapshot in the hot phase. But we will only index one document and trigger the rollover manually,
                    // to improve reliability and speed of the test.
                    new RolloverAction(RolloverConditions.newBuilder().addMaxIndexDocsCondition(2L).build()),
                    SearchableSnapshotAction.NAME,
                    new SearchableSnapshotAction(snapshotRepo)
                )
            ),
            new Phase("warm", TimeValue.timeValueDays(30), Map.of(SetPriorityAction.NAME, new SetPriorityAction(999))),
            null,
            null,
            null
        );

        createComposableTemplate(
            client(),
            randomAlphaOfLengthBetween(5, 10).toLowerCase(Locale.ROOT),
            dataStream,
            new Template(
                Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 5).put(LifecycleSettings.LIFECYCLE_NAME, policy).build(),
                null,
                null
            )
        );

        // rolling over the data stream so we can apply the searchable snapshot policy to a backing index that's not the write index
        indexDocument(client(), dataStream, true);
        rolloverMaxOneDocCondition(client(), dataStream);
        List<String> backingIndices = getDataStreamBackingIndexNames(dataStream);
        assertThat(backingIndices.size(), equalTo(2));

        String backingIndexName = backingIndices.getFirst();
        String restoredIndexName = SearchableSnapshotAction.FULL_RESTORED_INDEX_PREFIX + backingIndexName;
        awaitIndexExists(restoredIndexName, TimeValue.timeValueSeconds(20));
        TimeSeriesRestDriver.awaitStepKey(client(), restoredIndexName, "hot", null, PhaseCompleteStep.NAME);
        // Wait for the original index to be deleted, to ensure ILM has finished
        awaitIndexDoesNotExist(backingIndexName);

        createPolicy(
            client(),
            policy,
            new Phase("hot", TimeValue.ZERO, Map.of(SetPriorityAction.NAME, new SetPriorityAction(10))),
            new Phase(
                "warm",
                TimeValue.ZERO,
                Map.of(ShrinkAction.NAME, new ShrinkAction(1, null, false), ForceMergeAction.NAME, new ForceMergeAction(1, null))
            ),
            new Phase("cold", TimeValue.ZERO, Map.of(SearchableSnapshotAction.NAME, new SearchableSnapshotAction(snapshotRepo))),
            null,
            null
        );

        // even though the index is now mounted as a searchable snapshot, the actions that can't operate on it should
        // skip and ILM should not be blocked (not should the managed index move into the ERROR step)
        TimeSeriesRestDriver.awaitStepKey(client(), restoredIndexName, "cold", null, PhaseCompleteStep.NAME);
    }

    public void testRestoredIndexManagedByLocalPolicySkipsIllegalActions() throws Exception {
        // let's create a data stream, rollover it and convert the first generation backing index into a searchable snapshot
        createSnapshotRepo(client(), snapshotRepo, randomBoolean());
        createPolicy(
            client(),
            policy,
            new Phase(
                "hot",
                TimeValue.ZERO,
                Map.of(
                    RolloverAction.NAME,
                    // We create the policy with maxDocs 2 since we're required to have a rollover action if we're creating a searchable
                    // snapshot in the hot phase. But we will only index one document and trigger the rollover manually,
                    // to improve reliability and speed of the test.
                    new RolloverAction(RolloverConditions.newBuilder().addMaxIndexDocsCondition(2L).build()),
                    SearchableSnapshotAction.NAME,
                    new SearchableSnapshotAction(snapshotRepo)
                )
            ),
            new Phase("warm", TimeValue.timeValueDays(30), Map.of(SetPriorityAction.NAME, new SetPriorityAction(999))),
            null,
            null,
            null
        );

        String template = randomAlphaOfLengthBetween(5, 10).toLowerCase(Locale.ROOT);
        createComposableTemplate(
            client(),
            template,
            dataStream,
            new Template(
                Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 5).put(LifecycleSettings.LIFECYCLE_NAME, policy).build(),
                null,
                null
            )
        );

        // rolling over the data stream so we can apply the searchable snapshot policy to a backing index that's not the write index
        // indexing only one document as we want only one rollover to be triggered
        indexDocument(client(), dataStream, true);
        rolloverMaxOneDocCondition(client(), dataStream);
        List<String> backingIndices = getDataStreamBackingIndexNames(dataStream);
        assertThat(backingIndices.size(), equalTo(2));

        String backingIndexName = backingIndices.getFirst();
        String searchableSnapMountedIndexName = SearchableSnapshotAction.FULL_RESTORED_INDEX_PREFIX + backingIndexName;
        awaitIndexExists(searchableSnapMountedIndexName, TimeValue.timeValueSeconds(20));
        TimeSeriesRestDriver.awaitStepKey(client(), searchableSnapMountedIndexName, "hot", null, PhaseCompleteStep.NAME);
        // Wait for the original index to be deleted, to ensure ILM has finished
        awaitIndexDoesNotExist(backingIndexName);

        // snapshot the data stream
        String dsSnapshotName = "snapshot_ds_" + dataStream;
        Request takeSnapshotRequest = new Request("PUT", "/_snapshot/" + snapshotRepo + "/" + dsSnapshotName);
        takeSnapshotRequest.addParameter("wait_for_completion", "true");
        takeSnapshotRequest.setJsonEntity("{\"indices\": \"" + dataStream + "\", \"include_global_state\": false}");
        assertOK(client().performRequest(takeSnapshotRequest));

        // now that we have a backup of the data stream, let's delete the local one and update the ILM policy to include some illegal
        // actions for when we restore the data stream (given that the first generation backing index will be backed by a searchable
        // snapshot)
        assertOK(client().performRequest(new Request("DELETE", "/_data_stream/" + dataStream)));

        try {
            createPolicy(
                client(),
                policy,
                new Phase("hot", TimeValue.ZERO, Map.of()),
                new Phase(
                    "warm",
                    TimeValue.ZERO,
                    Map.of(ShrinkAction.NAME, new ShrinkAction(1, null, false), ForceMergeAction.NAME, new ForceMergeAction(1, null))
                ),
                new Phase("cold", TimeValue.ZERO, Map.of(FreezeAction.NAME, FreezeAction.INSTANCE)),
                null,
                null
            );
            fail("Expected a deprecation warning.");
        } catch (WarningFailureException e) {
            assertThat(e.getMessage(), containsString("The freeze action in ILM is deprecated and will be removed in a future version"));
        }

        // restore the datastream
        Request restoreSnapshot = new Request("POST", "/_snapshot/" + snapshotRepo + "/" + dsSnapshotName + "/_restore");
        restoreSnapshot.addParameter("wait_for_completion", "true");
        restoreSnapshot.setJsonEntity("{\"indices\": \"" + dataStream + "\", \"include_global_state\": false}");
        assertOK(client().performRequest(restoreSnapshot));

        awaitIndexExists(searchableSnapMountedIndexName);
        ensureGreen(searchableSnapMountedIndexName);

        // the restored index is now managed by the now updated ILM policy and needs to go through the warm and cold phase
        assertBusy(() -> {
            Step.StepKey stepKeyForIndex;
            try {
                stepKeyForIndex = getStepKeyForIndex(client(), searchableSnapMountedIndexName);
            } catch (WarningFailureException e) {
                assertThat(
                    e.getMessage(),
                    containsString("The freeze action in ILM is deprecated and will be removed in a future version")
                );
                ObjectPath objectPath = ObjectPath.createFromResponse(e.getResponse());
                Map<String, Map<String, Object>> indices = objectPath.evaluate("indices");
                stepKeyForIndex = TimeSeriesRestDriver.getStepKey(indices.get(searchableSnapMountedIndexName));
            }
            assertThat(stepKeyForIndex.phase(), is("cold"));
            assertThat(stepKeyForIndex.name(), is(PhaseCompleteStep.NAME));
        }, 30, TimeUnit.SECONDS);
    }

    public void testIdenticalSearchableSnapshotActionIsNoop() throws Exception {
        String index = "myindex-" + randomAlphaOfLength(4).toLowerCase(Locale.ROOT) + "-000001";
        createSnapshotRepo(client(), snapshotRepo, randomBoolean());
        createPolicy(
            client(),
            policy,
            null,
            null,
            new Phase(
                "hot",
                TimeValue.ZERO,
                Map.of(
                    RolloverAction.NAME,
                    // We create the policy with maxDocs 2 since we're required to have a rollover action if we're creating a searchable
                    // snapshot in the hot phase. But we will only index one document and trigger the rollover manually,
                    // to improve reliability and speed of the test.
                    new RolloverAction(RolloverConditions.newBuilder().addMaxIndexDocsCondition(2L).build()),
                    SearchableSnapshotAction.NAME,
                    new SearchableSnapshotAction(snapshotRepo, randomBoolean())
                )
            ),
            new Phase(
                "cold",
                TimeValue.ZERO,
                Map.of(SearchableSnapshotAction.NAME, new SearchableSnapshotAction(snapshotRepo, randomBoolean()))
            ),
            null
        );

        createIndex(
            index,
            Settings.builder().put(RolloverAction.LIFECYCLE_ROLLOVER_ALIAS, "alias").put(LifecycleSettings.LIFECYCLE_NAME, policy).build(),
            null,
            "\"alias\": {\"is_write_index\": true}"
        );
        ensureGreen(index);
        indexDocument(client(), index, true);
        rolloverMaxOneDocCondition(client(), "alias");

        final String searchableSnapMountedIndexName = SearchableSnapshotAction.FULL_RESTORED_INDEX_PREFIX + index;

        logger.info("--> waiting for [{}] to exist...", searchableSnapMountedIndexName);
        awaitIndexExists(searchableSnapMountedIndexName, TimeValue.timeValueSeconds(20));
        TimeSeriesRestDriver.awaitStepKey(client(), searchableSnapMountedIndexName, "cold", null, PhaseCompleteStep.NAME);
        // Wait for the original index to be deleted, to ensure ILM has finished
        awaitIndexDoesNotExist(index);

        List<Map<String, Object>> snapshots = getSnapshots();
        assertThat("expected to have only one snapshot, but got: " + snapshots, snapshots.size(), equalTo(1));

        Request hitCount = new Request("GET", "/" + searchableSnapMountedIndexName + "/_count");
        Map<String, Object> count = entityAsMap(client().performRequest(hitCount));
        assertThat("expected a single document but got: " + count, (int) count.get("count"), equalTo(1));
    }

    public void testConvertingSearchableSnapshotFromFullToPartial() throws Exception {
        String index = "myindex-" + randomAlphaOfLength(4).toLowerCase(Locale.ROOT);
        createSnapshotRepo(client(), snapshotRepo, randomBoolean());
        createPolicy(
            client(),
            policy,
            null,
            null,
            new Phase(
                "cold",
                TimeValue.ZERO,
                Map.of(SearchableSnapshotAction.NAME, new SearchableSnapshotAction(snapshotRepo, randomBoolean()))
            ),
            new Phase(
                "frozen",
                TimeValue.ZERO,
                Map.of(SearchableSnapshotAction.NAME, new SearchableSnapshotAction(snapshotRepo, randomBoolean()))
            ),
            null
        );

        createIndex(index, Settings.EMPTY);
        ensureGreen(index);
        indexDocument(client(), index, true);

        // enable ILM after we indexed a document as otherwise ILM might sometimes run so fast the indexDocument call will fail with
        // `index_not_found_exception`
        updateIndexSettings(index, Settings.builder().put(LifecycleSettings.LIFECYCLE_NAME, policy));

        final String searchableSnapMountedIndexName = SearchableSnapshotAction.PARTIAL_RESTORED_INDEX_PREFIX
            + SearchableSnapshotAction.FULL_RESTORED_INDEX_PREFIX + index;

        logger.info("--> waiting for [{}] to exist...", searchableSnapMountedIndexName);
        awaitIndexExists(searchableSnapMountedIndexName, TimeValue.timeValueSeconds(20));
        TimeSeriesRestDriver.awaitStepKey(client(), searchableSnapMountedIndexName, "frozen", null, PhaseCompleteStep.NAME);
        // Wait for the original index to be deleted, to ensure ILM has finished
        awaitIndexDoesNotExist(index);

        List<Map<String, Object>> snapshots = getSnapshots();
        assertThat("expected to have only one snapshot, but got: " + snapshots, snapshots.size(), equalTo(1));

        Request hitCount = new Request("GET", "/" + searchableSnapMountedIndexName + "/_count");
        Map<String, Object> count = entityAsMap(client().performRequest(hitCount));
        assertThat("expected a single document but got: " + count, (int) count.get("count"), equalTo(1));

        assertBusy(
            () -> assertTrue(
                "Expecting the mounted index to be deleted and to be converted to an alias",
                aliasExists(searchableSnapMountedIndexName, SearchableSnapshotAction.FULL_RESTORED_INDEX_PREFIX + index)
            )
        );
    }

    public void testResumingSearchableSnapshotFromFullToPartial() throws Exception {
        String index = "myindex-" + randomAlphaOfLength(4).toLowerCase(Locale.ROOT);
        createSnapshotRepo(client(), snapshotRepo, randomBoolean());
        var policyCold = "policy-cold";
        createPolicy(
            client(),
            policyCold,
            null,
            null,
            new Phase(
                "cold",
                TimeValue.ZERO,
                Map.of(SearchableSnapshotAction.NAME, new SearchableSnapshotAction(snapshotRepo, randomBoolean()))
            ),
            null,
            null
        );
        var policyFrozen = "policy-cold-frozen";
        createPolicy(
            client(),
            policyFrozen,
            null,
            null,
            new Phase(
                "cold",
                TimeValue.ZERO,
                Map.of(SearchableSnapshotAction.NAME, new SearchableSnapshotAction(snapshotRepo, randomBoolean()))
            ),
            new Phase(
                "frozen",
                TimeValue.ZERO,
                Map.of(SearchableSnapshotAction.NAME, new SearchableSnapshotAction(snapshotRepo, randomBoolean()))
            ),
            null
        );

        createIndex(index, Settings.EMPTY);
        ensureGreen(index);
        indexDocument(client(), index, true);

        // enable ILM after we indexed a document as otherwise ILM might sometimes run so fast the indexDocument call will fail with
        // `index_not_found_exception`
        updateIndexSettings(index, Settings.builder().put(LifecycleSettings.LIFECYCLE_NAME, policyCold));

        final String fullMountedIndexName = SearchableSnapshotAction.FULL_RESTORED_INDEX_PREFIX + index;
        logger.info("--> waiting for [{}] to exist...", fullMountedIndexName);
        awaitIndexExists(fullMountedIndexName, TimeValue.timeValueSeconds(20));
        TimeSeriesRestDriver.awaitStepKey(client(), fullMountedIndexName, "cold", null, PhaseCompleteStep.NAME);
        // Wait for the original index to be deleted, to ensure ILM has finished
        awaitIndexDoesNotExist(index);

        // remove ILM
        {
            Request request = new Request("POST", "/" + fullMountedIndexName + "/_ilm/remove");
            Map<String, Object> responseMap = responseAsMap(client().performRequest(request));
            assertThat(responseMap.get("has_failures"), is(false));
        }
        // add cold-frozen
        updateIndexSettings(index, Settings.builder().put(LifecycleSettings.LIFECYCLE_NAME, policyFrozen));

        String partiallyMountedIndexName = SearchableSnapshotAction.PARTIAL_RESTORED_INDEX_PREFIX + fullMountedIndexName;
        logger.info("--> waiting for [{}] to exist...", partiallyMountedIndexName);
        awaitIndexExists(partiallyMountedIndexName, TimeValue.timeValueSeconds(20));
        TimeSeriesRestDriver.awaitStepKey(client(), partiallyMountedIndexName, "frozen", null, PhaseCompleteStep.NAME);

        // Ensure the searchable snapshot is not deleted when the index was deleted because it was not created by this
        // policy. We add the delete phase now to ensure that the index will not be deleted before we verify the above
        // assertions
        createPolicy(
            client(),
            policyFrozen,
            null,
            null,
            new Phase(
                "cold",
                TimeValue.ZERO,
                Map.of(SearchableSnapshotAction.NAME, new SearchableSnapshotAction(snapshotRepo, randomBoolean()))
            ),
            new Phase(
                "frozen",
                TimeValue.ZERO,
                Map.of(SearchableSnapshotAction.NAME, new SearchableSnapshotAction(snapshotRepo, randomBoolean()))
            ),
            new Phase("delete", TimeValue.ZERO, Map.of(DeleteAction.NAME, WITH_SNAPSHOT_DELETE))
        );
        logger.info("--> waiting for [{}] to be deleted...", partiallyMountedIndexName);
        awaitIndexDoesNotExist(partiallyMountedIndexName);
        List<Map<String, Object>> snapshots = getSnapshots();
        assertThat("expected to have only one snapshot, but got: " + snapshots, snapshots.size(), equalTo(1));
    }

    public void testResumingSearchableSnapshotFromPartialToFull() throws Exception {
        String index = "myindex-" + randomAlphaOfLength(4).toLowerCase(Locale.ROOT);
        createSnapshotRepo(client(), snapshotRepo, randomBoolean());
        var policyCold = "policy-cold";
        createPolicy(
            client(),
            policyCold,
            null,
            null,
            new Phase(
                "cold",
                TimeValue.ZERO,
                Map.of(SearchableSnapshotAction.NAME, new SearchableSnapshotAction(snapshotRepo, randomBoolean()))
            ),
            null,
            null
        );
        var policyColdFrozen = "policy-cold-frozen";
        createPolicy(
            client(),
            policyColdFrozen,
            null,
            null,
            new Phase(
                "cold",
                TimeValue.ZERO,
                Map.of(SearchableSnapshotAction.NAME, new SearchableSnapshotAction(snapshotRepo, randomBoolean()))
            ),
            new Phase(
                "frozen",
                TimeValue.ZERO,
                Map.of(SearchableSnapshotAction.NAME, new SearchableSnapshotAction(snapshotRepo, randomBoolean()))
            ),
            null
        );

        createIndex(index, Settings.EMPTY);
        ensureGreen(index);
        indexDocument(client(), index, true);

        // enable ILM after we indexed a document as otherwise ILM might sometimes run so fast the indexDocument call will fail with
        // `index_not_found_exception`
        updateIndexSettings(index, Settings.builder().put(LifecycleSettings.LIFECYCLE_NAME, policyColdFrozen));

        final String fullMountedIndexName = SearchableSnapshotAction.FULL_RESTORED_INDEX_PREFIX + index;
        final String partialMountedIndexName = SearchableSnapshotAction.PARTIAL_RESTORED_INDEX_PREFIX + fullMountedIndexName;
        logger.info("--> waiting for [{}] to exist...", partialMountedIndexName);
        awaitIndexExists(partialMountedIndexName, TimeValue.timeValueSeconds(20));
        TimeSeriesRestDriver.awaitStepKey(client(), partialMountedIndexName, "frozen", null, PhaseCompleteStep.NAME);
        // Wait for the original index to be deleted, to ensure ILM has finished
        awaitIndexDoesNotExist(index);

        // remove ILM from the partially mounted searchable snapshot
        {
            Request request = new Request("POST", "/" + partialMountedIndexName + "/_ilm/remove");
            Map<String, Object> responseMap = responseAsMap(client().performRequest(request));
            assertThat(responseMap.get("has_failures"), is(false));
        }
        // add a policy that will only include the fully mounted searchable snapshot
        updateIndexSettings(index, Settings.builder().put(LifecycleSettings.LIFECYCLE_NAME, policyCold));

        String restoredPartiallyMountedIndexName = SearchableSnapshotAction.FULL_RESTORED_INDEX_PREFIX + partialMountedIndexName;
        logger.info("--> waiting for [{}] to exist...", restoredPartiallyMountedIndexName);
        awaitIndexExists(restoredPartiallyMountedIndexName, TimeValue.timeValueSeconds(20));
        TimeSeriesRestDriver.awaitStepKey(client(), restoredPartiallyMountedIndexName, "cold", null, PhaseCompleteStep.NAME);

        // Ensure the searchable snapshot is not deleted when the index was deleted because it was not created by this
        // policy. We add the delete phase now to ensure that the index will not be deleted before we verify the above
        // assertions
        createPolicy(
            client(),
            policyCold,
            null,
            null,
            new Phase(
                "cold",
                TimeValue.ZERO,
                Map.of(SearchableSnapshotAction.NAME, new SearchableSnapshotAction(snapshotRepo, randomBoolean()))
            ),
            null,
            new Phase("delete", TimeValue.ZERO, Map.of(DeleteAction.NAME, WITH_SNAPSHOT_DELETE))
        );
        logger.info("--> waiting for [{}] to be deleted...", restoredPartiallyMountedIndexName);
        awaitIndexDoesNotExist(restoredPartiallyMountedIndexName);
        List<Map<String, Object>> snapshots = getSnapshots();
        assertThat("expected to have only one snapshot, but got: " + snapshots, snapshots.size(), equalTo(1));
    }

    public void testSecondSearchableSnapshotUsingDifferentRepoThrows() throws Exception {
        String secondRepo = randomAlphaOfLengthBetween(10, 20);
        createSnapshotRepo(client(), snapshotRepo, randomBoolean());
        createSnapshotRepo(client(), secondRepo, randomBoolean());
        ResponseException e = expectThrows(
            ResponseException.class,
            () -> createPolicy(
                client(),
                policy,
                null,
                null,
                new Phase(
                    "cold",
                    TimeValue.ZERO,
                    Map.of(SearchableSnapshotAction.NAME, new SearchableSnapshotAction(snapshotRepo, randomBoolean()))
                ),
                new Phase(
                    "frozen",
                    TimeValue.ZERO,
                    Map.of(SearchableSnapshotAction.NAME, new SearchableSnapshotAction(secondRepo, randomBoolean()))
                ),
                null
            )
        );

        assertThat(
            e.getMessage(),
            containsString("policy specifies [searchable_snapshot] action multiple times with differing repositories")
        );
    }

    public void testSearchableSnapshotsInHotPhasePinnedToHotNodes() throws Exception {
        createSnapshotRepo(client(), snapshotRepo, randomBoolean());
        createPolicy(
            client(),
            policy,
            new Phase(
                "hot",
                TimeValue.ZERO,
                Map.of(
                    RolloverAction.NAME,
                    // We create the policy with maxDocs 2 since we're required to have a rollover action if we're creating a searchable
                    // snapshot in the hot phase. But we will only index one document and trigger the rollover manually,
                    // to improve reliability and speed of the test.
                    new RolloverAction(RolloverConditions.newBuilder().addMaxIndexDocsCondition(2L).build()),
                    SearchableSnapshotAction.NAME,
                    new SearchableSnapshotAction(snapshotRepo, randomBoolean())
                )
            ),
            null,
            null,
            null,
            null
        );

        createComposableTemplate(
            client(),
            randomAlphaOfLengthBetween(5, 10).toLowerCase(Locale.ROOT),
            dataStream,
            new Template(
                Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1).put(LifecycleSettings.LIFECYCLE_NAME, policy).build(),
                null,
                null
            )
        );

        // Create the data stream.
        assertOK(client().performRequest(new Request("PUT", "_data_stream/" + dataStream)));

        var backingIndices = getDataStreamBackingIndexNames(dataStream);
        String firstGenIndex = backingIndices.get(0);
        Map<String, Object> indexSettings = getIndexSettingsAsMap(firstGenIndex);
        assertThat(indexSettings.get(DataTier.TIER_PREFERENCE), is("data_hot"));

        // rollover the data stream so searchable_snapshot can complete
        indexDocument(client(), dataStream, true);
        rolloverMaxOneDocCondition(client(), dataStream);

        final String restoredIndex = SearchableSnapshotAction.FULL_RESTORED_INDEX_PREFIX + firstGenIndex;
        logger.info("--> waiting for [{}] to exist...", restoredIndex);
        awaitIndexExists(restoredIndex, TimeValue.timeValueSeconds(20));
        TimeSeriesRestDriver.awaitStepKey(client(), restoredIndex, "hot", PhaseCompleteStep.NAME, PhaseCompleteStep.NAME);

        Map<String, Object> hotIndexSettings = getIndexSettingsAsMap(restoredIndex);
        // searchable snapshots mounted in the hot phase should be pinned to hot nodes
        assertThat(hotIndexSettings.get(DataTier.TIER_PREFERENCE), is("data_hot"));

        // Wait for the original index to be deleted, to ensure ILM has finished
        awaitIndexDoesNotExist(firstGenIndex);
    }

    // See: https://github.com/elastic/elasticsearch/issues/77269
    public void testSearchableSnapshotInvokesAsyncActionOnNewIndex() throws Exception {
        createSnapshotRepo(client(), snapshotRepo, randomBoolean());
        Map<String, LifecycleAction> coldActions = new HashMap<>(2);
        coldActions.put(SearchableSnapshotAction.NAME, new SearchableSnapshotAction(snapshotRepo, randomBoolean()));
        // Normally putting an allocate action in the cold phase with a searchable snapshot action
        // would cause the new index to get "stuck" since the async action would never be invoked
        // for the new index.
        coldActions.put(AllocateAction.NAME, new AllocateAction(null, 1, null, null, null));
        Phase coldPhase = new Phase("cold", TimeValue.ZERO, coldActions);
        createPolicy(client(), policy, null, null, coldPhase, null, null);

        createComposableTemplate(
            client(),
            randomAlphaOfLengthBetween(5, 10).toLowerCase(Locale.ROOT),
            dataStream,
            new Template(Settings.builder().put(LifecycleSettings.LIFECYCLE_NAME, policy).build(), null, null)
        );

        indexDocument(client(), dataStream, true);

        // rolling over the data stream so we can apply the searchable snapshot policy to a backing index that's not the write index
        rolloverMaxOneDocCondition(client(), dataStream);

        List<String> backingIndices = getDataStreamBackingIndexNames(dataStream);
        assertThat(backingIndices.size(), equalTo(2));
        String backingIndexName = backingIndices.getFirst();
        String restoredIndexName = SearchableSnapshotAction.FULL_RESTORED_INDEX_PREFIX + backingIndexName;
        awaitIndexExists(restoredIndexName, TimeValue.timeValueSeconds(20));

        TimeSeriesRestDriver.awaitStepKey(client(), restoredIndexName, null, null, PhaseCompleteStep.NAME);
        // Wait for the original index to be deleted, to ensure ILM has finished
        awaitIndexDoesNotExist(backingIndexName);
    }

    public void testSearchableSnapshotTotalShardsPerNode() throws Exception {
        String index = "myindex-" + randomAlphaOfLength(4).toLowerCase(Locale.ROOT);
        Integer totalShardsPerNode = 2;
        createSnapshotRepo(client(), snapshotRepo, randomBoolean());
        createPolicy(
            client(),
            policy,
            null,
            null,
            new Phase(
                "cold",
                TimeValue.ZERO,
                Map.of(SearchableSnapshotAction.NAME, new SearchableSnapshotAction(snapshotRepo, randomBoolean()))
            ),
            new Phase(
                "frozen",
                TimeValue.ZERO,
                Map.of(
                    SearchableSnapshotAction.NAME,
                    new SearchableSnapshotAction(snapshotRepo, randomBoolean(), totalShardsPerNode, null, null)
                )
            ),
            null
        );

        createIndex(index, Settings.EMPTY);
        ensureGreen(index);
        indexDocument(client(), index, true);

        // enable ILM after we indexed a document as otherwise ILM might sometimes run so fast the indexDocument call will fail with
        // `index_not_found_exception`
        updateIndexSettings(index, Settings.builder().put(LifecycleSettings.LIFECYCLE_NAME, policy));

        // wait for snapshot successfully mounted and ILM execution completed
        final String searchableSnapMountedIndexName = SearchableSnapshotAction.PARTIAL_RESTORED_INDEX_PREFIX
            + SearchableSnapshotAction.FULL_RESTORED_INDEX_PREFIX + index;
        logger.info("--> waiting for [{}] to exist...", searchableSnapMountedIndexName);
        awaitIndexExists(searchableSnapMountedIndexName, TimeValue.timeValueSeconds(20));
        TimeSeriesRestDriver.awaitStepKey(client(), searchableSnapMountedIndexName, "frozen", null, PhaseCompleteStep.NAME);
        // Wait for the original index to be deleted, to ensure ILM has finished
        awaitIndexDoesNotExist(index);

        // validate total_shards_per_node setting
        Map<String, Object> indexSettings = getIndexSettingsAsMap(searchableSnapMountedIndexName);
        assertNotNull("expected total_shards_per_node to exist", indexSettings.get(INDEX_TOTAL_SHARDS_PER_NODE_SETTING.getKey()));
        Integer snapshotTotalShardsPerNode = Integer.valueOf((String) indexSettings.get(INDEX_TOTAL_SHARDS_PER_NODE_SETTING.getKey()));
        assertEquals(
            "expected total_shards_per_node to be " + totalShardsPerNode + ", but got: " + snapshotTotalShardsPerNode,
            totalShardsPerNode,
            snapshotTotalShardsPerNode
        );
    }

    public void testSearchableSnapshotReplicateFor() throws Exception {
        createSnapshotRepo(client(), snapshotRepo, randomBoolean());

        final boolean forceMergeIndex = randomBoolean();
        createPolicy(
            client(),
            policy,
            null,
            null,
            null,
            new Phase(
                "cold",
                TimeValue.ZERO,
                Map.of(
                    SearchableSnapshotAction.NAME,
                    new SearchableSnapshotAction(snapshotRepo, forceMergeIndex, null, TimeValue.timeValueHours(2), null)
                )
            ),
            new Phase("delete", TimeValue.timeValueDays(1), Map.of(DeleteAction.NAME, WITH_SNAPSHOT_DELETE))
        );

        createComposableTemplate(
            client(),
            randomAlphaOfLengthBetween(5, 10).toLowerCase(Locale.ROOT),
            dataStream,
            new Template(Settings.builder().put(LifecycleSettings.LIFECYCLE_NAME, policy).build(), null, null)
        );

        indexDocument(client(), dataStream, true);

        // rolling over the data stream so we can apply the searchable snapshot policy to a backing index that's not the write index
        rolloverMaxOneDocCondition(client(), dataStream);

        List<String> backingIndices = getDataStreamBackingIndexNames(dataStream);
        assertThat(backingIndices.size(), equalTo(2));
        String backingIndexName = backingIndices.getFirst();
        String restoredIndexName = SearchableSnapshotAction.FULL_RESTORED_INDEX_PREFIX + backingIndexName;
        awaitIndexExists(restoredIndexName, TimeValue.timeValueSeconds(20));

        // check that the index is in the expected step and has the expected step_info.message
        assertBusy(() -> {
            Map<String, Object> explainResponse = explainIndex(client(), restoredIndexName);
            assertThat(explainResponse.get("step"), is(WaitUntilReplicateForTimePassesStep.NAME));
            @SuppressWarnings("unchecked")
            final var stepInfo = (Map<String, String>) explainResponse.get("step_info");
            String message = stepInfo == null ? "" : stepInfo.get("message");
            assertThat(message, containsString("Waiting [less than 1d] until the replicate_for time [2h] has elapsed"));
            assertThat(message, containsString("for index [" + restoredIndexName + "] before removing replicas."));
        }, 30, TimeUnit.SECONDS);

        // check that it has the right number of replicas
        {
            Map<String, Object> indexSettings = getIndexSettingsAsMap(restoredIndexName);
            assertNotNull("expected number_of_replicas to exist", indexSettings.get(INDEX_NUMBER_OF_REPLICAS_SETTING.getKey()));
            Integer numberOfReplicas = Integer.valueOf((String) indexSettings.get(INDEX_NUMBER_OF_REPLICAS_SETTING.getKey()));
            assertThat(numberOfReplicas, is(1));
        }
        // Wait for the original index to be deleted, to ensure ILM has finished
        awaitIndexDoesNotExist(backingIndexName);

        // tweak the policy to replicate_for hardly any time at all
        createPolicy(
            client(),
            policy,
            null,
            null,
            null,
            new Phase(
                "cold",
                TimeValue.ZERO,
                Map.of(
                    SearchableSnapshotAction.NAME,
                    new SearchableSnapshotAction(snapshotRepo, forceMergeIndex, null, TimeValue.timeValueSeconds(10), null)
                )
            ),
            new Phase("delete", TimeValue.timeValueDays(1), Map.of(DeleteAction.NAME, WITH_SNAPSHOT_DELETE))
        );

        // check that the index has progressed because enough time has passed now that the policy is different
        TimeSeriesRestDriver.awaitStepKey(client(), restoredIndexName, "cold", null, PhaseCompleteStep.NAME);

        // check that it has the right number of replicas
        {
            Map<String, Object> indexSettings = getIndexSettingsAsMap(restoredIndexName);
            assertNotNull("expected number_of_replicas to exist", indexSettings.get(INDEX_NUMBER_OF_REPLICAS_SETTING.getKey()));
            Integer numberOfReplicas = Integer.valueOf((String) indexSettings.get(INDEX_NUMBER_OF_REPLICAS_SETTING.getKey()));
            assertThat(numberOfReplicas, is(0));
        }
    }

    /**
     * Prepares a data stream with the specified number of primary and replica shards,
     * creates a snapshot repository and ILM policy, applies a composable template,
     * indexes several documents, and performs a rollover. Returns the name of the
     * first generation backing index.
     */
    private String prepareDataStreamWithDocs(String phase, int numberOfPrimaries, int numberOfReplicas) throws Exception {
        logger.info(
            "--> running [{}] with [{}] primaries, [{}] replicas, in phase [{}}",
            getTestName(),
            numberOfPrimaries,
            numberOfReplicas,
            phase
        );
        createSnapshotRepo(client(), snapshotRepo, randomBoolean());
        createNewSingletonPolicy(client(), policy, phase, new SearchableSnapshotAction(snapshotRepo, true));

        final var indexSettings = indexSettings(numberOfPrimaries, numberOfReplicas);
        // Randomly enable auto-expand replicas to test that we remove the setting for the clone with 0 replicas.
        if (numberOfReplicas > 0 && randomBoolean()) {
            logger.info("--> enabling auto-expand replicas on backing index");
            indexSettings.put(IndexMetadata.SETTING_AUTO_EXPAND_REPLICAS, "1-all");
        }
        createComposableTemplate(
            client(),
            randomAlphaOfLengthBetween(5, 10).toLowerCase(Locale.ROOT),
            dataStream,
            new Template(indexSettings.build(), null, null)
        );
        for (int i = 0; i < randomIntBetween(5, 10); i++) {
            indexDocument(client(), dataStream, true);
        }
        final var backingIndexName = getDataStreamBackingIndexNames(dataStream).getFirst();

        // Retrieve the number of segments in the first (random) shard of the backing index.
        final Integer preLifecycleBackingIndexSegments = getNumberOfPrimarySegments(client(), backingIndexName);
        // If we have only one primary shard, we expect at least one segment. We're hoping to get multiple segments, but we can't guarantee
        // that, so we have to resort to a "greater than or equal to" check.
        if (numberOfPrimaries == 1) {
            assertThat(preLifecycleBackingIndexSegments, greaterThanOrEqualTo(1));
        } else {
            // With multiple primary shards, the segments are more spread out, so it's even less likely that we'll get more than 1 segment
            // in one shard, and some shards might even be empty.
            assertThat(preLifecycleBackingIndexSegments, greaterThanOrEqualTo(0));
        }

        // Rolling over the data stream so we can apply the searchable snapshot policy to a backing index that's not the write index.
        rolloverMaxOneDocCondition(client(), dataStream);
        // Wait for all shards to be allocated.
        ensureGreen(dataStream);
        return backingIndexName;
    }

    /**
     * Waits for the force-merge clone index to exist and asserts that it has the correct number of primary shards and zero replicas,
     * and that its name follows the expected naming convention.
     */
    private static void assertForceMergeCloneIndexSettings(String backingIndexName, int numberOfPrimaries) throws Exception {
        final String forceMergeIndexPattern = FORCE_MERGE_CLONE_INDEX_PREFIX + "*" + backingIndexName;
        assertBusy(() -> {
            // The force-merged index is hidden, so we need to expand wildcards.
            Request request = new Request("GET", "/" + forceMergeIndexPattern + "/_settings?expand_wildcards=all&flat_settings=true");
            Response response = client().performRequest(request);
            final Map<String, Object> indicesSettings = entityAsMap(response);
            assertThat(
                "expected only settings for index: " + forceMergeIndexPattern + ", but got " + indicesSettings,
                indicesSettings.size(),
                equalTo(1)
            );
            final String forceMergeCloneIndexName = indicesSettings.keySet().iterator().next();
            assertThat(forceMergeCloneIndexName, startsWith(FORCE_MERGE_CLONE_INDEX_PREFIX));
            assertThat(forceMergeCloneIndexName, endsWith(backingIndexName));
            @SuppressWarnings("unchecked")
            final var forceMergeIndexResponse = (Map<String, Object>) indicesSettings.get(forceMergeCloneIndexName);
            @SuppressWarnings("unchecked")
            final Map<String, Object> forceMergeIndexSettings = (Map<String, Object>) forceMergeIndexResponse.get("settings");
            assertThat(forceMergeIndexSettings.get("index.number_of_shards"), equalTo(String.valueOf(numberOfPrimaries)));
            assertThat(forceMergeIndexSettings.get("index.number_of_replicas"), equalTo("0"));
        });
    }

    /**
     * Asserts that the restored searchable snapshot index exists, the original and force-merge clone indices are deleted,
     * and the restored index has the expected number of segments. Also verifies the snapshot index naming conventions.
     *
     * @param phase The phase of the ILM policy that the searchable snapshot action runs in.
     * @param backingIndexName The original backing index name.
     * @param numberOfPrimaries The number of primaries that the original backing index had, affecting segment count assertions.
     * @param withReplicas True if the original backing index had one or more replicas, affecting snapshot index naming assertions.
     */
    private void assertForceMergedSnapshotDone(String phase, String backingIndexName, int numberOfPrimaries, boolean withReplicas)
        throws Exception {
        final String prefix = phase.equals("cold")
            ? SearchableSnapshotAction.FULL_RESTORED_INDEX_PREFIX
            : SearchableSnapshotAction.PARTIAL_RESTORED_INDEX_PREFIX;
        final String restoredIndexName = prefix + backingIndexName;
        awaitIndexExists(restoredIndexName, TimeValue.timeValueSeconds(20));

        assertBusy(() -> assertThat(explainIndex(client(), restoredIndexName).get("step"), is(PhaseCompleteStep.NAME)));
        // Wait for the original index to be deleted, to ensure ILM has finished
        awaitIndexDoesNotExist(backingIndexName);
        // Regardless of whether we force merged the backing index or a clone, the cloned index should not exist (anymore).
        awaitIndexDoesNotExist(FORCE_MERGE_CLONE_INDEX_PREFIX + "-*-" + backingIndexName);

        // Retrieve the total number of segments across all primary shards of the restored index.
        final Integer numberOfPrimarySegments = getNumberOfPrimarySegments(client(), restoredIndexName);
        // If the backing index had multiple primaries, some primaries might be empty, but others should have no more than 1 segment.
        if (numberOfPrimaries > 1 || phase.equals("frozen")) {
            assertThat(numberOfPrimarySegments, lessThanOrEqualTo(numberOfPrimaries));
        } else {
            // If the backing index had only one primary, we expect exactly 1 segment after force merging.
            assertThat(numberOfPrimarySegments, equalTo(1));
        }

        // We can't assert the replicas of the mounted snapshot as it's created with 0 replicas by default, but we can at least assert
        // that the mounted snapshot was taken from the correct source index.
        final List<Map<String, Object>> snapshots = getSnapshots();
        assertThat("expected to have only one snapshot, but got: " + snapshots, snapshots.size(), equalTo(1));
        final Map<String, Object> snapshot = snapshots.getFirst();
        @SuppressWarnings("unchecked")
        final List<String> indices = (List<String>) snapshot.get("indices");
        assertThat("expected to have only one index, but got: " + indices, indices.size(), equalTo(1));
        final String snapshotIndexName = indices.getFirst();
        // If the backing index had replicas, we force merged a clone, so the snapshot index name should match the clone naming pattern.
        if (withReplicas) {
            assertThat(
                "expected index to start with the force merge prefix",
                snapshotIndexName,
                startsWith(FORCE_MERGE_CLONE_INDEX_PREFIX)
            );
            assertThat("expected index to end with the backing index name", snapshotIndexName, endsWith(backingIndexName));
        } else {
            // If the backing index had no replicas, we force merged the backing index itself, so the snapshot index name should be equal
            // to the backing index name.
            assertThat("expected index to be the backing index name", snapshotIndexName, equalTo(backingIndexName));
        }
    }

    private List<Map<String, Object>> getSnapshots() throws IOException {
        Request getSnaps = new Request("GET", "/_snapshot/" + snapshotRepo + "/_all");
        Response response = client().performRequest(getSnaps);
        ObjectPath objectPath = ObjectPath.createFromResponse(response);
        return objectPath.evaluate("snapshots");
    }

    /**
     * Updates the cluster settings to enable or disable shard allocation in the whole cluster.
     */
    private static void configureClusterAllocation(boolean enable) throws IOException {
        final String value = enable ? null : "none";
        updateClusterSettings(
            Settings.builder().put(EnableAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ENABLE_SETTING.getKey(), value).build()
        );
    }
}
