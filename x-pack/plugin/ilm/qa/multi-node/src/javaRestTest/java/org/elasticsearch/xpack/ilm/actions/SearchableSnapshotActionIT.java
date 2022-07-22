/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ilm.actions;

import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.cluster.routing.allocation.DataTier;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
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
import org.junit.Before;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static java.util.Collections.singletonMap;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.xpack.TimeSeriesRestDriver.createComposableTemplate;
import static org.elasticsearch.xpack.TimeSeriesRestDriver.createNewSingletonPolicy;
import static org.elasticsearch.xpack.TimeSeriesRestDriver.createPolicy;
import static org.elasticsearch.xpack.TimeSeriesRestDriver.createSnapshotRepo;
import static org.elasticsearch.xpack.TimeSeriesRestDriver.explainIndex;
import static org.elasticsearch.xpack.TimeSeriesRestDriver.getNumberOfSegments;
import static org.elasticsearch.xpack.TimeSeriesRestDriver.getStepKeyForIndex;
import static org.elasticsearch.xpack.TimeSeriesRestDriver.indexDocument;
import static org.elasticsearch.xpack.TimeSeriesRestDriver.rolloverMaxOneDocCondition;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;

public class SearchableSnapshotActionIT extends ESRestTestCase {

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

    @Override
    protected boolean waitForAllSnapshotsWiped() {
        return true;
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

        String backingIndexName = DataStream.getDefaultBackingIndexName(dataStream, 1L);
        String restoredIndexName = SearchableSnapshotAction.FULL_RESTORED_INDEX_PREFIX + backingIndexName;
        assertTrue(waitUntil(() -> {
            try {
                return indexExists(restoredIndexName);
            } catch (IOException e) {
                return false;
            }
        }, 30, TimeUnit.SECONDS));

        assertBusy(
            () -> assertThat(explainIndex(client(), restoredIndexName).get("step"), is(PhaseCompleteStep.NAME)),
            30,
            TimeUnit.SECONDS
        );
    }

    public void testSearchableSnapshotForceMergesIndexToOneSegment() throws Exception {
        createSnapshotRepo(client(), snapshotRepo, randomBoolean());
        createNewSingletonPolicy(client(), policy, "cold", new SearchableSnapshotAction(snapshotRepo, true));

        createComposableTemplate(
            client(),
            randomAlphaOfLengthBetween(5, 10).toLowerCase(Locale.ROOT),
            dataStream,
            new Template(null, null, null)
        );

        for (int i = 0; i < randomIntBetween(5, 10); i++) {
            indexDocument(client(), dataStream, true);
        }

        String backingIndexName = DataStream.getDefaultBackingIndexName(dataStream, 1L);
        Integer preLifecycleBackingIndexSegments = getNumberOfSegments(client(), backingIndexName);
        assertThat(preLifecycleBackingIndexSegments, greaterThanOrEqualTo(1));

        // rolling over the data stream so we can apply the searchable snapshot policy to a backing index that's not the write index
        rolloverMaxOneDocCondition(client(), dataStream);

        updateIndexSettings(dataStream, Settings.builder().put(LifecycleSettings.LIFECYCLE_NAME, policy));
        assertTrue(waitUntil(() -> {
            try {
                Integer numberOfSegments = getNumberOfSegments(client(), backingIndexName);
                logger.info("index {} has {} segments", backingIndexName, numberOfSegments);
                // this is a loose assertion here as forcemerge is best effort
                if (preLifecycleBackingIndexSegments > 1) {
                    return numberOfSegments < preLifecycleBackingIndexSegments;
                } else {
                    // the index had only one segement to start with so nothing to assert
                    return true;
                }
            } catch (Exception e) {
                try {
                    // if ILM executed the action already we don't have an index to assert on so we don't fail the test
                    return indexExists(backingIndexName) == false;
                } catch (IOException ex) {
                    return false;
                }
            }
        }, 60, TimeUnit.SECONDS));

        String restoredIndexName = SearchableSnapshotAction.FULL_RESTORED_INDEX_PREFIX + backingIndexName;
        assertTrue(waitUntil(() -> {
            try {
                return indexExists(restoredIndexName);
            } catch (IOException e) {
                return false;
            }
        }, 60, TimeUnit.SECONDS));

        assertBusy(
            () -> assertThat(explainIndex(client(), restoredIndexName).get("step"), is(PhaseCompleteStep.NAME)),
            30,
            TimeUnit.SECONDS
        );
    }

    @SuppressWarnings("unchecked")
    public void testDeleteActionDeletesSearchableSnapshot() throws Exception {
        createSnapshotRepo(client(), snapshotRepo, randomBoolean());

        // create policy with cold and delete phases
        Map<String, LifecycleAction> coldActions = Map.of(SearchableSnapshotAction.NAME, new SearchableSnapshotAction(snapshotRepo));
        Map<String, Phase> phases = new HashMap<>();
        phases.put("cold", new Phase("cold", TimeValue.ZERO, coldActions));
        phases.put(
            "delete",
            new Phase("delete", TimeValue.timeValueMillis(10000), singletonMap(DeleteAction.NAME, DeleteAction.WITH_SNAPSHOT_DELETE))
        );
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

        String backingIndexName = DataStream.getDefaultBackingIndexName(dataStream, 1L);
        String restoredIndexName = SearchableSnapshotAction.FULL_RESTORED_INDEX_PREFIX + backingIndexName;

        // let's wait for ILM to finish
        assertBusy(() -> assertFalse(indexExists(backingIndexName)), 60, TimeUnit.SECONDS);
        assertBusy(() -> assertFalse(indexExists(restoredIndexName)), 60, TimeUnit.SECONDS);

        assertTrue("the snapshot we generate in the cold phase should be deleted by the delete phase", waitUntil(() -> {
            try {
                Request getSnapshotsRequest = new Request("GET", "_snapshot/" + snapshotRepo + "/_all");
                Response getSnapshotsResponse = client().performRequest(getSnapshotsRequest);

                Map<String, Object> responseMap;
                try (InputStream is = getSnapshotsResponse.getEntity().getContent()) {
                    responseMap = XContentHelper.convertToMap(XContentType.JSON.xContent(), is, true);
                }
                Object snapshots = responseMap.get("snapshots");
                return ((List<Map<String, Object>>) snapshots).size() == 0;
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
                return false;
            }
        }, 30, TimeUnit.SECONDS));
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
                "phases [warm,cold] define one or more of [forcemerge, freeze, shrink, rollup]"
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
                    new RolloverAction(null, null, null, 1L, null, null, null, null, null, null),
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
        for (int i = 0; i < randomIntBetween(5, 10); i++) {
            indexDocument(client(), dataStream, true);
        }

        String restoredIndexName = SearchableSnapshotAction.FULL_RESTORED_INDEX_PREFIX + DataStream.getDefaultBackingIndexName(
            dataStream,
            1L
        );
        assertTrue(waitUntil(() -> {
            try {
                return indexExists(restoredIndexName);
            } catch (IOException e) {
                return false;
            }
        }, 30, TimeUnit.SECONDS));

        assertBusy(() -> {
            Step.StepKey stepKeyForIndex = getStepKeyForIndex(client(), restoredIndexName);
            assertThat(stepKeyForIndex.getPhase(), is("hot"));
            assertThat(stepKeyForIndex.getName(), is(PhaseCompleteStep.NAME));
        }, 30, TimeUnit.SECONDS);

        createPolicy(
            client(),
            policy,
            new Phase("hot", TimeValue.ZERO, Map.of(SetPriorityAction.NAME, new SetPriorityAction(10))),
            new Phase(
                "warm",
                TimeValue.ZERO,
                Map.of(ShrinkAction.NAME, new ShrinkAction(1, null), ForceMergeAction.NAME, new ForceMergeAction(1, null))
            ),
            new Phase("cold", TimeValue.ZERO, Map.of(SearchableSnapshotAction.NAME, new SearchableSnapshotAction(snapshotRepo))),
            null,
            null
        );

        // even though the index is now mounted as a searchable snapshot, the actions that can't operate on it should
        // skip and ILM should not be blocked (not should the managed index move into the ERROR step)
        assertBusy(() -> {
            Step.StepKey stepKeyForIndex = getStepKeyForIndex(client(), restoredIndexName);
            assertThat(stepKeyForIndex.getPhase(), is("cold"));
            assertThat(stepKeyForIndex.getName(), is(PhaseCompleteStep.NAME));
        }, 30, TimeUnit.SECONDS);
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
                    new RolloverAction(null, null, null, 1L, null, null, null, null, null, null),
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
        // indexing only one document as we want only one rollover to be triggered
        indexDocument(client(), dataStream, true);

        String searchableSnapMountedIndexName = SearchableSnapshotAction.FULL_RESTORED_INDEX_PREFIX + DataStream.getDefaultBackingIndexName(
            dataStream,
            1L
        );
        assertTrue(waitUntil(() -> {
            try {
                return indexExists(searchableSnapMountedIndexName);
            } catch (IOException e) {
                return false;
            }
        }, 30, TimeUnit.SECONDS));

        assertBusy(() -> {
            Step.StepKey stepKeyForIndex = getStepKeyForIndex(client(), searchableSnapMountedIndexName);
            assertThat(stepKeyForIndex.getPhase(), is("hot"));
            assertThat(stepKeyForIndex.getName(), is(PhaseCompleteStep.NAME));
        }, 30, TimeUnit.SECONDS);

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

        createPolicy(
            client(),
            policy,
            new Phase("hot", TimeValue.ZERO, Map.of()),
            new Phase(
                "warm",
                TimeValue.ZERO,
                Map.of(ShrinkAction.NAME, new ShrinkAction(1, null), ForceMergeAction.NAME, new ForceMergeAction(1, null))
            ),
            new Phase("cold", TimeValue.ZERO, Map.of(FreezeAction.NAME, FreezeAction.INSTANCE)),
            null,
            null
        );

        // restore the datastream
        Request restoreSnapshot = new Request("POST", "/_snapshot/" + snapshotRepo + "/" + dsSnapshotName + "/_restore");
        restoreSnapshot.addParameter("wait_for_completion", "true");
        restoreSnapshot.setJsonEntity("{\"indices\": \"" + dataStream + "\", \"include_global_state\": false}");
        assertOK(client().performRequest(restoreSnapshot));

        assertThat(indexExists(searchableSnapMountedIndexName), is(true));
        ensureGreen(searchableSnapMountedIndexName);

        // the restored index is now managed by the now updated ILM policy and needs to go through the warm and cold phase
        assertBusy(() -> {
            Step.StepKey stepKeyForIndex = getStepKeyForIndex(client(), searchableSnapMountedIndexName);
            assertThat(stepKeyForIndex.getPhase(), is("cold"));
            assertThat(stepKeyForIndex.getName(), is(PhaseCompleteStep.NAME));
        }, 30, TimeUnit.SECONDS);
    }

    @SuppressWarnings("unchecked")
    public void testIdenticalSearchableSnapshotActionIsNoop() throws Exception {
        String index = "myindex-" + randomAlphaOfLength(4).toLowerCase(Locale.ROOT) + "-000001";
        createSnapshotRepo(client(), snapshotRepo, randomBoolean());
        Map<String, LifecycleAction> hotActions = new HashMap<>();
        hotActions.put(RolloverAction.NAME, new RolloverAction(null, null, null, 1L, null, null, null, null, null, null));
        hotActions.put(SearchableSnapshotAction.NAME, new SearchableSnapshotAction(snapshotRepo, randomBoolean()));
        createPolicy(
            client(),
            policy,
            null,
            null,
            new Phase("hot", TimeValue.ZERO, hotActions),
            new Phase(
                "cold",
                TimeValue.ZERO,
                singletonMap(SearchableSnapshotAction.NAME, new SearchableSnapshotAction(snapshotRepo, randomBoolean()))
            ),
            null
        );

        createIndex(
            index,
            Settings.builder().put(RolloverAction.LIFECYCLE_ROLLOVER_ALIAS, "alias").build(),
            null,
            "\"alias\": {\"is_write_index\": true}"
        );
        ensureGreen(index);
        indexDocument(client(), index, true);

        // enable ILM after we indexed a document as otherwise ILM might sometimes run so fast the indexDocument call will fail with
        // `index_not_found_exception`
        updateIndexSettings(index, Settings.builder().put(LifecycleSettings.LIFECYCLE_NAME, policy));

        final String searchableSnapMountedIndexName = SearchableSnapshotAction.FULL_RESTORED_INDEX_PREFIX + index;

        assertBusy(() -> {
            logger.info("--> waiting for [{}] to exist...", searchableSnapMountedIndexName);
            assertTrue(indexExists(searchableSnapMountedIndexName));
        }, 30, TimeUnit.SECONDS);

        assertBusy(() -> {
            Step.StepKey stepKeyForIndex = getStepKeyForIndex(client(), searchableSnapMountedIndexName);
            assertThat(stepKeyForIndex.getPhase(), is("cold"));
            assertThat(stepKeyForIndex.getName(), is(PhaseCompleteStep.NAME));
        }, 30, TimeUnit.SECONDS);

        Request getSnaps = new Request("GET", "/_snapshot/" + snapshotRepo + "/_all");
        Response response = client().performRequest(getSnaps);
        Map<String, Object> responseMap;
        try (InputStream is = response.getEntity().getContent()) {
            responseMap = XContentHelper.convertToMap(XContentType.JSON.xContent(), is, true);
        }
        assertThat(
            "expected to have only one snapshot, but got: " + responseMap,
            ((List<Map<String, Object>>) responseMap.get("snapshots")).size(),
            equalTo(1)
        );

        Request hitCount = new Request("GET", "/" + searchableSnapMountedIndexName + "/_count");
        Map<String, Object> count = entityAsMap(client().performRequest(hitCount));
        assertThat("expected a single document but got: " + count, (int) count.get("count"), equalTo(1));
    }

    @SuppressWarnings("unchecked")
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
                singletonMap(SearchableSnapshotAction.NAME, new SearchableSnapshotAction(snapshotRepo, randomBoolean()))
            ),
            new Phase(
                "frozen",
                TimeValue.ZERO,
                singletonMap(SearchableSnapshotAction.NAME, new SearchableSnapshotAction(snapshotRepo, randomBoolean()))
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

        assertBusy(() -> {
            logger.info("--> waiting for [{}] to exist...", searchableSnapMountedIndexName);
            assertTrue(indexExists(searchableSnapMountedIndexName));
        }, 30, TimeUnit.SECONDS);

        assertBusy(() -> {
            Step.StepKey stepKeyForIndex = getStepKeyForIndex(client(), searchableSnapMountedIndexName);
            assertThat(stepKeyForIndex.getPhase(), is("frozen"));
            assertThat(stepKeyForIndex.getName(), is(PhaseCompleteStep.NAME));
        }, 30, TimeUnit.SECONDS);

        Request getSnaps = new Request("GET", "/_snapshot/" + snapshotRepo + "/_all");
        Response response = client().performRequest(getSnaps);
        Map<String, Object> responseMap;
        try (InputStream is = response.getEntity().getContent()) {
            responseMap = XContentHelper.convertToMap(XContentType.JSON.xContent(), is, true);
        }
        assertThat(
            "expected to have only one snapshot, but got: " + responseMap,
            ((List<Map<String, Object>>) responseMap.get("snapshots")).size(),
            equalTo(1)
        );

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
                    singletonMap(SearchableSnapshotAction.NAME, new SearchableSnapshotAction(snapshotRepo, randomBoolean()))
                ),
                new Phase(
                    "frozen",
                    TimeValue.ZERO,
                    singletonMap(SearchableSnapshotAction.NAME, new SearchableSnapshotAction(secondRepo, randomBoolean()))
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
                    new RolloverAction(null, null, null, 1L, null, null, null, null, null, null),
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

        indexDocument(client(), dataStream, true);
        String firstGenIndex = DataStream.getDefaultBackingIndexName(dataStream, 1L);
        Map<String, Object> indexSettings = getIndexSettingsAsMap(firstGenIndex);
        assertThat(indexSettings.get(DataTier.TIER_PREFERENCE), is("data_hot"));

        // rollover the data stream so searchable_snapshot can complete
        rolloverMaxOneDocCondition(client(), dataStream);

        final String restoredIndex = SearchableSnapshotAction.FULL_RESTORED_INDEX_PREFIX + firstGenIndex;
        assertBusy(() -> {
            logger.info("--> waiting for [{}] to exist...", restoredIndex);
            assertTrue(indexExists(restoredIndex));
        }, 30, TimeUnit.SECONDS);
        assertBusy(
            () -> assertThat(getStepKeyForIndex(client(), restoredIndex), is(PhaseCompleteStep.finalStep("hot").getKey())),
            30,
            TimeUnit.SECONDS
        );

        Map<String, Object> hotIndexSettings = getIndexSettingsAsMap(restoredIndex);
        // searchable snapshots mounted in the hot phase should be pinned to hot nodes
        assertThat(hotIndexSettings.get(DataTier.TIER_PREFERENCE), is("data_hot"));
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

        String backingIndexName = DataStream.getDefaultBackingIndexName(dataStream, 1L);
        String restoredIndexName = SearchableSnapshotAction.FULL_RESTORED_INDEX_PREFIX + backingIndexName;
        assertTrue(waitUntil(() -> {
            try {
                return indexExists(restoredIndexName);
            } catch (IOException e) {
                return false;
            }
        }, 30, TimeUnit.SECONDS));

        assertBusy(
            () -> assertThat(explainIndex(client(), restoredIndexName).get("step"), is(PhaseCompleteStep.NAME)),
            30,
            TimeUnit.SECONDS
        );
    }
}
