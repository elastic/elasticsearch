/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ilm.actions;

import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.test.rest.ESRestTestCase;
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
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static java.util.Collections.singletonMap;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.xpack.TimeSeriesRestDriver.createComposableTemplate;
import static org.elasticsearch.xpack.TimeSeriesRestDriver.createNewSingletonPolicy;
import static org.elasticsearch.xpack.TimeSeriesRestDriver.createPolicy;
import static org.elasticsearch.xpack.TimeSeriesRestDriver.createSnapshotRepo;
import static org.elasticsearch.xpack.TimeSeriesRestDriver.explainIndex;
import static org.elasticsearch.xpack.TimeSeriesRestDriver.getNumberOfSegments;
import static org.elasticsearch.xpack.TimeSeriesRestDriver.getStepKeyForIndex;
import static org.elasticsearch.xpack.TimeSeriesRestDriver.indexDocument;
import static org.elasticsearch.xpack.TimeSeriesRestDriver.rolloverMaxOneDocCondition;
import static org.hamcrest.Matchers.containsStringIgnoringCase;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;

public class SearchableSnapshotActionIT extends ESRestTestCase {

    private String policy;
    private String dataStream;
    private String snapshotRepo;

    @Before
    public void refreshIndex() {
        dataStream = "logs-" + randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        policy = "policy-" + randomAlphaOfLength(5);
        snapshotRepo = randomAlphaOfLengthBetween(4, 10);
    }

    @After
    public void waitNoRestoresInProgressInRepo() throws Exception {
        /*
         * This should be a "fairly quick" wait as each test waits for the searchable snapshot action
         * to complete.
         * It seems that sometime there is a discrepancy between the {@link org.elasticsearch.repositories.RepositoryData}
         * status of the snapshots and the cluster state {@link org.elasticsearch.cluster.RestoreInProgress} metadata, which
         * will prevent {@link #wipeCluster} from wiping the snapshots as the cluster state would show some
         * as "in progress" and won't be able to delete them.
         */
        assertBusy(() -> {
            try {
                Response response = client().performRequest(new Request("GET", "/_snapshot/" + snapshotRepo + "/_status"));
                assertThat(EntityUtils.toString(response.getEntity()), containsStringIgnoringCase("\"snapshots\":[]"));
            } catch (IOException e) {
                // converting to AssertionError here so assertBusy retries
                throw new AssertionError(e);
            }
        });
    }

    public void testSearchableSnapshotAction() throws Exception {
        createSnapshotRepo(client(), snapshotRepo, randomBoolean());
        createNewSingletonPolicy(client(), policy, "cold", new SearchableSnapshotAction(snapshotRepo, true));

        createComposableTemplate(client(), "template-name", dataStream,
            new Template(Settings.builder().put(LifecycleSettings.LIFECYCLE_NAME, policy).build(), null, null));

        indexDocument(client(), dataStream, true);

        // rolling over the data stream so we can apply the searchable snapshot policy to a backing index that's not the write index
        rolloverMaxOneDocCondition(client(), dataStream);

        String backingIndexName = DataStream.getDefaultBackingIndexName(dataStream, 1L);
        String restoredIndexName = SearchableSnapshotAction.RESTORED_INDEX_PREFIX + backingIndexName;
        assertTrue(waitUntil(() -> {
            try {
                return indexExists(restoredIndexName);
            } catch (IOException e) {
                return false;
            }
        }, 30, TimeUnit.SECONDS));

        assertBusy(() -> assertThat(explainIndex(client(), restoredIndexName).get("step"), is(PhaseCompleteStep.NAME)), 30,
            TimeUnit.SECONDS);
    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/67879")
    public void testSearchableSnapshotForceMergesIndexToOneSegment() throws Exception {
        createSnapshotRepo(client(), snapshotRepo, randomBoolean());
        createNewSingletonPolicy(client(), policy, "cold", new SearchableSnapshotAction(snapshotRepo, true));

        createComposableTemplate(client(), "template-name", dataStream, new Template(null, null, null));

        for (int i = 0; i < randomIntBetween(5, 10); i++) {
            indexDocument(client(), dataStream, true);
        }

        String backingIndexName = DataStream.getDefaultBackingIndexName(dataStream, 1L);
        assertThat(getNumberOfSegments(client(), backingIndexName), greaterThan(1));

        // rolling over the data stream so we can apply the searchable snapshot policy to a backing index that's not the write index
        rolloverMaxOneDocCondition(client(), dataStream);

        updateIndexSettings(dataStream, Settings.builder().put(LifecycleSettings.LIFECYCLE_NAME, policy));
        assertTrue(waitUntil(() -> {
            try {
                Integer numberOfSegments = getNumberOfSegments(client(), backingIndexName);
                logger.info("index {} has {} segments", backingIndexName, numberOfSegments);
                return numberOfSegments == 1;
            } catch (Exception e) {
                try {
                    // if ILM executed the action already we don't have an index to assert on so we don't fail the test
                    return indexExists(backingIndexName) == false;
                } catch (IOException ex) {
                    return false;
                }
            }
        }, 60, TimeUnit.SECONDS));

        String restoredIndexName = SearchableSnapshotAction.RESTORED_INDEX_PREFIX + backingIndexName;
        assertTrue(waitUntil(() -> {
            try {
                return indexExists(restoredIndexName);
            } catch (IOException e) {
                return false;
            }
        }, 30, TimeUnit.SECONDS));

        assertBusy(() -> assertThat(explainIndex(client(), restoredIndexName).get("step"), is(PhaseCompleteStep.NAME)), 30,
            TimeUnit.SECONDS);
    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/pull/54433")
    public void testDeleteActionDeletesSearchableSnapshot() throws Exception {
        createSnapshotRepo(client(), snapshotRepo, randomBoolean());

        // create policy with cold and delete phases
        Map<String, LifecycleAction> coldActions =
            Map.of(SearchableSnapshotAction.NAME, new SearchableSnapshotAction(snapshotRepo));
        Map<String, Phase> phases = new HashMap<>();
        phases.put("cold", new Phase("cold", TimeValue.ZERO, coldActions));
        phases.put("delete", new Phase("delete", TimeValue.timeValueMillis(10000), singletonMap(DeleteAction.NAME,
            new DeleteAction(true))));
        LifecyclePolicy lifecyclePolicy = new LifecyclePolicy(policy, phases);
        // PUT policy
        XContentBuilder builder = jsonBuilder();
        lifecyclePolicy.toXContent(builder, null);
        final StringEntity entity = new StringEntity(
            "{ \"policy\":" + Strings.toString(builder) + "}", ContentType.APPLICATION_JSON);
        Request createPolicyRequest = new Request("PUT", "_ilm/policy/" + policy);
        createPolicyRequest.setEntity(entity);
        assertOK(client().performRequest(createPolicyRequest));

        createComposableTemplate(client(), "template-name", dataStream,
            new Template(Settings.builder().put(LifecycleSettings.LIFECYCLE_NAME, policy).build(), null, null));

        indexDocument(client(), dataStream, true);

        // rolling over the data stream so we can apply the searchable snapshot policy to a backing index that's not the write index
        rolloverMaxOneDocCondition(client(), dataStream);

        String[] snapshotName = new String[1];
        String backingIndexName = DataStream.getDefaultBackingIndexName(dataStream, 1L);
        String restoredIndexName = SearchableSnapshotAction.RESTORED_INDEX_PREFIX + backingIndexName;
        assertTrue(waitUntil(() -> {
            try {
                Map<String, Object> explainIndex = explainIndex(client(), backingIndexName);
                if (explainIndex == null) {
                    // in case we missed the original index and it was deleted
                    explainIndex = explainIndex(client(), restoredIndexName);
                }
                snapshotName[0] = (String) explainIndex.get("snapshot_name");
                return snapshotName[0] != null;
            } catch (IOException e) {
                return false;
            }
        }, 30, TimeUnit.SECONDS));
        assertBusy(() -> assertFalse(indexExists(restoredIndexName)));

        assertTrue("the snapshot we generate in the cold phase should be deleted by the delete phase", waitUntil(() -> {
            try {
                Request getSnapshotsRequest = new Request("GET", "_snapshot/" + snapshotRepo + "/" + snapshotName[0]);
                Response getSnapshotsResponse = client().performRequest(getSnapshotsRequest);
                return EntityUtils.toString(getSnapshotsResponse.getEntity()).contains("snapshot_missing_exception");
            } catch (IOException e) {
                return false;
            }
        }, 30, TimeUnit.SECONDS));
    }

    public void testCreateInvalidPolicy() {
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> createPolicy(client(), policy,
            new Phase("hot", TimeValue.ZERO, Map.of(RolloverAction.NAME, new RolloverAction(null, null, 1L), SearchableSnapshotAction.NAME,
                new SearchableSnapshotAction(randomAlphaOfLengthBetween(4, 10)))),
            new Phase("warm", TimeValue.ZERO, Map.of(ForceMergeAction.NAME, new ForceMergeAction(1, null))),
            new Phase("cold", TimeValue.ZERO, Map.of(FreezeAction.NAME, new FreezeAction())),
            null
            )
        );

        assertThat(exception.getMessage(), is("phases [warm,cold] define one or more of [searchable_snapshot, forcemerge, freeze, shrink, rollup]" +
            " actions which are not allowed after a managed index is mounted as a searchable snapshot"));
    }

    public void testUpdatePolicyToAddPhasesYieldsInvalidActionsToBeSkipped() throws Exception {
        createSnapshotRepo(client(), snapshotRepo, randomBoolean());
        createPolicy(client(), policy,
            new Phase("hot", TimeValue.ZERO, Map.of(RolloverAction.NAME, new RolloverAction(null, null, 1L), SearchableSnapshotAction.NAME,
                new SearchableSnapshotAction(snapshotRepo))),
            new Phase("warm", TimeValue.timeValueDays(30), Map.of(SetPriorityAction.NAME, new SetPriorityAction(999))),
            null, null
        );

        createComposableTemplate(client(), "template-name", dataStream,
            new Template(Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 5)
                .put(LifecycleSettings.LIFECYCLE_NAME, policy)
                .build(), null, null)
        );

        // rolling over the data stream so we can apply the searchable snapshot policy to a backing index that's not the write index
        for (int i = 0; i < randomIntBetween(5, 10); i++) {
            indexDocument(client(), dataStream, true);
        }

        String restoredIndexName = SearchableSnapshotAction.RESTORED_INDEX_PREFIX + DataStream.getDefaultBackingIndexName(dataStream, 1L);
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

        createPolicy(client(), policy,
            new Phase("hot", TimeValue.ZERO, Map.of(SetPriorityAction.NAME, new SetPriorityAction(10))),
            new Phase("warm", TimeValue.ZERO,
                Map.of(ShrinkAction.NAME, new ShrinkAction(1), ForceMergeAction.NAME, new ForceMergeAction(1, null))
            ),
            new Phase("cold", TimeValue.ZERO, Map.of(SearchableSnapshotAction.NAME, new SearchableSnapshotAction(snapshotRepo))),
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
        createPolicy(client(), policy,
            new Phase("hot", TimeValue.ZERO, Map.of(RolloverAction.NAME, new RolloverAction(null, null, 1L),
                SearchableSnapshotAction.NAME, new SearchableSnapshotAction(snapshotRepo))),
            new Phase("warm", TimeValue.timeValueDays(30), Map.of(SetPriorityAction.NAME, new SetPriorityAction(999))),
            null, null
        );

        createComposableTemplate(client(), "template-name", dataStream,
            new Template(Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 5)
                .put(LifecycleSettings.LIFECYCLE_NAME, policy)
                .build(), null, null)
        );

        // rolling over the data stream so we can apply the searchable snapshot policy to a backing index that's not the write index
        for (int i = 0; i < randomIntBetween(5, 10); i++) {
            indexDocument(client(), dataStream, true);
        }

        String searchableSnapMountedIndexName = SearchableSnapshotAction.RESTORED_INDEX_PREFIX +
            DataStream.getDefaultBackingIndexName(dataStream, 1L);
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

        createPolicy(client(), policy,
            new Phase("hot", TimeValue.ZERO, Map.of()),
            new Phase("warm", TimeValue.ZERO,
                Map.of(ShrinkAction.NAME, new ShrinkAction(1), ForceMergeAction.NAME, new ForceMergeAction(1, null))
            ),
            new Phase("cold", TimeValue.ZERO, Map.of(FreezeAction.NAME, new FreezeAction())),
            null
        );

        // restore the datastream
        Request restoreSnapshot = new Request("POST", "/_snapshot/" + snapshotRepo + "/" + dsSnapshotName + "/_restore");
        restoreSnapshot.addParameter("wait_for_completion", "true");
        restoreSnapshot.setJsonEntity("{\"indices\": \"" + dataStream + "\", \"include_global_state\": true}");
        assertOK(client().performRequest(restoreSnapshot));

        assertThat(indexExists(searchableSnapMountedIndexName), is(true));

        // the restored index is now managed by the now updated ILM policy and needs to go through the warm and cold phase
        assertBusy(() -> {
            Step.StepKey stepKeyForIndex = getStepKeyForIndex(client(), searchableSnapMountedIndexName);
            assertThat(stepKeyForIndex.getPhase(), is("cold"));
            assertThat(stepKeyForIndex.getName(), is(PhaseCompleteStep.NAME));
        }, 30, TimeUnit.SECONDS);
    }
}
