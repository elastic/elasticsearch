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
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xpack.cluster.routing.allocation.DataTierAllocationDecider;
import org.elasticsearch.xpack.core.ilm.LifecycleAction;
import org.elasticsearch.xpack.core.ilm.LifecyclePolicy;
import org.elasticsearch.xpack.core.ilm.LifecycleSettings;
import org.elasticsearch.xpack.core.ilm.MigrateAction;
import org.elasticsearch.xpack.core.ilm.Phase;
import org.elasticsearch.xpack.core.ilm.PhaseCompleteStep;
import org.elasticsearch.xpack.core.ilm.RolloverAction;
import org.elasticsearch.xpack.core.ilm.SetSingleNodeAllocateStep;
import org.elasticsearch.xpack.core.ilm.ShrinkAction;
import org.elasticsearch.xpack.core.ilm.ShrinkStep;
import org.junit.Before;

import java.io.IOException;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static java.util.Collections.singletonMap;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.xpack.TimeSeriesRestDriver.createIndexWithSettings;
import static org.elasticsearch.xpack.TimeSeriesRestDriver.createNewSingletonPolicy;
import static org.elasticsearch.xpack.TimeSeriesRestDriver.createPolicy;
import static org.elasticsearch.xpack.TimeSeriesRestDriver.explainIndex;
import static org.elasticsearch.xpack.TimeSeriesRestDriver.getOnlyIndexSettings;
import static org.elasticsearch.xpack.TimeSeriesRestDriver.getSnapshotState;
import static org.elasticsearch.xpack.TimeSeriesRestDriver.getStepKeyForIndex;
import static org.elasticsearch.xpack.TimeSeriesRestDriver.index;
import static org.elasticsearch.xpack.TimeSeriesRestDriver.indexDocument;
import static org.elasticsearch.xpack.TimeSeriesRestDriver.updatePolicy;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class ShrinkActionIT extends ESRestTestCase {
    private static final String FAILED_STEP_RETRY_COUNT_FIELD = "failed_step_retry_count";
    private static final String SHRINK_INDEX_NAME = "shrink_index_name";

    private String policy;
    private String index;
    private String alias;

    @Before
    public void refreshAbstractions() {
        policy = "policy-" + randomAlphaOfLength(5);
        index = "index-" + randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        alias = "alias-" + randomAlphaOfLength(5);
        logger.info("--> running [{}] with index [{}], alias [{}] and policy [{}]", getTestName(), index, alias, policy);
    }

    public void testShrinkAction() throws Exception {
        int numShards = 4;
        int divisor = randomFrom(2, 4);
        int expectedFinalShards = numShards / divisor;
        createIndexWithSettings(client(), index, alias, Settings.builder().put(SETTING_NUMBER_OF_SHARDS, numShards)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0));
        createNewSingletonPolicy(client(), policy, "warm", new ShrinkAction(expectedFinalShards, null));
        updatePolicy(client(), index, policy);

        String shrunkenIndexName = getShrinkIndexName(index);
        assertBusy(() -> assertTrue(indexExists(shrunkenIndexName)), 30, TimeUnit.SECONDS);
        assertBusy(() -> assertTrue(aliasExists(shrunkenIndexName, index)));
        assertBusy(() -> assertThat(getStepKeyForIndex(client(), shrunkenIndexName),
            equalTo(PhaseCompleteStep.finalStep("warm").getKey())));
        assertBusy(() -> {
            Map<String, Object> settings = getOnlyIndexSettings(client(), shrunkenIndexName);
            assertThat(settings.get(SETTING_NUMBER_OF_SHARDS), equalTo(String.valueOf(expectedFinalShards)));
            assertThat(settings.get(IndexMetadata.INDEX_BLOCKS_WRITE_SETTING.getKey()), equalTo("true"));
            assertThat(settings.get(IndexMetadata.INDEX_ROUTING_REQUIRE_GROUP_SETTING.getKey() + "_id"), nullValue());
        });
        expectThrows(ResponseException.class, () -> indexDocument(client(), index));
    }

    public void testShrinkSameShards() throws Exception {
        int numberOfShards = randomFrom(1, 2);
        createIndexWithSettings(client(), index, alias, Settings.builder().put(SETTING_NUMBER_OF_SHARDS, numberOfShards)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0));
        createNewSingletonPolicy(client(), policy, "warm", new ShrinkAction(numberOfShards, null));
        updatePolicy(client(), index, policy);
        String shrunkenIndex = getShrinkIndexName(index);
        assertBusy(() -> {
            assertTrue(indexExists(index));
            assertFalse(indexExists(shrunkenIndex));
            assertFalse(aliasExists(shrunkenIndex, index));
            Map<String, Object> settings = getOnlyIndexSettings(client(), index);
            assertThat(getStepKeyForIndex(client(), index), equalTo(PhaseCompleteStep.finalStep("warm").getKey()));
            assertThat(settings.get(SETTING_NUMBER_OF_SHARDS), equalTo(String.valueOf(numberOfShards)));
            assertNull(settings.get(IndexMetadata.INDEX_BLOCKS_WRITE_SETTING.getKey()));
            assertThat(settings.get(IndexMetadata.INDEX_ROUTING_REQUIRE_GROUP_SETTING.getKey() + "_id"), nullValue());
        });
    }

    public void testShrinkDuringSnapshot() throws Exception {
        // Create the repository before taking the snapshot.
        Request request = new Request("PUT", "/_snapshot/repo");
        request.setJsonEntity(Strings
            .toString(JsonXContent.contentBuilder()
                .startObject()
                .field("type", "fs")
                .startObject("settings")
                .field("compress", randomBoolean())
                .field("location", System.getProperty("tests.path.repo"))
                .field("max_snapshot_bytes_per_sec", "256b")
                .endObject()
                .endObject()));
        assertOK(client().performRequest(request));
        // create delete policy
        createNewSingletonPolicy(client(), policy, "warm", new ShrinkAction(1, null), TimeValue.timeValueMillis(0));
        // create index without policy
        createIndexWithSettings(client(), index, alias, Settings.builder()
            .put(SETTING_NUMBER_OF_SHARDS, 2)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            // required so the shrink doesn't wait on SetSingleNodeAllocateStep
            .put(IndexMetadata.INDEX_ROUTING_REQUIRE_GROUP_SETTING.getKey() + "_name", "javaRestTest-0"));
        // index document so snapshot actually does something
        indexDocument(client(), index);
        // start snapshot
        request = new Request("PUT", "/_snapshot/repo/snapshot");
        request.addParameter("wait_for_completion", "false");
        request.setJsonEntity("{\"indices\": \"" + index + "\"}");
        assertOK(client().performRequest(request));
        // add policy and expect it to trigger shrink immediately (while snapshot in progress)
        updatePolicy(client(), index, policy);
        String shrunkenIndex = getShrinkIndexName(index);
        // assert that index was shrunk and original index was deleted
        assertBusy(() -> {
            assertTrue(indexExists(shrunkenIndex));
            assertTrue(aliasExists(shrunkenIndex, index));
            Map<String, Object> settings = getOnlyIndexSettings(client(), shrunkenIndex);
            assertThat(getStepKeyForIndex(client(), shrunkenIndex), equalTo(PhaseCompleteStep.finalStep("warm").getKey()));
            assertThat(settings.get(SETTING_NUMBER_OF_SHARDS), equalTo(String.valueOf(1)));
            assertThat(settings.get(IndexMetadata.INDEX_BLOCKS_WRITE_SETTING.getKey()), equalTo("true"));
            assertThat(settings.get(IndexMetadata.INDEX_ROUTING_REQUIRE_GROUP_SETTING.getKey() + "_id"), nullValue());
        }, 2, TimeUnit.MINUTES);
        expectThrows(ResponseException.class, () -> indexDocument(client(), index));
        // assert that snapshot succeeded
        assertThat(getSnapshotState(client(), "snapshot"), equalTo("SUCCESS"));
        assertOK(client().performRequest(new Request("DELETE", "/_snapshot/repo/snapshot")));
    }

    public void testShrinkActionInTheHotPhase() throws Exception {
        int numShards = 2;
        int expectedFinalShards = 1;
        String originalIndex = index + "-000001";

        // add a policy
        Map<String, LifecycleAction> hotActions = Map.of(
            RolloverAction.NAME, new RolloverAction(null, null, null, 1L),
            ShrinkAction.NAME, new ShrinkAction(expectedFinalShards, null));
        Map<String, Phase> phases = Map.of(
            "hot", new Phase("hot", TimeValue.ZERO, hotActions));
        LifecyclePolicy lifecyclePolicy = new LifecyclePolicy(policy, phases);
        Request createPolicyRequest = new Request("PUT", "_ilm/policy/" + policy);
        createPolicyRequest.setJsonEntity("{ \"policy\":" + Strings.toString(lifecyclePolicy) + "}");
        client().performRequest(createPolicyRequest);

        // and a template
        Request createTemplateRequest = new Request("PUT", "_template/" + index);
        createTemplateRequest.setJsonEntity("{" +
            "\"index_patterns\": [\"" + index + "-*\"], \n" +
            "  \"settings\": {\n" +
            "    \"number_of_shards\": " + numShards + ",\n" +
            "    \"number_of_replicas\": 0,\n" +
            "    \"index.lifecycle.name\": \"" + policy + "\", \n" +
            "    \"index.lifecycle.rollover_alias\": \"" + alias + "\"\n" +
            "  }\n" +
            "}");
        client().performRequest(createTemplateRequest);

        // then create the index and index a document to trigger rollover
        createIndexWithSettings(client(), originalIndex, alias, Settings.builder(), true);
        index(client(), originalIndex, "_id", "foo", "bar");

        String shrunkenIndex = getShrinkIndexName(originalIndex);
        assertBusy(() -> assertTrue(indexExists(shrunkenIndex)), 30, TimeUnit.SECONDS);
        assertBusy(() -> assertThat(getStepKeyForIndex(client(), shrunkenIndex), equalTo(PhaseCompleteStep.finalStep("hot").getKey())));
        assertBusy(() -> {
            Map<String, Object> settings = getOnlyIndexSettings(client(), shrunkenIndex);
            assertThat(settings.get(SETTING_NUMBER_OF_SHARDS), equalTo(String.valueOf(expectedFinalShards)));
        });
    }

    public void testSetSingleNodeAllocationRetriesUntilItSucceeds() throws Exception {
        int numShards = 2;
        int expectedFinalShards = 1;
        createIndexWithSettings(client(), index, alias, Settings.builder()
            .put(SETTING_NUMBER_OF_SHARDS, numShards)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .putNull(DataTierAllocationDecider.INDEX_ROUTING_PREFER));

        ensureGreen(index);

        // unallocate all index shards
        Request setAllocationToMissingAttribute = new Request("PUT", "/" + index + "/_settings");
        setAllocationToMissingAttribute.setJsonEntity("{\n" +
            "  \"settings\": {\n" +
            "    \"index.routing.allocation.include.rack\": \"bogus_rack\"" +
            "  }\n" +
            "}");
        client().performRequest(setAllocationToMissingAttribute);

        ensureHealth(index, (request) -> {
            request.addParameter("wait_for_status", "red");
            request.addParameter("timeout", "70s");
            request.addParameter("level", "shards");
        });

        // assign the policy that'll attempt to shrink the index (disabling the migrate action as it'll otherwise wait for
        // all shards to be active and we want that to happen as part of the shrink action)
        MigrateAction migrateAction = new MigrateAction(false);
        ShrinkAction shrinkAction = new ShrinkAction(expectedFinalShards, null);
        Phase phase = new Phase("warm", TimeValue.ZERO, Map.of(migrateAction.getWriteableName(), migrateAction,
            shrinkAction.getWriteableName(), shrinkAction));
        LifecyclePolicy lifecyclePolicy = new LifecyclePolicy(policy, singletonMap(phase.getName(), phase));
        XContentBuilder builder = jsonBuilder();
        lifecyclePolicy.toXContent(builder, null);
        final StringEntity entity = new StringEntity(
            "{ \"policy\":" + Strings.toString(builder) + "}", ContentType.APPLICATION_JSON);
        Request putPolicyRequest = new Request("PUT", "_ilm/policy/" + policy);
        putPolicyRequest.setEntity(entity);
        client().performRequest(putPolicyRequest);
        updatePolicy(client(), index, policy);

        assertTrue("ILM did not start retrying the set-single-node-allocation step", waitUntil(() -> {
            try {
                Map<String, Object> explainIndexResponse = explainIndex(client(), index);
                if (explainIndexResponse == null) {
                    return false;
                }
                String failedStep = (String) explainIndexResponse.get("failed_step");
                Integer retryCount = (Integer) explainIndexResponse.get(FAILED_STEP_RETRY_COUNT_FIELD);
                return failedStep != null && failedStep.equals(SetSingleNodeAllocateStep.NAME) && retryCount != null && retryCount >= 1;
            } catch (IOException e) {
                return false;
            }
        }, 30, TimeUnit.SECONDS));

        String shrunkenIndex = getShrinkIndexName(index);
        Request resetAllocationForIndex = new Request("PUT", "/" + index + "/_settings");
        resetAllocationForIndex.setJsonEntity("{\n" +
            "  \"settings\": {\n" +
            "    \"index.routing.allocation.include.rack\": null" +
            "  }\n" +
            "}");
        client().performRequest(resetAllocationForIndex);

        assertBusy(() -> assertTrue(indexExists(shrunkenIndex)), 30, TimeUnit.SECONDS);
        assertBusy(() -> assertTrue(aliasExists(shrunkenIndex, index)));
        assertBusy(() -> assertThat(getStepKeyForIndex(client(), shrunkenIndex), equalTo(PhaseCompleteStep.finalStep("warm").getKey())));
    }

    public void testAutomaticRetryFailedShrinkAction() throws Exception {
        int numShards = 4;
        int divisor = randomFrom(2, 4);
        int expectedFinalShards = numShards / divisor;
        createIndexWithSettings(client(), index, alias, Settings.builder().put(SETTING_NUMBER_OF_SHARDS, numShards)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0));
        createNewSingletonPolicy(client(), policy, "warm", new ShrinkAction(numShards + randomIntBetween(1, numShards), null));
        updatePolicy(client(), index, policy);
        assertBusy(() -> {
            String failedStep = getFailedStepForIndex(index);
            assertThat(failedStep, equalTo(ShrinkStep.NAME));
        }, 60, TimeUnit.SECONDS);

        // update policy to be correct
        createNewSingletonPolicy(client(), policy, "warm", new ShrinkAction(expectedFinalShards, null));
        updatePolicy(client(), index, policy);

        // assert corrected policy is picked up and index is shrunken
        String shrunkenIndex = getShrinkIndexName(index);
        assertBusy(() -> assertTrue(indexExists(shrunkenIndex)), 30, TimeUnit.SECONDS);
        assertBusy(() -> assertTrue(aliasExists(shrunkenIndex, index)));
        assertBusy(() -> assertThat(getStepKeyForIndex(client(), shrunkenIndex), equalTo(PhaseCompleteStep.finalStep("warm").getKey())));
        assertBusy(() -> {
            Map<String, Object> settings = getOnlyIndexSettings(client(), shrunkenIndex);
            assertThat(settings.get(SETTING_NUMBER_OF_SHARDS), equalTo(String.valueOf(expectedFinalShards)));
            assertThat(settings.get(IndexMetadata.INDEX_BLOCKS_WRITE_SETTING.getKey()), equalTo("true"));
            assertThat(settings.get(IndexMetadata.INDEX_ROUTING_REQUIRE_GROUP_SETTING.getKey() + "_id"), nullValue());
        });
        expectThrows(ResponseException.class, () -> indexDocument(client(), index));
    }

    public void testShrinkStepMovesForwardIfShrunkIndexIsCreatedBetweenRetries() throws Exception {
        int numShards = 4;
        int expectedFinalShards = 1;
        createIndexWithSettings(client(), index, alias, Settings.builder().put(SETTING_NUMBER_OF_SHARDS, numShards)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0));
        createNewSingletonPolicy(client(), policy, "warm", new ShrinkAction(numShards + randomIntBetween(1, numShards), null));
        updatePolicy(client(), index, policy);
        assertBusy(() -> {
            String failedStep = getFailedStepForIndex(index);
            assertThat(failedStep, equalTo(ShrinkStep.NAME));
        }, 60, TimeUnit.SECONDS);

        String shrinkIndexName = getShrinkIndexName(index);
        Request shrinkIndexRequest = new Request("POST", index + "/_shrink/" + shrinkIndexName);
        shrinkIndexRequest.setEntity(new StringEntity(
            "{\"settings\": {\n" +
                "      \"" + SETTING_NUMBER_OF_SHARDS + "\": 1,\n" +
                "      \"" + SETTING_NUMBER_OF_REPLICAS + "\": 0,\n" +
                "      \"" + LifecycleSettings.LIFECYCLE_NAME + "\": \"" + policy + "\",\n" +
                "      \"" + IndexMetadata.INDEX_ROUTING_REQUIRE_GROUP_SETTING.getKey() + "_id" + "\": null\n" +
                "   \n}\n" +
                "\n}", ContentType.APPLICATION_JSON));
        client().performRequest(shrinkIndexRequest);

        // assert manually shrunk index is picked up and policy completes successfully
        String shrunkenIndex = getShrinkIndexName(index);
        assertBusy(() -> assertTrue(indexExists(shrunkenIndex)), 30, TimeUnit.SECONDS);
        assertBusy(() -> assertTrue(aliasExists(shrunkenIndex, index)));
        assertBusy(() -> assertThat(getStepKeyForIndex(client(), shrunkenIndex), equalTo(PhaseCompleteStep.finalStep("warm").getKey())));
        assertBusy(() -> {
            Map<String, Object> settings = getOnlyIndexSettings(client(), shrunkenIndex);
            assertThat(settings.get(SETTING_NUMBER_OF_SHARDS), equalTo(String.valueOf(expectedFinalShards)));
            assertThat(settings.get(IndexMetadata.INDEX_BLOCKS_WRITE_SETTING.getKey()), equalTo("true"));
            assertThat(settings.get(IndexMetadata.INDEX_ROUTING_REQUIRE_GROUP_SETTING.getKey() + "_id"), nullValue());
        });
        expectThrows(ResponseException.class, () -> indexDocument(client(), index));
    }

    public void testWaitInShrunkShardsAllocatedExceedsThreshold() throws Exception {
        int numShards = 4;
        int expectedFinalShards = 1;
        createIndexWithSettings(client(), index, alias, Settings.builder().put(SETTING_NUMBER_OF_SHARDS, numShards)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(LifecycleSettings.LIFECYCLE_STEP_WAIT_TIME_THRESHOLD, "1s")
        );

        createPolicy(client(), policy, null, new Phase("warm", TimeValue.ZERO,
            Map.of(MigrateAction.NAME, new MigrateAction(false), ShrinkAction.NAME,
                new ShrinkAction(numShards + randomIntBetween(1, numShards), null))), null, null, null
        );
        updatePolicy(client(), index, policy);

        // ILM will retry the shrink step because the number of shards to shrink to is gt the current number of shards
        assertBusy(() -> {
            Map<String, Object> explainIndex = explainIndex(client(), index);
            Integer retryCount = (Integer) explainIndex.get(FAILED_STEP_RETRY_COUNT_FIELD);
            assertThat(retryCount, notNullValue());
            assertThat(retryCount, greaterThanOrEqualTo(1));
        }, 30, TimeUnit.SECONDS);

        String firstAttemptShrinkIndexName = getShrinkIndexName(index);

        // we're manually shrinking the index but configuring a very high number of replicas and waiting for all active shards
        // this will make ths shrunk index unable to allocate successfully, so ILM will wait in the `shrunk-shards-allocated` step
        Request shrinkIndexRequest = new Request("POST", index + "/_shrink/" + firstAttemptShrinkIndexName);
        shrinkIndexRequest.setEntity(new StringEntity(
            "{\"settings\": {\n" +
                "      \"" + SETTING_NUMBER_OF_SHARDS + "\": 1,\n" +
                "      \"" + SETTING_NUMBER_OF_REPLICAS + "\": 349,\n" +
                "      \"index.write.wait_for_active_shards\": \"all\",\n" +
                "      \"" + LifecycleSettings.LIFECYCLE_NAME + "\": \"" + policy + "\",\n" +
                "      \"" + IndexMetadata.INDEX_ROUTING_REQUIRE_GROUP_SETTING.getKey() + "_id" + "\": null\n" +
                "   \n}\n" +
                "\n}", ContentType.APPLICATION_JSON));
        client().performRequest(shrinkIndexRequest);

        // wait until the threshold is passed and a new shrink index name is generated
        assertBusy(() -> {
            Map<String, Object> explainIndexResponse = explainIndex(client(), index);
            String secondCycleShrinkIndexName = (String) explainIndexResponse.get(SHRINK_INDEX_NAME);
            assertThat(secondCycleShrinkIndexName, notNullValue());
            // ILM generated another shrink index name
            assertThat(secondCycleShrinkIndexName, not(equalTo(firstAttemptShrinkIndexName)));
        }, 60, TimeUnit.SECONDS);

        // the index we first attempted to shrink to must be deleted as we went back to the `cleanup-shrink-index` step
        assertThat(indexExists(firstAttemptShrinkIndexName), is(false));

        // ILM will again stop in the `shrink` step as the number of target shards in the shrink action is still configured to a
        // number higher than the index's current number of shards
        assertBusy(() -> {
            Map<String, Object> explainIndex = explainIndex(client(), index);
            Integer retryCount = (Integer) explainIndex.get(FAILED_STEP_RETRY_COUNT_FIELD);
            assertThat(retryCount, notNullValue());
            assertThat(retryCount, greaterThanOrEqualTo(1));
        }, 30, TimeUnit.SECONDS);

        // update policy to be correct and allow the index to shrink successfully
        createPolicy(client(), policy, null, new Phase("warm", TimeValue.ZERO,
                Map.of(MigrateAction.NAME, new MigrateAction(false), ShrinkAction.NAME, new ShrinkAction(expectedFinalShards, null))),
            null, null, null);
        updatePolicy(client(), index, policy);

        // assert corrected policy is picked up and index is shrunken
        String shrunkenIndex = getShrinkIndexName(index);
        assertBusy(() -> assertTrue(indexExists(shrunkenIndex)), 30, TimeUnit.SECONDS);
        assertBusy(() -> assertTrue(aliasExists(shrunkenIndex, index)));
        assertBusy(() -> assertThat(getStepKeyForIndex(client(), shrunkenIndex), equalTo(PhaseCompleteStep.finalStep("warm").getKey())));
    }

    @Nullable
    private String getFailedStepForIndex(String indexName) throws IOException {
        Map<String, Object> indexResponse = explainIndex(client(), indexName);
        if (indexResponse == null) {
            return null;
        }

        return (String) indexResponse.get("failed_step");
    }

    private String getShrinkIndexName(String originalIndex) throws InterruptedException, IOException {
        String[] shrunkenIndexName = new String[1];
        waitUntil(() -> {
            try {
                Map<String, Object> explainIndexResponse = explainIndex(client(), originalIndex);
                if (explainIndexResponse == null) {
                    return false;
                }
                shrunkenIndexName[0] = (String) explainIndexResponse.get(SHRINK_INDEX_NAME);
                return shrunkenIndexName[0] != null;
            } catch (IOException e) {
                return false;
            }
        }, 30, TimeUnit.SECONDS);
        assert shrunkenIndexName[0] != null : "lifecycle execution state must contain the target shrink index name for policy [" + policy +
            "] and originalIndex [" + originalIndex + "]. state is: " + explainIndex(client(), originalIndex);
        return shrunkenIndexName[0];
    }
}
