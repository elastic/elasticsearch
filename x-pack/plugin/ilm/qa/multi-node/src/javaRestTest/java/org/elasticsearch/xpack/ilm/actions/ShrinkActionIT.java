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
import org.elasticsearch.cluster.routing.allocation.decider.ShardsLimitAllocationDecider;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.rest.action.admin.indices.RestPutIndexTemplateAction;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xpack.cluster.routing.allocation.DataTierAllocationDecider;
import org.elasticsearch.xpack.core.ilm.CheckTargetShardsCountStep;
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
import org.elasticsearch.xpack.core.ilm.Step;
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
import static org.elasticsearch.xpack.TimeSeriesRestDriver.explainIndex;
import static org.elasticsearch.xpack.TimeSeriesRestDriver.getOnlyIndexSettings;
import static org.elasticsearch.xpack.TimeSeriesRestDriver.getSnapshotState;
import static org.elasticsearch.xpack.TimeSeriesRestDriver.getStepKeyForIndex;
import static org.elasticsearch.xpack.TimeSeriesRestDriver.index;
import static org.elasticsearch.xpack.TimeSeriesRestDriver.indexDocument;
import static org.elasticsearch.xpack.TimeSeriesRestDriver.updatePolicy;
import static org.elasticsearch.xpack.TimeSeriesRestDriver.waitAndGetShrinkIndexName;
import static org.hamcrest.Matchers.equalTo;
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

        String shrunkenIndexName = waitAndGetShrinkIndexName(client(), index);
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
        assertBusy(() -> {
            assertTrue(indexExists(index));
            Map<String, Object> settings = getOnlyIndexSettings(client(), index);
            assertThat(getStepKeyForIndex(client(), index), equalTo(PhaseCompleteStep.finalStep("warm").getKey()));
            assertThat(settings.get(SETTING_NUMBER_OF_SHARDS), equalTo(String.valueOf(numberOfShards)));
            assertNull(settings.get(IndexMetadata.INDEX_BLOCKS_WRITE_SETTING.getKey()));
            assertThat(settings.get(IndexMetadata.INDEX_ROUTING_REQUIRE_GROUP_SETTING.getKey() + "_id"), nullValue());
            // the shrink action was effectively skipped so there must not be any `shrink_index_name` in the ILM state
            assertThat(explainIndex(client(), index).get("shrink_index_name"), nullValue());
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
        String shrunkenIndex = waitAndGetShrinkIndexName(client(), index);
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
        createTemplateRequest.setOptions(expectWarnings(RestPutIndexTemplateAction.DEPRECATION_WARNING));
        client().performRequest(createTemplateRequest);

        // then create the index and index a document to trigger rollover
        createIndexWithSettings(client(), originalIndex, alias, Settings.builder(), true);
        index(client(), originalIndex, "_id", "foo", "bar");

        String shrunkenIndex = waitAndGetShrinkIndexName(client(), originalIndex);
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

        Request resetAllocationForIndex = new Request("PUT", "/" + index + "/_settings");
        resetAllocationForIndex.setJsonEntity("{\n" +
            "  \"settings\": {\n" +
            "    \"index.routing.allocation.include.rack\": null" +
            "  }\n" +
            "}");
        client().performRequest(resetAllocationForIndex);

        String shrunkenIndex = waitAndGetShrinkIndexName(client(), index);
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
        assertBusy(() -> assertThat(getStepKeyForIndex(client(), index),
            equalTo(new Step.StepKey("warm", ShrinkAction.NAME, CheckTargetShardsCountStep.NAME))), 60, TimeUnit.SECONDS);

        // update policy to be correct
        createNewSingletonPolicy(client(), policy, "warm", new ShrinkAction(expectedFinalShards, null));
        updatePolicy(client(), index, policy);

        // assert corrected policy is picked up and index is shrunken
        String shrunkenIndex = waitAndGetShrinkIndexName(client(), index);
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

    /*
     * This test verifies that we can still shrink an index even if the total number of shards in the index is greater than
     * index.routing.allocation.total_shards_per_node.
     */
    public void testTotalShardsPerNodeTooLow() throws Exception {
        int numShards = 4;
        int divisor = randomFrom(2, 4);
        int expectedFinalShards = numShards / divisor;
        createIndexWithSettings(client(), index, alias, Settings.builder().put(SETTING_NUMBER_OF_SHARDS, numShards)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(ShardsLimitAllocationDecider.INDEX_TOTAL_SHARDS_PER_NODE_SETTING.getKey(), numShards - 2));
        createNewSingletonPolicy(client(), policy, "warm", new ShrinkAction(expectedFinalShards, null));
        updatePolicy(client(), index, policy);

        String shrunkenIndexName = waitAndGetShrinkIndexName(client(), index);
        assertBusy(() -> assertTrue(indexExists(shrunkenIndexName)), 60, TimeUnit.SECONDS);
        assertBusy(() -> assertTrue(aliasExists(shrunkenIndexName, index)));
        assertBusy(() -> assertThat(getStepKeyForIndex(client(), shrunkenIndexName),
            equalTo(PhaseCompleteStep.finalStep("warm").getKey())));
        assertBusy(() -> {
            Map<String, Object> settings = getOnlyIndexSettings(client(), shrunkenIndexName);
            assertThat(settings.get(SETTING_NUMBER_OF_SHARDS), equalTo(String.valueOf(expectedFinalShards)));
            assertThat(settings.get(IndexMetadata.INDEX_BLOCKS_WRITE_SETTING.getKey()), equalTo("true"));
            assertThat(settings.get(IndexMetadata.INDEX_ROUTING_REQUIRE_GROUP_SETTING.getKey() + "_id"), nullValue());
            assertThat(settings.get(ShardsLimitAllocationDecider.INDEX_TOTAL_SHARDS_PER_NODE_SETTING.getKey()), equalTo("-1"));
        });
        expectThrows(ResponseException.class, () -> indexDocument(client(), index));
    }
}
