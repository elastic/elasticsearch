/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ilm;

import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xpack.core.ilm.AllocateAction;
import org.elasticsearch.xpack.core.ilm.DeleteAction;
import org.elasticsearch.xpack.core.ilm.ErrorStep;
import org.elasticsearch.xpack.core.ilm.ForceMergeAction;
import org.elasticsearch.xpack.core.ilm.FreezeAction;
import org.elasticsearch.xpack.core.ilm.InitializePolicyContextStep;
import org.elasticsearch.xpack.core.ilm.LifecycleAction;
import org.elasticsearch.xpack.core.ilm.LifecyclePolicy;
import org.elasticsearch.xpack.core.ilm.LifecycleSettings;
import org.elasticsearch.xpack.core.ilm.Phase;
import org.elasticsearch.xpack.core.ilm.PhaseCompleteStep;
import org.elasticsearch.xpack.core.ilm.ReadOnlyAction;
import org.elasticsearch.xpack.core.ilm.RolloverAction;
import org.elasticsearch.xpack.core.ilm.SetPriorityAction;
import org.elasticsearch.xpack.core.ilm.ShrinkAction;
import org.elasticsearch.xpack.core.ilm.ShrinkStep;
import org.elasticsearch.xpack.core.ilm.Step;
import org.elasticsearch.xpack.core.ilm.Step.StepKey;
import org.elasticsearch.xpack.core.ilm.UpdateRolloverLifecycleDateStep;
import org.elasticsearch.xpack.core.ilm.WaitForActiveShardsStep;
import org.elasticsearch.xpack.core.ilm.WaitForRolloverReadyStep;
import org.elasticsearch.xpack.core.ilm.WaitForSnapshotAction;
import org.hamcrest.Matchers;
import org.junit.Before;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static java.util.Collections.singletonMap;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

public class TimeSeriesLifecycleActionsIT extends ESRestTestCase {
    private static final Logger logger = LogManager.getLogger(TimeSeriesLifecycleActionsIT.class);
    private static final String FAILED_STEP_RETRY_COUNT_FIELD = "failed_step_retry_count";
    private static final String IS_AUTO_RETRYABLE_ERROR_FIELD = "is_auto_retryable_error";

    private String index;
    private String policy;
    private String alias;

    @Before
    public void refreshIndex() {
        index = "index-" + randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        policy = "policy-" + randomAlphaOfLength(5);
        alias = "alias-" + randomAlphaOfLength(5);
    }

    public static void updatePolicy(String indexName, String policy) throws IOException {
        Request changePolicyRequest = new Request("PUT", "/" + indexName + "/_settings");
        final StringEntity changePolicyEntity = new StringEntity("{ \"index.lifecycle.name\": \"" + policy + "\" }",
                ContentType.APPLICATION_JSON);
        changePolicyRequest.setEntity(changePolicyEntity);
        assertOK(client().performRequest(changePolicyRequest));
    }

    public void testFullPolicy() throws Exception {
        String originalIndex = index + "-000001";
        String shrunkenOriginalIndex = ShrinkAction.SHRUNKEN_INDEX_PREFIX + originalIndex;
        String secondIndex = index + "-000002";
        createIndexWithSettings(originalIndex, Settings.builder().put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 4)
            .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0)
            .put("index.routing.allocation.include._name", "integTest-0")
            .put(RolloverAction.LIFECYCLE_ROLLOVER_ALIAS, alias));

        // create policy
        createFullPolicy(TimeValue.ZERO);
        // update policy on index
        updatePolicy(originalIndex, policy);
        // index document {"foo": "bar"} to trigger rollover
        index(client(), originalIndex, "_id", "foo", "bar");

        /*
         * These asserts are in the order that they should be satisfied in, in
         * order to maximize the time for all operations to complete.
         * An "out of order" assert here may result in this test occasionally
         * timing out and failing inappropriately.
         */
        // asserts that rollover was called
        assertBusy(() -> assertTrue(indexExists(secondIndex)));
        // asserts that shrink deleted the original index
        assertBusy(() -> assertFalse(indexExists(originalIndex)), 60, TimeUnit.SECONDS);
        // asserts that the delete phase completed for the managed shrunken index
        assertBusy(() -> assertFalse(indexExists(shrunkenOriginalIndex)));
    }

    public void testMoveToAllocateStep() throws Exception {
        String originalIndex = index + "-000001";
        createIndexWithSettings(originalIndex, Settings.builder().put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 4)
            .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0)
            .put("index.routing.allocation.include._name", "integTest-0")
            .put(RolloverAction.LIFECYCLE_ROLLOVER_ALIAS, "alias"));

        // create policy
        createFullPolicy(TimeValue.timeValueHours(10));
        // update policy on index
        updatePolicy(originalIndex, policy);

        // move to a step
        Request moveToStepRequest = new Request("POST", "_ilm/move/" + originalIndex);
        assertBusy(() -> assertTrue(getStepKeyForIndex(originalIndex).equals(new StepKey("new", "complete", "complete"))));
        moveToStepRequest.setJsonEntity("{\n" +
            "  \"current_step\": {\n" +
            "    \"phase\": \"new\",\n" +
            "    \"action\": \"complete\",\n" +
            "    \"name\": \"complete\"\n" +
            "  },\n" +
            "  \"next_step\": {\n" +
            "    \"phase\": \"cold\",\n" +
            "    \"action\": \"allocate\",\n" +
            "    \"name\": \"allocate\"\n" +
            "  }\n" +
            "}");
        client().performRequest(moveToStepRequest);
        assertBusy(() -> assertFalse(indexExists(originalIndex)));
    }


    public void testMoveToRolloverStep() throws Exception {
        String originalIndex = index + "-000001";
        String shrunkenOriginalIndex = ShrinkAction.SHRUNKEN_INDEX_PREFIX + originalIndex;
        String secondIndex = index + "-000002";
        createIndexWithSettings(originalIndex, Settings.builder().put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 4)
            .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0)
            .put("index.routing.allocation.include._name", "integTest-0")
            .put(RolloverAction.LIFECYCLE_ROLLOVER_ALIAS, alias));

        createFullPolicy(TimeValue.timeValueHours(10));
        // update policy on index
        updatePolicy(originalIndex, policy);

        // move to a step
        Request moveToStepRequest = new Request("POST", "_ilm/move/" + originalIndex);
        // index document to trigger rollover
        index(client(), originalIndex, "_id", "foo", "bar");
        logger.info(getStepKeyForIndex(originalIndex));
        moveToStepRequest.setJsonEntity("{\n" +
            "  \"current_step\": {\n" +
            "    \"phase\": \"new\",\n" +
            "    \"action\": \"complete\",\n" +
            "    \"name\": \"complete\"\n" +
            "  },\n" +
            "  \"next_step\": {\n" +
            "    \"phase\": \"hot\",\n" +
            "    \"action\": \"rollover\",\n" +
            "    \"name\": \"attempt-rollover\"\n" +
            "  }\n" +
            "}");
        client().performRequest(moveToStepRequest);

        /*
         * These asserts are in the order that they should be satisfied in, in
         * order to maximize the time for all operations to complete.
         * An "out of order" assert here may result in this test occasionally
         * timing out and failing inappropriately.
         */
        // asserts that rollover was called
        assertBusy(() -> assertTrue(indexExists(secondIndex)));
        // asserts that shrink deleted the original index
        assertBusy(() -> assertFalse(indexExists(originalIndex)), 30, TimeUnit.SECONDS);
        // asserts that the delete phase completed for the managed shrunken index
        assertBusy(() -> assertFalse(indexExists(shrunkenOriginalIndex)));
    }

    public void testRetryFailedShrinkAction() throws Exception {
        int numShards = 4;
        int divisor = randomFrom(2, 4);
        int expectedFinalShards = numShards / divisor;
        String shrunkenIndex = ShrinkAction.SHRUNKEN_INDEX_PREFIX + index;
        createIndexWithSettings(index, Settings.builder().put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, numShards)
            .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0));
        createNewSingletonPolicy("warm", new ShrinkAction(numShards + randomIntBetween(1, numShards)));
        updatePolicy(index, policy);
        assertBusy(() -> {
            String failedStep = getFailedStepForIndex(index);
            assertThat(failedStep, equalTo(ShrinkStep.NAME));
        });

        // update policy to be correct
        createNewSingletonPolicy("warm", new ShrinkAction(expectedFinalShards));
        updatePolicy(index, policy);

        // retry step
        Request retryRequest = new Request("POST", index + "/_ilm/retry");
        assertOK(client().performRequest(retryRequest));

        // assert corrected policy is picked up and index is shrunken
        assertBusy(() -> assertTrue(indexExists(shrunkenIndex)), 30, TimeUnit.SECONDS);
        assertBusy(() -> assertTrue(aliasExists(shrunkenIndex, index)));
        assertBusy(() -> assertThat(getStepKeyForIndex(shrunkenIndex), equalTo(PhaseCompleteStep.finalStep("warm").getKey())));
        assertBusy(() -> {
            Map<String, Object> settings = getOnlyIndexSettings(shrunkenIndex);
            assertThat(settings.get(IndexMetaData.SETTING_NUMBER_OF_SHARDS), equalTo(String.valueOf(expectedFinalShards)));
            assertThat(settings.get(IndexMetaData.INDEX_BLOCKS_WRITE_SETTING.getKey()), equalTo("true"));
            assertThat(settings.get(IndexMetaData.INDEX_ROUTING_REQUIRE_GROUP_SETTING.getKey() + "_id"), nullValue());
        });
        expectThrows(ResponseException.class, this::indexDocument);
    }

    public void testRolloverAction() throws Exception {
        String originalIndex = index + "-000001";
        String secondIndex = index + "-000002";
        createIndexWithSettings(originalIndex, Settings.builder().put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(RolloverAction.LIFECYCLE_ROLLOVER_ALIAS, alias));

        // create policy
        createNewSingletonPolicy("hot", new RolloverAction(null, null, 1L));
        // update policy on index
        updatePolicy(originalIndex, policy);
        // index document {"foo": "bar"} to trigger rollover
        index(client(), originalIndex, "_id", "foo", "bar");
        assertBusy(() -> assertTrue(indexExists(secondIndex)));
        assertBusy(() -> assertTrue(indexExists(originalIndex)));
        assertBusy(() -> assertEquals("true", getOnlyIndexSettings(originalIndex).get(LifecycleSettings.LIFECYCLE_INDEXING_COMPLETE)));
    }

    public void testRolloverActionWithIndexingComplete() throws Exception {
        String originalIndex = index + "-000001";
        String secondIndex = index + "-000002";
        createIndexWithSettings(originalIndex, Settings.builder().put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(RolloverAction.LIFECYCLE_ROLLOVER_ALIAS, alias));

        Request updateSettingsRequest = new Request("PUT", "/" + originalIndex + "/_settings");
        updateSettingsRequest.setJsonEntity("{\n" +
            "  \"settings\": {\n" +
            "    \"" + LifecycleSettings.LIFECYCLE_INDEXING_COMPLETE + "\": true\n" +
            "  }\n" +
            "}");
        client().performRequest(updateSettingsRequest);
        Request updateAliasRequest = new Request("POST", "/_aliases");
        updateAliasRequest.setJsonEntity("{\n" +
            "  \"actions\": [\n" +
            "    {\n" +
            "      \"add\": {\n" +
            "        \"index\": \"" + originalIndex + "\",\n" +
            "        \"alias\": \"" + alias + "\",\n" +
            "        \"is_write_index\": false\n" +
            "      }\n" +
            "    }\n" +
            "  ]\n" +
            "}");
        client().performRequest(updateAliasRequest);

        // create policy
        createNewSingletonPolicy("hot", new RolloverAction(null, null, 1L));
        // update policy on index
        updatePolicy(originalIndex, policy);
        // index document {"foo": "bar"} to trigger rollover
        index(client(), originalIndex, "_id", "foo", "bar");
        assertBusy(() -> assertThat(getStepKeyForIndex(originalIndex), equalTo(PhaseCompleteStep.finalStep("hot").getKey())));
        assertBusy(() -> assertTrue(indexExists(originalIndex)));
        assertBusy(() -> assertFalse(indexExists(secondIndex)));
        assertBusy(() -> assertEquals("true", getOnlyIndexSettings(originalIndex).get(LifecycleSettings.LIFECYCLE_INDEXING_COMPLETE)));
    }

    public void testAllocateOnlyAllocation() throws Exception {
        createIndexWithSettings(index, Settings.builder().put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 2)
            .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0));
        String allocateNodeName = "integTest-" + randomFrom(0, 1);
        AllocateAction allocateAction = new AllocateAction(null, null, null, singletonMap("_name", allocateNodeName));
        String endPhase = randomFrom("warm", "cold");
        createNewSingletonPolicy(endPhase, allocateAction);
        updatePolicy(index, policy);
        assertBusy(() -> {
            assertThat(getStepKeyForIndex(index), equalTo(PhaseCompleteStep.finalStep(endPhase).getKey()));
        });
        ensureGreen(index);
    }

    public void testAllocateActionOnlyReplicas() throws Exception {
        int numShards = randomFrom(1, 5);
        int numReplicas = randomFrom(0, 1);
        int finalNumReplicas = (numReplicas + 1) % 2;
        createIndexWithSettings(index, Settings.builder().put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, numShards)
            .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, numReplicas));
        AllocateAction allocateAction = new AllocateAction(finalNumReplicas, null, null, null);
        String endPhase = randomFrom("warm", "cold");
        createNewSingletonPolicy(endPhase, allocateAction);
        updatePolicy(index, policy);
        assertBusy(() -> {
            Map<String, Object> settings = getOnlyIndexSettings(index);
            assertThat(getStepKeyForIndex(index), equalTo(PhaseCompleteStep.finalStep(endPhase).getKey()));
            assertThat(settings.get(IndexMetaData.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey()), equalTo(String.valueOf(finalNumReplicas)));
        });
    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/50781")
    public void testWaitForSnapshot() throws Exception {
        createIndexWithSettings(index, Settings.builder().put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0));
        String smlPolicy = randomAlphaOfLengthBetween(4, 10);
        createNewSingletonPolicy("delete", new WaitForSnapshotAction(smlPolicy));
        updatePolicy(index, policy);
        assertBusy(() -> assertThat(getStepKeyForIndex(index).getAction(), equalTo("wait_for_snapshot")));
        assertBusy(() -> assertThat(getFailedStepForIndex(index), equalTo("wait-for-snapshot")));

        String repo = createSnapshotRepo();
        createSlmPolicy(smlPolicy, repo);

        assertBusy(() -> assertThat(getStepKeyForIndex(index).getAction(), equalTo("wait_for_snapshot")));

        Request request = new Request("PUT", "/_slm/policy/" + smlPolicy + "/_execute");
        assertOK(client().performRequest(request));


        assertBusy(() -> assertThat(getStepKeyForIndex(index).getAction(), equalTo("completed")), 2, TimeUnit.MINUTES);

        request = new Request("DELETE", "/_slm/policy/" + smlPolicy);
        assertOK(client().performRequest(request));

        request = new Request("DELETE", "/_snapshot/" + repo);
        assertOK(client().performRequest(request));
    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/50781")
    public void testWaitForSnapshotSlmExecutedBefore() throws Exception {
        createIndexWithSettings(index, Settings.builder().put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0));
        String smlPolicy = randomAlphaOfLengthBetween(4, 10);
        createNewSingletonPolicy("delete", new WaitForSnapshotAction(smlPolicy));

        String repo = createSnapshotRepo();
        createSlmPolicy(smlPolicy, repo);

        Request request = new Request("PUT", "/_slm/policy/" + smlPolicy + "/_execute");
        assertOK(client().performRequest(request));

        updatePolicy(index, policy);
        assertBusy(() -> assertThat(getStepKeyForIndex(index).getAction(), equalTo("wait_for_snapshot")));
        assertBusy(() -> assertThat(getStepKeyForIndex(index).getName(), equalTo("wait-for-snapshot")));

        request = new Request("PUT", "/_slm/policy/" + smlPolicy + "/_execute");
        assertOK(client().performRequest(request));

        request = new Request("PUT", "/_slm/policy/" + smlPolicy + "/_execute");
        assertOK(client().performRequest(request));

        assertBusy(() -> assertThat(getStepKeyForIndex(index).getAction(), equalTo("completed")), 2, TimeUnit.MINUTES);

        request = new Request("DELETE", "/_slm/policy/" + smlPolicy);
        assertOK(client().performRequest(request));

        request = new Request("DELETE", "/_snapshot/" + repo);
        assertOK(client().performRequest(request));
    }

    public void testDelete() throws Exception {
        createIndexWithSettings(index, Settings.builder().put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0));
        createNewSingletonPolicy("delete", new DeleteAction());
        updatePolicy(index, policy);
        assertBusy(() -> assertFalse(indexExists(index)));
    }

    public void testDeleteOnlyShouldNotMakeIndexReadonly() throws Exception {
        createIndexWithSettings(index, Settings.builder().put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0));
        createNewSingletonPolicy("delete", new DeleteAction(), TimeValue.timeValueHours(1));
        updatePolicy(index, policy);
        assertBusy(() -> {
            assertThat(getStepKeyForIndex(index).getAction(), equalTo("complete"));
            Map<String, Object> settings = getOnlyIndexSettings(index);
            assertThat(settings.get(IndexMetaData.INDEX_BLOCKS_WRITE_SETTING.getKey()), not("true"));
        });
        indexDocument();
    }

    public void testDeleteDuringSnapshot() throws Exception {
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
        createNewSingletonPolicy("delete", new DeleteAction(), TimeValue.timeValueMillis(0));
        // create index without policy
        createIndexWithSettings(index, Settings.builder().put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0));
        // index document so snapshot actually does something
        indexDocument();
        // start snapshot
        String snapName = "snapshot-" + randomAlphaOfLength(6).toLowerCase(Locale.ROOT);
        request = new Request("PUT", "/_snapshot/repo/" + snapName);
        request.addParameter("wait_for_completion", "false");
        request.setJsonEntity("{\"indices\": \"" + index + "\"}");
        assertOK(client().performRequest(request));
        // add policy and expect it to trigger delete immediately (while snapshot in progress)
        updatePolicy(index, policy);
        // assert that index was deleted
        assertBusy(() -> assertFalse(indexExists(index)), 2, TimeUnit.MINUTES);
        // assert that snapshot is still in progress and clean up
        assertThat(getSnapshotState(snapName), equalTo("SUCCESS"));
        assertOK(client().performRequest(new Request("DELETE", "/_snapshot/repo/" + snapName)));
    }

    public void testReadOnly() throws Exception {
        createIndexWithSettings(index, Settings.builder().put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0));
        createNewSingletonPolicy("warm", new ReadOnlyAction());
        updatePolicy(index, policy);
        assertBusy(() -> {
            Map<String, Object> settings = getOnlyIndexSettings(index);
            assertThat(getStepKeyForIndex(index), equalTo(PhaseCompleteStep.finalStep("warm").getKey()));
            assertThat(settings.get(IndexMetaData.INDEX_BLOCKS_WRITE_SETTING.getKey()), equalTo("true"));
        });
    }

    @SuppressWarnings("unchecked")
    public void testForceMergeAction() throws Exception {
        createIndexWithSettings(index, Settings.builder().put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0));
        for (int i = 0; i < randomIntBetween(2, 10); i++) {
            Request request = new Request("PUT", index + "/_doc/" + i);
            request.addParameter("refresh", "true");
            request.setEntity(new StringEntity("{\"a\": \"test\"}", ContentType.APPLICATION_JSON));
            client().performRequest(request);
        }

        Supplier<Integer> numSegments = () -> {
            try {
                Map<String, Object> segmentResponse = getAsMap(index + "/_segments");
                segmentResponse = (Map<String, Object>) segmentResponse.get("indices");
                segmentResponse = (Map<String, Object>) segmentResponse.get(index);
                segmentResponse = (Map<String, Object>) segmentResponse.get("shards");
                List<Map<String, Object>> shards = (List<Map<String, Object>>) segmentResponse.get("0");
                return (Integer) shards.get(0).get("num_search_segments");
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        };
        assertThat(numSegments.get(), greaterThan(1));

        createNewSingletonPolicy("warm", new ForceMergeAction(1));
        updatePolicy(index, policy);

        assertBusy(() -> {
            assertThat(getStepKeyForIndex(index), equalTo(PhaseCompleteStep.finalStep("warm").getKey()));
            Map<String, Object> settings = getOnlyIndexSettings(index);
            assertThat(numSegments.get(), equalTo(1));
            assertThat(settings.get(IndexMetaData.INDEX_BLOCKS_WRITE_SETTING.getKey()), equalTo("true"));
        });
        expectThrows(ResponseException.class, this::indexDocument);
    }

    public void testShrinkAction() throws Exception {
        int numShards = 4;
        int divisor = randomFrom(2, 4);
        int expectedFinalShards = numShards / divisor;
        String shrunkenIndex = ShrinkAction.SHRUNKEN_INDEX_PREFIX + index;
        createIndexWithSettings(index, Settings.builder().put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, numShards)
            .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0));
        createNewSingletonPolicy("warm", new ShrinkAction(expectedFinalShards));
        updatePolicy(index, policy);
        assertBusy(() -> assertTrue(indexExists(shrunkenIndex)), 30, TimeUnit.SECONDS);
        assertBusy(() -> assertTrue(aliasExists(shrunkenIndex, index)));
        assertBusy(() -> assertThat(getStepKeyForIndex(shrunkenIndex), equalTo(PhaseCompleteStep.finalStep("warm").getKey())));
        assertBusy(() -> {
            Map<String, Object> settings = getOnlyIndexSettings(shrunkenIndex);
            assertThat(settings.get(IndexMetaData.SETTING_NUMBER_OF_SHARDS), equalTo(String.valueOf(expectedFinalShards)));
            assertThat(settings.get(IndexMetaData.INDEX_BLOCKS_WRITE_SETTING.getKey()), equalTo("true"));
            assertThat(settings.get(IndexMetaData.INDEX_ROUTING_REQUIRE_GROUP_SETTING.getKey() + "_id"), nullValue());
        });
        expectThrows(ResponseException.class, this::indexDocument);
    }

    public void testShrinkSameShards() throws Exception {
        int numberOfShards = randomFrom(1, 2);
        String shrunkenIndex = ShrinkAction.SHRUNKEN_INDEX_PREFIX + index;
        createIndexWithSettings(index, Settings.builder().put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, numberOfShards)
            .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0));
        createNewSingletonPolicy("warm", new ShrinkAction(numberOfShards));
        updatePolicy(index, policy);
        assertBusy(() -> {
            assertTrue(indexExists(index));
            assertFalse(indexExists(shrunkenIndex));
            assertFalse(aliasExists(shrunkenIndex, index));
            Map<String, Object> settings = getOnlyIndexSettings(index);
            assertThat(getStepKeyForIndex(index), equalTo(PhaseCompleteStep.finalStep("warm").getKey()));
            assertThat(settings.get(IndexMetaData.SETTING_NUMBER_OF_SHARDS), equalTo(String.valueOf(numberOfShards)));
            assertNull(settings.get(IndexMetaData.INDEX_BLOCKS_WRITE_SETTING.getKey()));
            assertThat(settings.get(IndexMetaData.INDEX_ROUTING_REQUIRE_GROUP_SETTING.getKey() + "_id"), nullValue());
        });
    }

    public void testShrinkDuringSnapshot() throws Exception {
        String shrunkenIndex = ShrinkAction.SHRUNKEN_INDEX_PREFIX + index;
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
        createNewSingletonPolicy("warm", new ShrinkAction(1), TimeValue.timeValueMillis(0));
        // create index without policy
        createIndexWithSettings(index, Settings.builder()
            .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 2)
            .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0)
            // required so the shrink doesn't wait on SetSingleNodeAllocateStep
            .put(IndexMetaData.INDEX_ROUTING_REQUIRE_GROUP_SETTING.getKey() + "_name", "integTest-0"));
        // index document so snapshot actually does something
        indexDocument();
        // start snapshot
        request = new Request("PUT", "/_snapshot/repo/snapshot");
        request.addParameter("wait_for_completion", "false");
        request.setJsonEntity("{\"indices\": \"" + index + "\"}");
        assertOK(client().performRequest(request));
        // add policy and expect it to trigger shrink immediately (while snapshot in progress)
        updatePolicy(index, policy);
        // assert that index was shrunk and original index was deleted
        assertBusy(() -> {
            assertTrue(indexExists(shrunkenIndex));
            assertTrue(aliasExists(shrunkenIndex, index));
            Map<String, Object> settings = getOnlyIndexSettings(shrunkenIndex);
            assertThat(getStepKeyForIndex(shrunkenIndex), equalTo(PhaseCompleteStep.finalStep("warm").getKey()));
            assertThat(settings.get(IndexMetaData.SETTING_NUMBER_OF_SHARDS), equalTo(String.valueOf(1)));
            assertThat(settings.get(IndexMetaData.INDEX_BLOCKS_WRITE_SETTING.getKey()), equalTo("true"));
            assertThat(settings.get(IndexMetaData.INDEX_ROUTING_REQUIRE_GROUP_SETTING.getKey() + "_id"), nullValue());
        }, 2, TimeUnit.MINUTES);
        expectThrows(ResponseException.class, this::indexDocument);
        // assert that snapshot succeeded
        assertThat(getSnapshotState("snapshot"), equalTo("SUCCESS"));
        assertOK(client().performRequest(new Request("DELETE", "/_snapshot/repo/snapshot")));
    }

    public void testFreezeAction() throws Exception {
        createIndexWithSettings(index, Settings.builder().put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0));
        createNewSingletonPolicy("cold", new FreezeAction());
        updatePolicy(index, policy);
        assertBusy(() -> {
            Map<String, Object> settings = getOnlyIndexSettings(index);
            assertThat(getStepKeyForIndex(index), equalTo(PhaseCompleteStep.finalStep("cold").getKey()));
            assertThat(settings.get(IndexMetaData.INDEX_BLOCKS_WRITE_SETTING.getKey()), equalTo("true"));
            assertThat(settings.get(IndexSettings.INDEX_SEARCH_THROTTLED.getKey()), equalTo("true"));
            assertThat(settings.get("index.frozen"), equalTo("true"));
        });
    }

    public void testFreezeDuringSnapshot() throws Exception {
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
        createNewSingletonPolicy("cold", new FreezeAction(), TimeValue.timeValueMillis(0));
        // create index without policy
        createIndexWithSettings(index, Settings.builder().put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0));
        // index document so snapshot actually does something
        indexDocument();
        // start snapshot
        request = new Request("PUT", "/_snapshot/repo/snapshot");
        request.addParameter("wait_for_completion", "false");
        request.setJsonEntity("{\"indices\": \"" + index + "\"}");
        assertOK(client().performRequest(request));
        // add policy and expect it to trigger delete immediately (while snapshot in progress)
        updatePolicy(index, policy);
        // assert that the index froze
        assertBusy(() -> {
            Map<String, Object> settings = getOnlyIndexSettings(index);
            assertThat(getStepKeyForIndex(index), equalTo(PhaseCompleteStep.finalStep("cold").getKey()));
            assertThat(settings.get(IndexMetaData.INDEX_BLOCKS_WRITE_SETTING.getKey()), equalTo("true"));
            assertThat(settings.get(IndexSettings.INDEX_SEARCH_THROTTLED.getKey()), equalTo("true"));
            assertThat(settings.get("index.frozen"), equalTo("true"));
        }, 2, TimeUnit.MINUTES);
        // assert that snapshot is still in progress and clean up
        assertThat(getSnapshotState("snapshot"), equalTo("SUCCESS"));
        assertOK(client().performRequest(new Request("DELETE", "/_snapshot/repo/snapshot")));
    }

    public void testSetPriority() throws Exception {
        createIndexWithSettings(index, Settings.builder().put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0).put(IndexMetaData.INDEX_PRIORITY_SETTING.getKey(), 100));
        int priority = randomIntBetween(0, 99);
        createNewSingletonPolicy("warm", new SetPriorityAction(priority));
        updatePolicy(index, policy);
        assertBusy(() -> {
            Map<String, Object> settings = getOnlyIndexSettings(index);
            assertThat(getStepKeyForIndex(index), equalTo(PhaseCompleteStep.finalStep("warm").getKey()));
            assertThat(settings.get(IndexMetaData.INDEX_PRIORITY_SETTING.getKey()), equalTo(String.valueOf(priority)));
        });
    }

    public void testSetNullPriority() throws Exception {
        createIndexWithSettings(index, Settings.builder().put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0).put(IndexMetaData.INDEX_PRIORITY_SETTING.getKey(), 100));
        createNewSingletonPolicy("warm", new SetPriorityAction((Integer) null));
        updatePolicy(index, policy);
        assertBusy(() -> {
            Map<String, Object> settings = getOnlyIndexSettings(index);
            assertThat(getStepKeyForIndex(index), equalTo(PhaseCompleteStep.finalStep("warm").getKey()));
            assertNull(settings.get(IndexMetaData.INDEX_PRIORITY_SETTING.getKey()));
        });
    }

    @SuppressWarnings("unchecked")
    public void testNonexistentPolicy() throws Exception {
        String indexPrefix = randomAlphaOfLengthBetween(5,15).toLowerCase(Locale.ROOT);
        final StringEntity template = new StringEntity("{\n" +
            "  \"index_patterns\": \"" + indexPrefix + "*\",\n" +
            "  \"settings\": {\n" +
            "    \"index\": {\n" +
            "      \"lifecycle\": {\n" +
            "        \"name\": \"does_not_exist\",\n" +
            "        \"rollover_alias\": \"test_alias\"\n" +
            "      }\n" +
            "    }\n" +
            "  }\n" +
            "}", ContentType.APPLICATION_JSON);
        Request templateRequest = new Request("PUT", "_template/test");
        templateRequest.setEntity(template);
        client().performRequest(templateRequest);

        policy = randomAlphaOfLengthBetween(5,20);
        createNewSingletonPolicy("hot", new RolloverAction(null, null, 1L));

        index = indexPrefix + "-000001";
        final StringEntity putIndex = new StringEntity("{\n" +
            "  \"aliases\": {\n" +
            "    \"test_alias\": {\n" +
            "      \"is_write_index\": true\n" +
            "    }\n" +
            "  }\n" +
            "}", ContentType.APPLICATION_JSON);
        Request putIndexRequest = new Request("PUT", index);
        putIndexRequest.setEntity(putIndex);
        client().performRequest(putIndexRequest);
        indexDocument();

        assertBusy(() -> {
            Request explainRequest = new Request("GET", index + "/_ilm/explain");
            Response response = client().performRequest(explainRequest);
            Map<String, Object> responseMap;
            try (InputStream is = response.getEntity().getContent()) {
                responseMap = XContentHelper.convertToMap(XContentType.JSON.xContent(), is, true);
            }
            logger.info(responseMap);
            Map<String, Object> indexStatus = (Map<String, Object>)((Map<String, Object>) responseMap.get("indices")).get(index);
            assertNull(indexStatus.get("phase"));
            assertNull(indexStatus.get("action"));
            assertNull(indexStatus.get("step"));
            Map<String, String> stepInfo = (Map<String, String>) indexStatus.get("step_info");
            assertNotNull(stepInfo);
            assertEquals("policy [does_not_exist] does not exist", stepInfo.get("reason"));
            assertEquals("illegal_argument_exception", stepInfo.get("type"));
        });
    }

    public void testInvalidPolicyNames() {
        ResponseException ex;

        policy = randomAlphaOfLengthBetween(0,10) + "," + randomAlphaOfLengthBetween(0,10);
        ex = expectThrows(ResponseException.class, () -> createNewSingletonPolicy("delete", new DeleteAction()));
        assertThat(ex.getMessage(), containsString("invalid policy name"));

        policy = randomAlphaOfLengthBetween(0,10) + "%20" + randomAlphaOfLengthBetween(0,10);
        ex = expectThrows(ResponseException.class, () -> createNewSingletonPolicy("delete", new DeleteAction()));
        assertThat(ex.getMessage(), containsString("invalid policy name"));

        policy = "_" + randomAlphaOfLengthBetween(1, 20);
        ex = expectThrows(ResponseException.class, () -> createNewSingletonPolicy("delete", new DeleteAction()));
        assertThat(ex.getMessage(), containsString("invalid policy name"));

        policy = randomAlphaOfLengthBetween(256, 1000);
        ex = expectThrows(ResponseException.class, () -> createNewSingletonPolicy("delete", new DeleteAction()));
        assertThat(ex.getMessage(), containsString("invalid policy name"));
    }

    public void testDeletePolicyInUse() throws IOException {
        String managedIndex1 = randomAlphaOfLength(7).toLowerCase(Locale.ROOT);
        String managedIndex2 = randomAlphaOfLength(8).toLowerCase(Locale.ROOT);
        String unmanagedIndex = randomAlphaOfLength(9).toLowerCase(Locale.ROOT);
        String managedByOtherPolicyIndex = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);

        createNewSingletonPolicy("delete", new DeleteAction(), TimeValue.timeValueHours(12));
        String originalPolicy = policy;
        String otherPolicy = randomValueOtherThan(policy, () -> randomAlphaOfLength(5));
        policy = otherPolicy;
        createNewSingletonPolicy("delete", new DeleteAction(), TimeValue.timeValueHours(13));

        createIndexWithSettingsNoAlias(managedIndex1, Settings.builder()
            .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, randomIntBetween(1,10))
            .put(LifecycleSettings.LIFECYCLE_NAME_SETTING.getKey(), originalPolicy));
        createIndexWithSettingsNoAlias(managedIndex2, Settings.builder()
            .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, randomIntBetween(1,10))
            .put(LifecycleSettings.LIFECYCLE_NAME_SETTING.getKey(), originalPolicy));
        createIndexWithSettingsNoAlias(unmanagedIndex, Settings.builder()
            .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, randomIntBetween(1,10)));
        createIndexWithSettingsNoAlias(managedByOtherPolicyIndex, Settings.builder()
            .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, randomIntBetween(1,10))
            .put(LifecycleSettings.LIFECYCLE_NAME_SETTING.getKey(), otherPolicy));

        Request deleteRequest = new Request("DELETE", "_ilm/policy/" + originalPolicy);
        ResponseException ex = expectThrows(ResponseException.class, () -> client().performRequest(deleteRequest));
        assertThat(ex.getMessage(),
            Matchers.allOf(
                containsString("Cannot delete policy [" + originalPolicy + "]. It is in use by one or more indices: ["),
                containsString(managedIndex1),
                containsString(managedIndex2),
                not(containsString(unmanagedIndex)),
                not(containsString(managedByOtherPolicyIndex))));
    }

    public void testRemoveAndReaddPolicy() throws Exception {
        String originalIndex = index + "-000001";
        String secondIndex = index + "-000002";
        // Set up a policy with rollover
        createNewSingletonPolicy("hot", new RolloverAction(null, null, 1L));
        createIndexWithSettings(
            originalIndex,
            Settings.builder().put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0)
                .put(LifecycleSettings.LIFECYCLE_NAME, policy)
                .put(RolloverAction.LIFECYCLE_ROLLOVER_ALIAS, alias));

        // Index a document
        index(client(), originalIndex, "_id", "foo", "bar");

        // Wait for rollover to happen
        assertBusy(() -> assertTrue(indexExists(secondIndex)));

        // Remove the policy from the original index
        Request removeRequest = new Request("POST", "/" + originalIndex + "/_ilm/remove");
        removeRequest.setJsonEntity("");
        client().performRequest(removeRequest);

        // Add the policy again
        Request addPolicyRequest = new Request("PUT", "/" + originalIndex + "/_settings");
        addPolicyRequest.setJsonEntity("{\n" +
            "  \"settings\": {\n" +
            "    \"index.lifecycle.name\": \"" + policy + "\",\n" +
            "    \"index.lifecycle.rollover_alias\": \"" + alias + "\"\n" +
            "  }\n" +
            "}");
        client().performRequest(addPolicyRequest);
        assertBusy(() -> assertTrue((boolean) explainIndex(originalIndex).getOrDefault("managed", false)));

        // Wait for everything to be copacetic
        assertBusy(() -> assertThat(getStepKeyForIndex(originalIndex), equalTo(PhaseCompleteStep.finalStep("hot").getKey())));
    }

    public void testMoveToInjectedStep() throws Exception {
        String shrunkenIndex = ShrinkAction.SHRUNKEN_INDEX_PREFIX + index;
        createNewSingletonPolicy("warm", new ShrinkAction(1), TimeValue.timeValueHours(12));

        createIndexWithSettings(index, Settings.builder().put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 3)
            .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(LifecycleSettings.LIFECYCLE_NAME, policy)
            .put(RolloverAction.LIFECYCLE_ROLLOVER_ALIAS, alias));

        assertBusy(() -> assertThat(getStepKeyForIndex(index), equalTo(new StepKey("new", "complete", "complete"))));

        // Move to a step from the injected unfollow action
        Request moveToStepRequest = new Request("POST", "_ilm/move/" + index);
        moveToStepRequest.setJsonEntity("{\n" +
            "  \"current_step\": { \n" +
            "    \"phase\": \"new\",\n" +
            "    \"action\": \"complete\",\n" +
            "    \"name\": \"complete\"\n" +
            "  },\n" +
            "  \"next_step\": { \n" +
            "    \"phase\": \"warm\",\n" +
            "    \"action\": \"unfollow\",\n" +
            "    \"name\": \"wait-for-indexing-complete\"\n" +
            "  }\n" +
            "}");
        // If we get an OK on this request we have successfully moved to the injected step
        assertOK(client().performRequest(moveToStepRequest));

        // Make sure we actually move on to and execute the shrink action
        assertBusy(() -> {
            assertTrue(indexExists(shrunkenIndex));
            assertTrue(aliasExists(shrunkenIndex, index));
            assertThat(getStepKeyForIndex(shrunkenIndex), equalTo(PhaseCompleteStep.finalStep("warm").getKey()));
        });
    }

    public void testMoveToStepRereadsPolicy() throws Exception {
        createNewSingletonPolicy("hot", new RolloverAction(null, TimeValue.timeValueHours(1), null), TimeValue.ZERO);

        createIndexWithSettings("test-1", Settings.builder()
                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0)
                .put(LifecycleSettings.LIFECYCLE_NAME, policy)
                .put(RolloverAction.LIFECYCLE_ROLLOVER_ALIAS, alias),
            true);

        assertBusy(() -> assertThat(getStepKeyForIndex("test-1"), equalTo(new StepKey("hot", "rollover", "check-rollover-ready"))));

        createNewSingletonPolicy("hot", new RolloverAction(null, TimeValue.timeValueSeconds(1), null), TimeValue.ZERO);

        // Move to the same step, which should re-read the policy
        Request moveToStepRequest = new Request("POST", "_ilm/move/test-1");
        moveToStepRequest.setJsonEntity("{\n" +
            "  \"current_step\": { \n" +
            "    \"phase\": \"hot\",\n" +
            "    \"action\": \"rollover\",\n" +
            "    \"name\": \"check-rollover-ready\"\n" +
            "  },\n" +
            "  \"next_step\": { \n" +
            "    \"phase\": \"hot\",\n" +
            "    \"action\": \"rollover\",\n" +
            "    \"name\": \"check-rollover-ready\"\n" +
            "  }\n" +
            "}");
        assertOK(client().performRequest(moveToStepRequest));

        // Make sure we actually rolled over
        assertBusy(() -> {
            indexExists("test-000002");
        });
    }

    public void testCanStopILMWithPolicyUsingNonexistentPolicy() throws Exception {
        createIndexWithSettings(index, Settings.builder().put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(LifecycleSettings.LIFECYCLE_NAME_SETTING.getKey(), randomAlphaOfLengthBetween(5,15)));

        Request stopILMRequest = new Request("POST", "_ilm/stop");
        assertOK(client().performRequest(stopILMRequest));

        Request statusRequest = new Request("GET", "_ilm/status");
        assertBusy(() -> {
            Response statusResponse = client().performRequest(statusRequest);
            assertOK(statusResponse);
            Map<String, Object> statusResponseMap = entityAsMap(statusResponse);
            String status = (String) statusResponseMap.get("operation_mode");
            assertEquals("STOPPED", status);
        });

        // Re-start ILM so that subsequent tests don't fail
        Request startILMReqest = new Request("POST", "_ilm/start");
        assertOK(client().performRequest(startILMReqest));
    }

    public void testExplainFilters() throws Exception {
        String goodIndex = index + "-good-000001";
        String errorIndex = index + "-error";
        String nonexistantPolicyIndex = index + "-nonexistant-policy";
        String unmanagedIndex = index + "-unmanaged";

        createFullPolicy(TimeValue.ZERO);

        {
            // Create a "shrink-only-policy"
            Map<String, LifecycleAction> warmActions = new HashMap<>();
            warmActions.put(ShrinkAction.NAME, new ShrinkAction(17));
            Map<String, Phase> phases = new HashMap<>();
            phases.put("warm", new Phase("warm", TimeValue.ZERO, warmActions));
            LifecyclePolicy lifecyclePolicy = new LifecyclePolicy("shrink-only-policy", phases);
            // PUT policy
            XContentBuilder builder = jsonBuilder();
            lifecyclePolicy.toXContent(builder, null);
            final StringEntity entity = new StringEntity(
                "{ \"policy\":" + Strings.toString(builder) + "}", ContentType.APPLICATION_JSON);
            Request request = new Request("PUT", "_ilm/policy/shrink-only-policy");
            request.setEntity(entity);
            assertOK(client().performRequest(request));
        }

        createIndexWithSettings(goodIndex, Settings.builder()
            .put(RolloverAction.LIFECYCLE_ROLLOVER_ALIAS, alias)
            .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(LifecycleSettings.LIFECYCLE_NAME, policy)
        );
        createIndexWithSettingsNoAlias(errorIndex, Settings.builder()
            .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(LifecycleSettings.LIFECYCLE_NAME, "shrink-only-policy")
        );
        createIndexWithSettingsNoAlias(nonexistantPolicyIndex, Settings.builder()
            .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(LifecycleSettings.LIFECYCLE_NAME, randomValueOtherThan(policy, () -> randomAlphaOfLengthBetween(3,10))));
        createIndexWithSettingsNoAlias(unmanagedIndex, Settings.builder()
            .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0));

        assertBusy(() -> {
            Map<String, Map<String, Object>> explainResponse = explain(index + "*", false, false);
            assertNotNull(explainResponse);
            assertThat(explainResponse,
                allOf(hasKey(goodIndex), hasKey(errorIndex), hasKey(nonexistantPolicyIndex), hasKey(unmanagedIndex)));

            Map<String, Map<String, Object>> onlyManagedResponse = explain(index + "*", false, true);
            assertNotNull(onlyManagedResponse);
            assertThat(onlyManagedResponse, allOf(hasKey(goodIndex), hasKey(errorIndex), hasKey(nonexistantPolicyIndex)));
            assertThat(onlyManagedResponse, not(hasKey(unmanagedIndex)));

            Map<String, Map<String, Object>> onlyErrorsResponse = explain(index + "*", true, true);
            assertNotNull(onlyErrorsResponse);
            assertThat(onlyErrorsResponse, allOf(hasKey(errorIndex), hasKey(nonexistantPolicyIndex)));
            assertThat(onlyErrorsResponse, allOf(not(hasKey(goodIndex)), not(hasKey(unmanagedIndex))));
        });
    }

    public void testExplainIndexContainsAutomaticRetriesInformation() throws Exception {
        createFullPolicy(TimeValue.ZERO);

        // create index without alias so the rollover action fails and is retried
        createIndexWithSettingsNoAlias(index, Settings.builder()
            .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(LifecycleSettings.LIFECYCLE_NAME, policy)
        );

        assertBusy(() -> {
            Map<String, Object> explainIndex = explainIndex(index);
            assertThat((Integer) explainIndex.get(FAILED_STEP_RETRY_COUNT_FIELD), greaterThanOrEqualTo(1));
            assertThat(explainIndex.get(IS_AUTO_RETRYABLE_ERROR_FIELD), is(true));
        });
    }

    public void testILMRolloverRetriesOnReadOnlyBlock() throws Exception {
        String firstIndex = index + "-000001";

        createNewSingletonPolicy("hot", new RolloverAction(null, TimeValue.timeValueSeconds(1), null));

        // create the index as readonly and associate the ILM policy to it
        createIndexWithSettings(
            firstIndex,
            Settings.builder().put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0)
                .put(LifecycleSettings.LIFECYCLE_NAME, policy)
                .put(RolloverAction.LIFECYCLE_ROLLOVER_ALIAS, alias)
                .put("index.blocks.read_only", true),
            true
        );

        // wait for ILM to start retrying the step
        assertBusy(() -> assertThat((Integer) explainIndex(firstIndex).get(FAILED_STEP_RETRY_COUNT_FIELD), greaterThanOrEqualTo(1)));

        // remove the read only block
        Request allowWritesOnIndexSettingUpdate = new Request("PUT", firstIndex + "/_settings");
        allowWritesOnIndexSettingUpdate.setJsonEntity("{" +
            "  \"index\": {\n" +
            "     \"blocks.read_only\" : \"false\" \n" +
            "  }\n" +
            "}");
        client().performRequest(allowWritesOnIndexSettingUpdate);

        // index is not readonly so the ILM should complete successfully
        assertBusy(() -> assertThat(getStepKeyForIndex(firstIndex), equalTo(PhaseCompleteStep.finalStep("hot").getKey())));
    }

    public void testILMRolloverOnManuallyRolledIndex() throws Exception {
        String originalIndex = index + "-000001";
        String secondIndex = index + "-000002";
        String thirdIndex = index + "-000003";

        // Set up a policy with rollover
        createNewSingletonPolicy("hot", new RolloverAction(null, null, 2L));
        Request createIndexTemplate = new Request("PUT", "_template/rolling_indexes");
        createIndexTemplate.setJsonEntity("{" +
            "\"index_patterns\": [\"" + index + "-*\"], \n" +
            "  \"settings\": {\n" +
            "    \"number_of_shards\": 1,\n" +
            "    \"number_of_replicas\": 0,\n" +
            "    \"index.lifecycle.name\": \"" + policy + "\", \n" +
            "    \"index.lifecycle.rollover_alias\": \"" + alias + "\"\n" +
            "  }\n" +
            "}");
        client().performRequest(createIndexTemplate);

        createIndexWithSettings(
            originalIndex,
            Settings.builder().put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0),
            true
        );

        // Index a document
        index(client(), originalIndex, "1", "foo", "bar");
        Request refreshOriginalIndex = new Request("POST", "/" + originalIndex + "/_refresh");
        client().performRequest(refreshOriginalIndex);

        // Manual rollover
        Request rolloverRequest = new Request("POST", "/" + alias + "/_rollover");
        rolloverRequest.setJsonEntity("{\n" +
            "  \"conditions\": {\n" +
            "    \"max_docs\": \"1\"\n" +
            "  }\n" +
            "}"
        );
        client().performRequest(rolloverRequest);
        assertBusy(() -> assertTrue(indexExists(secondIndex)));

        // Index another document into the original index so the ILM rollover policy condition is met
        index(client(), originalIndex, "2", "foo", "bar");
        client().performRequest(refreshOriginalIndex);

        // Wait for the rollover policy to execute
        assertBusy(() -> assertThat(getStepKeyForIndex(originalIndex), equalTo(PhaseCompleteStep.finalStep("hot").getKey())));

        // ILM should manage the second index after attempting (and skipping) rolling the original index
        assertBusy(() -> assertTrue((boolean) explainIndex(secondIndex).getOrDefault("managed", true)));

        // index some documents to trigger an ILM rollover
        index(client(), alias, "1", "foo", "bar");
        index(client(), alias, "2", "foo", "bar");
        index(client(), alias, "3", "foo", "bar");
        Request refreshSecondIndex = new Request("POST", "/" + secondIndex + "/_refresh");
        client().performRequest(refreshSecondIndex).getStatusLine();

        // ILM should rollover the second index even though it skipped the first one
        assertBusy(() -> assertThat(getStepKeyForIndex(secondIndex), equalTo(PhaseCompleteStep.finalStep("hot").getKey())));
        assertBusy(() -> assertTrue(indexExists(thirdIndex)));
    }

    public void testRolloverStepRetriesUntilRolledOverIndexIsDeleted() throws Exception {
        String index = this.index + "-000001";
        String rolledIndex = this.index + "-000002";

        createNewSingletonPolicy("hot", new RolloverAction(null, TimeValue.timeValueSeconds(1), null));

        // create the rolled index so the rollover of the first index fails
        createIndexWithSettings(
            rolledIndex,
            Settings.builder().put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0)
                .put(RolloverAction.LIFECYCLE_ROLLOVER_ALIAS, alias),
            false
        );

        createIndexWithSettings(
            index,
            Settings.builder().put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0)
                .put(LifecycleSettings.LIFECYCLE_NAME, policy)
                .put(RolloverAction.LIFECYCLE_ROLLOVER_ALIAS, alias),
            true
        );

        assertBusy(() -> assertThat((Integer) explainIndex(index).get(FAILED_STEP_RETRY_COUNT_FIELD), greaterThanOrEqualTo(1)), 30,
            TimeUnit.SECONDS);

        Request moveToStepRequest = new Request("POST", "_ilm/move/" + index);
        moveToStepRequest.setJsonEntity("{\n" +
            "  \"current_step\": {\n" +
            "    \"phase\": \"hot\",\n" +
            "    \"action\": \"rollover\",\n" +
            "    \"name\": \"check-rollover-ready\"\n" +
            "  },\n" +
            "  \"next_step\": {\n" +
            "    \"phase\": \"hot\",\n" +
            "    \"action\": \"rollover\",\n" +
            "    \"name\": \"attempt-rollover\"\n" +
            "  }\n" +
            "}");

        // Using {@link #waitUntil} here as ILM moves back and forth between the {@link WaitForRolloverReadyStep} step and
        // {@link org.elasticsearch.xpack.core.ilm.ErrorStep} in order to retry the failing step. As {@link #assertBusy}
        // increases the wait time between calls exponentially, we might miss the window where the policy is on
        // {@link WaitForRolloverReadyStep} and the move to `attempt-rollover` request will not be successful.
        waitUntil(() -> {
            try {
                return client().performRequest(moveToStepRequest).getStatusLine().getStatusCode() == 200;
            } catch (IOException e) {
                return false;
            }
        }, 30, TimeUnit.SECONDS);

        // Similar to above, using {@link #waitUntil} as we want to make sure the `attempt-rollover` step started failing and is being
        // retried (which means ILM moves back and forth between the `attempt-rollover` step and the `error` step)
        waitUntil(() -> {
            try {
                Map<String, Object> explainIndexResponse = explainIndex(index);
                String step = (String) explainIndexResponse.get("step");
                Integer retryCount = (Integer) explainIndexResponse.get(FAILED_STEP_RETRY_COUNT_FIELD);
                return step != null && step.equals("attempt-rollover") && retryCount != null && retryCount >= 1;
            } catch (IOException e) {
                return false;
            }
        }, 30, TimeUnit.SECONDS);

        deleteIndex(rolledIndex);

        // the rollover step should eventually succeed
        assertBusy(() -> assertThat(indexExists(rolledIndex), is(true)));
        assertBusy(() -> assertThat(getStepKeyForIndex(index), equalTo(PhaseCompleteStep.finalStep("hot").getKey())));
    }

    public void testUpdateRolloverLifecycleDateStepRetriesWhenRolloverInfoIsMissing() throws Exception {
        String index = this.index + "-000001";

        createNewSingletonPolicy("hot", new RolloverAction(null, null, 1L));

        createIndexWithSettings(
            index,
            Settings.builder().put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0)
                .put(LifecycleSettings.LIFECYCLE_NAME, policy)
                .put(RolloverAction.LIFECYCLE_ROLLOVER_ALIAS, alias),
            true
        );

        assertBusy(() -> assertThat(getStepKeyForIndex(index).getName(), is(WaitForRolloverReadyStep.NAME)));

        // moving ILM to the "update-rollover-lifecycle-date" without having gone through the actual rollover step
        // the "update-rollover-lifecycle-date" step will fail as the index has no rollover information
        Request moveToStepRequest = new Request("POST", "_ilm/move/" + index);
        moveToStepRequest.setJsonEntity("{\n" +
            "  \"current_step\": {\n" +
            "    \"phase\": \"hot\",\n" +
            "    \"action\": \"rollover\",\n" +
            "    \"name\": \"check-rollover-ready\"\n" +
            "  },\n" +
            "  \"next_step\": {\n" +
            "    \"phase\": \"hot\",\n" +
            "    \"action\": \"rollover\",\n" +
            "    \"name\": \"update-rollover-lifecycle-date\"\n" +
            "  }\n" +
            "}");
        client().performRequest(moveToStepRequest);

        waitUntil(() -> {
            try {
                Map<String, Object> explainIndexResponse = explainIndex(index);
                String step = (String) explainIndexResponse.get("step");
                Integer retryCount = (Integer) explainIndexResponse.get(FAILED_STEP_RETRY_COUNT_FIELD);
                return step != null && step.equals(UpdateRolloverLifecycleDateStep.NAME) && retryCount != null && retryCount >= 1;
            } catch (IOException e) {
                return false;
            }
        });

        index(client(), index, "1", "foo", "bar");
        Request refreshIndex = new Request("POST", "/" + index + "/_refresh");
        client().performRequest(refreshIndex);

        // manual rollover the index so the "update-rollover-lifecycle-date" ILM step can continue and finish successfully as the index
        // will have rollover information now
        Request rolloverRequest = new Request("POST", "/" + alias + "/_rollover");
        rolloverRequest.setJsonEntity("{\n" +
            "  \"conditions\": {\n" +
            "    \"max_docs\": \"1\"\n" +
            "  }\n" +
            "}"
        );
        client().performRequest(rolloverRequest);
        assertBusy(() -> assertThat(getStepKeyForIndex(index), equalTo(PhaseCompleteStep.finalStep("hot").getKey())));
    }

    public void testWaitForActiveShardsStep() throws Exception {
        String originalIndex = index + "-000001";
        String secondIndex = index + "-000002";
        createIndexWithSettings(originalIndex, Settings.builder().put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(RolloverAction.LIFECYCLE_ROLLOVER_ALIAS, alias),
            true);

        // create policy
        createNewSingletonPolicy("hot", new RolloverAction(null, null, 1L));
        // update policy on index
        updatePolicy(originalIndex, policy);
        Request createIndexTemplate = new Request("PUT", "_template/rolling_indexes");
        createIndexTemplate.setJsonEntity("{" +
            "\"index_patterns\": [\""+ index + "-*\"], \n" +
            "  \"settings\": {\n" +
            "    \"number_of_shards\": 1,\n" +
            "    \"number_of_replicas\": 142,\n" +
            "    \"index.write.wait_for_active_shards\": \"all\"\n" +
            "  }\n" +
            "}");
        client().performRequest(createIndexTemplate);

        // index document to trigger rollover
        index(client(), originalIndex, "_id", "foo", "bar");
        assertBusy(() -> assertTrue(indexExists(secondIndex)));

        assertBusy(() -> assertThat(getStepKeyForIndex(originalIndex).getName(), equalTo(WaitForActiveShardsStep.NAME)));

        // reset the number of replicas to 0 so that the second index wait for active shard condition can be met
        updateIndexSettings(secondIndex, Settings.builder().put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0));

        assertBusy(() -> assertThat(getStepKeyForIndex(originalIndex), equalTo(PhaseCompleteStep.finalStep("hot").getKey())));
    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/50353")
    public void testHistoryIsWrittenWithSuccess() throws Exception {
        String index = "success-index";

        createNewSingletonPolicy("hot", new RolloverAction(null, null, 1L));
        Request createIndexTemplate = new Request("PUT", "_template/rolling_indexes");
        createIndexTemplate.setJsonEntity("{" +
            "\"index_patterns\": [\""+ index + "-*\"], \n" +
            "  \"settings\": {\n" +
            "    \"number_of_shards\": 1,\n" +
            "    \"number_of_replicas\": 0,\n" +
            "    \"index.lifecycle.name\": \"" + policy+ "\",\n" +
            "    \"index.lifecycle.rollover_alias\": \"" + alias + "\"\n" +
            "  }\n" +
            "}");
        client().performRequest(createIndexTemplate);

        createIndexWithSettings(index + "-1",
            Settings.builder().put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0),
            true);

        // Index a document
        index(client(), index + "-1", "1", "foo", "bar");
        Request refreshIndex = new Request("POST", "/" + index + "-1/_refresh");
        client().performRequest(refreshIndex);

        assertBusy(() -> assertThat(getStepKeyForIndex(index + "-1"), equalTo(PhaseCompleteStep.finalStep("hot").getKey())));

        assertBusy(() -> assertHistoryIsPresent(policy, index + "-1", true, "wait-for-indexing-complete"), 30, TimeUnit.SECONDS);
        assertBusy(() -> assertHistoryIsPresent(policy, index + "-1", true, "wait-for-follow-shard-tasks"), 30, TimeUnit.SECONDS);
        assertBusy(() -> assertHistoryIsPresent(policy, index + "-1", true, "pause-follower-index"), 30, TimeUnit.SECONDS);
        assertBusy(() -> assertHistoryIsPresent(policy, index + "-1", true, "close-follower-index"), 30, TimeUnit.SECONDS);
        assertBusy(() -> assertHistoryIsPresent(policy, index + "-1", true, "unfollow-follower-index"), 30, TimeUnit.SECONDS);
        assertBusy(() -> assertHistoryIsPresent(policy, index + "-1", true, "open-follower-index"), 30, TimeUnit.SECONDS);
        assertBusy(() -> assertHistoryIsPresent(policy, index + "-1", true, "wait-for-yellow-step"), 30, TimeUnit.SECONDS);
        assertBusy(() -> assertHistoryIsPresent(policy, index + "-1", true, "check-rollover-ready"), 30, TimeUnit.SECONDS);
        assertBusy(() -> assertHistoryIsPresent(policy, index + "-1", true, "attempt-rollover"), 30, TimeUnit.SECONDS);
        assertBusy(() -> assertHistoryIsPresent(policy, index + "-1", true, "update-rollover-lifecycle-date"), 30, TimeUnit.SECONDS);
        assertBusy(() -> assertHistoryIsPresent(policy, index + "-1", true, "set-indexing-complete"), 30, TimeUnit.SECONDS);
        assertBusy(() -> assertHistoryIsPresent(policy, index + "-1", true, "completed"), 30, TimeUnit.SECONDS);

        assertBusy(() -> assertHistoryIsPresent(policy, index + "-000002", true, "check-rollover-ready"), 30, TimeUnit.SECONDS);
    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/50353")
    public void testHistoryIsWrittenWithFailure() throws Exception {
        String index = "failure-index";

        createNewSingletonPolicy("hot", new RolloverAction(null, null, 1L));
        Request createIndexTemplate = new Request("PUT", "_template/rolling_indexes");
        createIndexTemplate.setJsonEntity("{" +
            "\"index_patterns\": [\""+ index + "-*\"], \n" +
            "  \"settings\": {\n" +
            "    \"number_of_shards\": 1,\n" +
            "    \"number_of_replicas\": 0,\n" +
            "    \"index.lifecycle.name\": \"" + policy+ "\"\n" +
            "  }\n" +
            "}");
        client().performRequest(createIndexTemplate);

        createIndexWithSettings(index + "-1",
            Settings.builder().put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0),
            false);

        // Index a document
        index(client(), index + "-1", "1", "foo", "bar");
        Request refreshIndex = new Request("POST", "/" + index + "-1/_refresh");
        client().performRequest(refreshIndex);

        assertBusy(() -> assertThat(getStepKeyForIndex(index + "-1").getName(), equalTo(ErrorStep.NAME)));

        assertBusy(() -> assertHistoryIsPresent(policy, index + "-1", false, "ERROR"), 30, TimeUnit.SECONDS);
    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/50353")
    public void testHistoryIsWrittenWithDeletion() throws Exception {
        String index = "delete-index";

        createNewSingletonPolicy("delete", new DeleteAction());
        Request createIndexTemplate = new Request("PUT", "_template/delete_indexes");
        createIndexTemplate.setJsonEntity("{" +
            "\"index_patterns\": [\""+ index + "\"], \n" +
            "  \"settings\": {\n" +
            "    \"number_of_shards\": 1,\n" +
            "    \"number_of_replicas\": 0,\n" +
            "    \"index.lifecycle.name\": \"" + policy+ "\"\n" +
            "  }\n" +
            "}");
        client().performRequest(createIndexTemplate);

        // Index should be created and then deleted by ILM
        createIndexWithSettings(index, Settings.builder(), false);

        assertBusy(() -> {
            logger.info("--> checking for index deletion...");
            Request existCheck = new Request("HEAD", "/" + index);
            Response resp = client().performRequest(existCheck);
            assertThat(resp.getStatusLine().getStatusCode(), equalTo(404));
        });

        assertBusy(() -> {
            assertHistoryIsPresent(policy, index, true, "delete", "delete", "wait-for-shard-history-leases");
            assertHistoryIsPresent(policy, index, true, "delete", "delete", "complete");
        }, 30, TimeUnit.SECONDS);
    }

    public void testRetryableInitializationStep() throws Exception {
        String index = "retryinit-20xx-01-10";
        Request stopReq = new Request("POST", "/_ilm/stop");
        Request startReq = new Request("POST", "/_ilm/start");

        createNewSingletonPolicy("hot", new SetPriorityAction(1));

        // Stop ILM so that the initialize step doesn't run
        assertOK(client().performRequest(stopReq));

        // Create the index with the origination parsing turn *off* so it doesn't prevent creation
        createIndexWithSettings(
            index,
            Settings.builder().put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0)
                .put(LifecycleSettings.LIFECYCLE_NAME, policy)
                .put(LifecycleSettings.LIFECYCLE_PARSE_ORIGINATION_DATE, false));

        updateIndexSettings(index, Settings.builder()
            .put(LifecycleSettings.LIFECYCLE_PARSE_ORIGINATION_DATE, true));

        assertOK(client().performRequest(startReq));

        // Wait until an error has occurred.
        waitUntil(() -> {
            try {
                Map<String, Object> explainIndexResponse = explainIndex(index);
                String step = (String) explainIndexResponse.get("step");
                Integer retryCount = (Integer) explainIndexResponse.get(FAILED_STEP_RETRY_COUNT_FIELD);
                return step != null && step.equals(InitializePolicyContextStep.KEY.getAction()) && retryCount != null && retryCount >= 1;
            } catch (IOException e) {
                return false;
            }
        }, 30, TimeUnit.SECONDS);

        // Turn origination date parsing back off
        updateIndexSettings(index, Settings.builder()
            .put(LifecycleSettings.LIFECYCLE_PARSE_ORIGINATION_DATE, false));

        assertBusy(() -> {
            Map<String, Object> explainResp = explainIndex(index);
            String phase = (String) explainResp.get("phase");
            assertThat(phase, equalTo("hot"));
        });
    }

    public void testRefreshablePhaseJson() throws Exception {
        String index = "refresh-index";

        createNewSingletonPolicy("hot", new RolloverAction(null, null, 100L));
        Request createIndexTemplate = new Request("PUT", "_template/rolling_indexes");
        createIndexTemplate.setJsonEntity("{" +
            "\"index_patterns\": [\""+ index + "-*\"], \n" +
            "  \"settings\": {\n" +
            "    \"number_of_shards\": 1,\n" +
            "    \"number_of_replicas\": 0,\n" +
            "    \"index.lifecycle.name\": \"" + policy+ "\",\n" +
            "    \"index.lifecycle.rollover_alias\": \"" + alias + "\"\n" +
            "  }\n" +
            "}");
        client().performRequest(createIndexTemplate);

        createIndexWithSettings(index + "-1",
            Settings.builder().put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0),
            true);

        // Index a document
        index(client(), index + "-1", "1", "foo", "bar");

        // Wait for the index to enter the check-rollover-ready step
        assertBusy(() -> assertThat(getStepKeyForIndex(index + "-1").getName(), equalTo(WaitForRolloverReadyStep.NAME)));

        // Update the policy to allow rollover at 1 document instead of 100
        createNewSingletonPolicy("hot", new RolloverAction(null, null, 1L));

        // Index should now have been able to roll over, creating the new index and proceeding to the "complete" step
        assertBusy(() -> assertThat(indexExists(index + "-000002"), is(true)));
        assertBusy(() -> assertThat(getStepKeyForIndex(index + "-1").getName(), equalTo(PhaseCompleteStep.NAME)));
    }

    public void testHaltAtEndOfPhase() throws Exception {
        String index = "halt-index";

        createNewSingletonPolicy("hot", new SetPriorityAction(100));

        createIndexWithSettings(index,
            Settings.builder()
                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0)
                .put(LifecycleSettings.LIFECYCLE_NAME, policy),
            randomBoolean());

        // Wait for the index to finish the "hot" phase
        assertBusy(() -> assertThat(getStepKeyForIndex(index), equalTo(PhaseCompleteStep.finalStep("hot").getKey())));

        // Update the policy to add a delete phase
        {
            Map<String, LifecycleAction> hotActions = new HashMap<>();
            hotActions.put(SetPriorityAction.NAME, new SetPriorityAction(100));
            Map<String, Phase> phases = new HashMap<>();
            phases.put("hot", new Phase("hot", TimeValue.ZERO, hotActions));
            phases.put("delete", new Phase("delete", TimeValue.ZERO, singletonMap(DeleteAction.NAME, new DeleteAction())));
            LifecyclePolicy lifecyclePolicy = new LifecyclePolicy(policy, phases);
            // PUT policy
            XContentBuilder builder = jsonBuilder();
            lifecyclePolicy.toXContent(builder, null);
            final StringEntity entity = new StringEntity(
                "{ \"policy\":" + Strings.toString(builder) + "}", ContentType.APPLICATION_JSON);
            Request request = new Request("PUT", "_ilm/policy/" + policy);
            request.setEntity(entity);
            assertOK(client().performRequest(request));
        }

        // The index should move into the deleted phase and be deleted
        assertBusy(() -> assertFalse("expected " + index + " to be deleted by ILM", indexExists(index)));
    }

    // This method should be called inside an assertBusy, it has no retry logic of its own
    private void assertHistoryIsPresent(String policyName, String indexName, boolean success, String stepName) throws IOException {
        assertHistoryIsPresent(policyName, indexName, success, null, null, stepName);
    }

    // This method should be called inside an assertBusy, it has no retry logic of its own
    private void assertHistoryIsPresent(String policyName, String indexName, boolean success,
                                        @Nullable String phase, @Nullable String action, String stepName) throws IOException {
        logger.info("--> checking for history item [{}], [{}], success: [{}], phase: [{}], action: [{}], step: [{}]",
            policyName, indexName, success, phase, action, stepName);
        final Request historySearchRequest = new Request("GET", "ilm-history*/_search");
        historySearchRequest.setJsonEntity("{\n" +
            "  \"query\": {\n" +
            "    \"bool\": {\n" +
            "      \"must\": [\n" +
            "        {\n" +
            "          \"term\": {\n" +
            "            \"policy\": \"" + policyName + "\"\n" +
            "          }\n" +
            "        },\n" +
            "        {\n" +
            "          \"term\": {\n" +
            "            \"success\": " + success + "\n" +
            "          }\n" +
            "        },\n" +
            "        {\n" +
            "          \"term\": {\n" +
            "            \"index\": \"" + indexName + "\"\n" +
            "          }\n" +
            "        },\n" +
            "        {\n" +
            "          \"term\": {\n" +
            "            \"state.step\": \"" + stepName + "\"\n" +
            "          }\n" +
            "        }\n" +
            (phase == null ? "" : ",{\"term\": {\"state.phase\": \"" + phase + "\"}}") +
            (action == null ? "" : ",{\"term\": {\"state.action\": \"" + action + "\"}}") +
            "      ]\n" +
            "    }\n" +
            "  }\n" +
            "}");
        Response historyResponse;
        try {
            historyResponse = client().performRequest(historySearchRequest);
            Map<String, Object> historyResponseMap;
            try (InputStream is = historyResponse.getEntity().getContent()) {
                historyResponseMap = XContentHelper.convertToMap(XContentType.JSON.xContent(), is, true);
            }
            logger.info("--> history response: {}", historyResponseMap);
            int hits = (int)((Map<String, Object>) ((Map<String, Object>) historyResponseMap.get("hits")).get("total")).get("value");

            // For a failure, print out whatever history we *do* have for the index
            if (hits == 0) {
                final Request allResults = new Request("GET", "ilm-history*/_search");
                allResults.setJsonEntity("{\n" +
                    "  \"query\": {\n" +
                    "    \"bool\": {\n" +
                    "      \"must\": [\n" +
                    "        {\n" +
                    "          \"term\": {\n" +
                    "            \"policy\": \"" + policyName + "\"\n" +
                    "          }\n" +
                    "        },\n" +
                    "        {\n" +
                    "          \"term\": {\n" +
                    "            \"index\": \"" + indexName + "\"\n" +
                    "          }\n" +
                    "        }\n" +
                    "      ]\n" +
                    "    }\n" +
                    "  }\n" +
                    "}");
                final Response allResultsResp = client().performRequest(historySearchRequest);
                Map<String, Object> allResultsMap;
                try (InputStream is = allResultsResp.getEntity().getContent()) {
                    allResultsMap = XContentHelper.convertToMap(XContentType.JSON.xContent(), is, true);
                }
                logger.info("--> expected at least 1 hit, got 0. All history for index [{}]: {}", index, allResultsMap);
            }
            assertThat(hits, greaterThanOrEqualTo(1));
        } catch (ResponseException e) {
            // Throw AssertionError instead of an exception if the search fails so that assertBusy works as expected
            logger.error(e);
            fail("failed to perform search:" + e.getMessage());
        }

        // Finally, check that the history index is in a good state
        Step.StepKey stepKey = getStepKeyForIndex("ilm-history-1-000001");
        assertEquals("hot", stepKey.getPhase());
        assertEquals(RolloverAction.NAME, stepKey.getAction());
        assertEquals(WaitForRolloverReadyStep.NAME, stepKey.getName());
    }

    private void createFullPolicy(TimeValue hotTime) throws IOException {
        Map<String, LifecycleAction> hotActions = new HashMap<>();
        hotActions.put(SetPriorityAction.NAME, new SetPriorityAction(100));
        hotActions.put(RolloverAction.NAME,  new RolloverAction(null, null, 1L));
        Map<String, LifecycleAction> warmActions = new HashMap<>();
        warmActions.put(SetPriorityAction.NAME, new SetPriorityAction(50));
        warmActions.put(ForceMergeAction.NAME, new ForceMergeAction(1));
        warmActions.put(AllocateAction.NAME, new AllocateAction(1, singletonMap("_name", "integTest-1,integTest-2"), null, null));
        warmActions.put(ShrinkAction.NAME, new ShrinkAction(1));
        Map<String, LifecycleAction> coldActions = new HashMap<>();
        coldActions.put(SetPriorityAction.NAME, new SetPriorityAction(0));
        coldActions.put(AllocateAction.NAME, new AllocateAction(0, singletonMap("_name", "integTest-3"), null, null));
        Map<String, Phase> phases = new HashMap<>();
        phases.put("hot", new Phase("hot", hotTime, hotActions));
        phases.put("warm", new Phase("warm", TimeValue.ZERO, warmActions));
        phases.put("cold", new Phase("cold", TimeValue.ZERO, coldActions));
        phases.put("delete", new Phase("delete", TimeValue.ZERO, singletonMap(DeleteAction.NAME, new DeleteAction())));
        LifecyclePolicy lifecyclePolicy = new LifecyclePolicy(policy, phases);
        // PUT policy
        XContentBuilder builder = jsonBuilder();
        lifecyclePolicy.toXContent(builder, null);
        final StringEntity entity = new StringEntity(
            "{ \"policy\":" + Strings.toString(builder) + "}", ContentType.APPLICATION_JSON);
        Request request = new Request("PUT", "_ilm/policy/" + policy);
        request.setEntity(entity);
        assertOK(client().performRequest(request));
    }

    private void createNewSingletonPolicy(String phaseName, LifecycleAction action) throws IOException {
        createNewSingletonPolicy(phaseName, action, TimeValue.ZERO);
    }

    private void createNewSingletonPolicy(String phaseName, LifecycleAction action, TimeValue after) throws IOException {
        Phase phase = new Phase(phaseName, after, singletonMap(action.getWriteableName(), action));
        LifecyclePolicy lifecyclePolicy = new LifecyclePolicy(policy, singletonMap(phase.getName(), phase));
        XContentBuilder builder = jsonBuilder();
        lifecyclePolicy.toXContent(builder, null);
        final StringEntity entity = new StringEntity(
            "{ \"policy\":" + Strings.toString(builder) + "}", ContentType.APPLICATION_JSON);
        Request request = new Request("PUT", "_ilm/policy/" + policy);
        request.setEntity(entity);
        client().performRequest(request);
    }

    private void createIndexWithSettingsNoAlias(String index, Settings.Builder settings) throws IOException {
        Request request = new Request("PUT", "/" + index);
        request.setJsonEntity("{\n \"settings\": " + Strings.toString(settings.build())
            + "}");
        client().performRequest(request);
        // wait for the shards to initialize
        ensureGreen(index);
    }

    private void createIndexWithSettings(String index, Settings.Builder settings) throws IOException {
        createIndexWithSettings(index, settings, randomBoolean());
    }

    private void createIndexWithSettings(String index, Settings.Builder settings, boolean useWriteIndex) throws IOException {
        Request request = new Request("PUT", "/" + index);

        String writeIndexSnippet = "";
        if (useWriteIndex) {
            writeIndexSnippet = "\"is_write_index\": true";
        }
        request.setJsonEntity("{\n \"settings\": " + Strings.toString(settings.build())
            + ", \"aliases\" : { \"" + alias + "\": { " + writeIndexSnippet + " } } }");
        client().performRequest(request);
        // wait for the shards to initialize
        ensureGreen(index);
    }

    private static void index(RestClient client, String index, String id, Object... fields) throws IOException {
        XContentBuilder document = jsonBuilder().startObject();
        for (int i = 0; i < fields.length; i += 2) {
            document.field((String) fields[i], fields[i + 1]);
        }
        document.endObject();
        final Request request = new Request("POST", "/" + index + "/_doc/" + id);
        request.setJsonEntity(Strings.toString(document));
        assertOK(client.performRequest(request));
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> getOnlyIndexSettings(String index) throws IOException {
        Map<String, Object> response = (Map<String, Object>) getIndexSettings(index).get(index);
        if (response == null) {
            return Collections.emptyMap();
        }
        return (Map<String, Object>) response.get("settings");
    }


    public static StepKey getStepKeyForIndex(String indexName) throws IOException {
        Map<String, Object> indexResponse = explainIndex(indexName);
        if (indexResponse == null) {
            return new StepKey(null, null, null);
        }

        return getStepKey(indexResponse);
    }

    private static StepKey getStepKey(Map<String, Object> explainIndexResponse) {
        String phase = (String) explainIndexResponse.get("phase");
        String action = (String) explainIndexResponse.get("action");
        String step = (String) explainIndexResponse.get("step");
        return new StepKey(phase, action, step);
    }

    private String getFailedStepForIndex(String indexName) throws IOException {
        Map<String, Object> indexResponse = explainIndex(indexName);
        if (indexResponse == null) {
            return null;
        }

        return (String) indexResponse.get("failed_step");
    }

    private static Map<String, Object> explainIndex(String indexName) throws IOException {
        return explain(indexName, false, false).get(indexName);
    }

    private static Map<String, Map<String, Object>> explain(String indexPattern, boolean onlyErrors,
                                                            boolean onlyManaged) throws IOException {
        Request explainRequest = new Request("GET", indexPattern + "/_ilm/explain");
        explainRequest.addParameter("only_errors", Boolean.toString(onlyErrors));
        explainRequest.addParameter("only_managed", Boolean.toString(onlyManaged));
        Response response = client().performRequest(explainRequest);
        Map<String, Object> responseMap;
        try (InputStream is = response.getEntity().getContent()) {
            responseMap = XContentHelper.convertToMap(XContentType.JSON.xContent(), is, true);
        }

        @SuppressWarnings("unchecked") Map<String, Map<String, Object>> indexResponse =
            ((Map<String, Map<String, Object>>) responseMap.get("indices"));
        return indexResponse;
    }

    private void indexDocument() throws IOException {
        Request indexRequest = new Request("POST", index + "/_doc");
        indexRequest.setEntity(new StringEntity("{\"a\": \"test\"}", ContentType.APPLICATION_JSON));
        Response response = client().performRequest(indexRequest);
        logger.info(response.getStatusLine());
    }

    @SuppressWarnings("unchecked")
    private String getSnapshotState(String snapshot) throws IOException {
        Response response = client().performRequest(new Request("GET", "/_snapshot/repo/" + snapshot));
        Map<String, Object> responseMap;
        try (InputStream is = response.getEntity().getContent()) {
            responseMap = XContentHelper.convertToMap(XContentType.JSON.xContent(), is, true);
        }

        Map<String, Object> repoResponse = ((List<Map<String, Object>>) responseMap.get("responses")).get(0);
        Map<String, Object> snapResponse = ((List<Map<String, Object>>) repoResponse.get("snapshots")).get(0);
        assertThat(snapResponse.get("snapshot"), equalTo(snapshot));
        return (String) snapResponse.get("state");
    }

    private void createSlmPolicy(String smlPolicy, String repo) throws IOException {
        Request request;
        request = new Request("PUT", "/_slm/policy/" + smlPolicy);
        request.setJsonEntity(Strings
            .toString(JsonXContent.contentBuilder()
                .startObject()
                .field("schedule", "59 59 23 31 12 ? 2099")
                .field("repository", repo)
                .field("name", "snap" + randomAlphaOfLengthBetween(5, 10).toLowerCase(Locale.ROOT))
                .startObject("config")
                    .field("include_global_state", false)
                .endObject()
                .endObject()));

        assertOK(client().performRequest(request));
    }

    private String createSnapshotRepo() throws IOException {
        String repo = randomAlphaOfLengthBetween(4, 10);
        Request request = new Request("PUT", "/_snapshot/" + repo);
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
        return repo;
    }
}
