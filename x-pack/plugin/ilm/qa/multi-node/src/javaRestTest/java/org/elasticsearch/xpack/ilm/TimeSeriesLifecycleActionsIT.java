/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ilm;

import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.core.CheckedRunnable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.engine.EngineConfig;
import org.elasticsearch.rest.action.admin.indices.RestPutIndexTemplateAction;
import org.elasticsearch.snapshots.SnapshotState;
import org.elasticsearch.test.rest.ESRestTestCase;
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
import org.elasticsearch.xpack.core.ilm.Step;
import org.elasticsearch.xpack.core.ilm.WaitForActiveShardsStep;
import org.elasticsearch.xpack.core.ilm.WaitForRolloverReadyStep;
import org.elasticsearch.xpack.core.ilm.WaitForSnapshotAction;
import org.hamcrest.Matchers;
import org.junit.Before;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static java.util.Collections.singletonMap;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.xpack.TimeSeriesRestDriver.createFullPolicy;
import static org.elasticsearch.xpack.TimeSeriesRestDriver.createIndexWithSettings;
import static org.elasticsearch.xpack.TimeSeriesRestDriver.createNewSingletonPolicy;
import static org.elasticsearch.xpack.TimeSeriesRestDriver.createSnapshotRepo;
import static org.elasticsearch.xpack.TimeSeriesRestDriver.explainIndex;
import static org.elasticsearch.xpack.TimeSeriesRestDriver.getNumberOfSegments;
import static org.elasticsearch.xpack.TimeSeriesRestDriver.getOnlyIndexSettings;
import static org.elasticsearch.xpack.TimeSeriesRestDriver.getSnapshotState;
import static org.elasticsearch.xpack.TimeSeriesRestDriver.getStepKeyForIndex;
import static org.elasticsearch.xpack.TimeSeriesRestDriver.index;
import static org.elasticsearch.xpack.TimeSeriesRestDriver.indexDocument;
import static org.elasticsearch.xpack.TimeSeriesRestDriver.updatePolicy;
import static org.elasticsearch.xpack.core.ilm.ShrinkIndexNameSupplier.SHRUNKEN_INDEX_PREFIX;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

public class TimeSeriesLifecycleActionsIT extends ESRestTestCase {
    private static final Logger logger = LogManager.getLogger(TimeSeriesLifecycleActionsIT.class);
    private static final String FAILED_STEP_RETRY_COUNT_FIELD = "failed_step_retry_count";

    private String index;
    private String policy;
    private String alias;

    @Before
    public void refreshIndex() {
        index = "index-" + randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        policy = "policy-" + randomAlphaOfLength(5);
        alias = "alias-" + randomAlphaOfLength(5);
    }

    public void testFullPolicy() throws Exception {
        String originalIndex = index + "-000001";
        String shrunkenOriginalIndex = SHRUNKEN_INDEX_PREFIX + originalIndex;
        String secondIndex = index + "-000002";
        createIndexWithSettings(client(), originalIndex, alias, Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 2)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put("index.routing.allocation.include._name", "javaRestTest-0")
            .put(RolloverAction.LIFECYCLE_ROLLOVER_ALIAS, alias));

        // create policy
        createFullPolicy(client(), policy, TimeValue.ZERO);
        // update policy on index
        updatePolicy(client(), originalIndex, policy);
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

    public void testRetryFailedDeleteAction() throws Exception {
        createNewSingletonPolicy(client(), policy, "delete", new DeleteAction());
        createIndexWithSettings(client(), index, alias, Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexMetadata.SETTING_READ_ONLY, true)
            .put("index.lifecycle.name", policy));

        assertBusy(() -> assertThat((Integer) explainIndex(client(), index).get(FAILED_STEP_RETRY_COUNT_FIELD), greaterThanOrEqualTo(1)),
            30, TimeUnit.SECONDS);
        assertTrue(indexExists(index));

        Request request = new Request("PUT", index + "/_settings");
        request.setJsonEntity("{\"index.blocks.read_only\":false}");
        assertOK(client().performRequest(request));

        assertBusy(() -> assertFalse(indexExists(index)));
    }

    public void testRetryFreezeDeleteAction() throws Exception {
        createNewSingletonPolicy(client(), policy, "cold", new FreezeAction());

        createIndexWithSettings(client(), index, alias, Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexMetadata.SETTING_READ_ONLY, true)
            .put("index.lifecycle.name", policy));

        assertBusy(() -> assertThat((Integer) explainIndex(client(), index).get(FAILED_STEP_RETRY_COUNT_FIELD), greaterThanOrEqualTo(1)),
            30, TimeUnit.SECONDS);
        assertFalse(getOnlyIndexSettings(client(), index).containsKey("index.frozen"));

        Request request = new Request("PUT", index + "/_settings");
        request.setJsonEntity("{\"index.blocks.read_only\":false}");
        assertOK(client().performRequest(request));

        assertBusy(() -> assertThat(getOnlyIndexSettings(client(), index).get("index.frozen"), equalTo("true")));
    }


    public void testAllocateOnlyAllocation() throws Exception {
        createIndexWithSettings(client(), index, alias, Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 2)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0));
        String allocateNodeName = "javaRestTest-0,javaRestTest-1,javaRestTest-2,javaRestTest-3";
        AllocateAction allocateAction = new AllocateAction(null, singletonMap("_name", allocateNodeName), null, null);
        String endPhase = randomFrom("warm", "cold");
        createNewSingletonPolicy(client(), policy, endPhase, allocateAction);
        updatePolicy(client(), index, policy);
        assertBusy(() -> assertThat(getStepKeyForIndex(client(), index), equalTo(PhaseCompleteStep.finalStep(endPhase).getKey())),
            30, TimeUnit.SECONDS);
        ensureGreen(index);
    }

    public void testAllocateActionOnlyReplicas() throws Exception {
        int numShards = randomFrom(1, 5);
        int numReplicas = randomFrom(0, 1);
        int finalNumReplicas = (numReplicas + 1) % 2;
        createIndexWithSettings(client(), index, alias, Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, numShards)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, numReplicas));
        AllocateAction allocateAction = new AllocateAction(finalNumReplicas, null, null, null);
        String endPhase = randomFrom("warm", "cold");
        createNewSingletonPolicy(client(), policy, endPhase, allocateAction);
        updatePolicy(client(), index, policy);
        assertBusy(() -> {
            Map<String, Object> settings = getOnlyIndexSettings(client(), index);
            assertThat(getStepKeyForIndex(client(), index), equalTo(PhaseCompleteStep.finalStep(endPhase).getKey()));
            assertThat(settings.get(IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey()), equalTo(String.valueOf(finalNumReplicas)));
        });
    }

    public void testWaitForSnapshot() throws Exception {
        createIndexWithSettings(client(), index, alias, Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0));
        String slmPolicy = randomAlphaOfLengthBetween(4, 10);
        createNewSingletonPolicy(client(), policy, "delete", new WaitForSnapshotAction(slmPolicy));
        updatePolicy(client(), index, policy);
        waitForPhaseTime();
        assertBusy(() -> {
            Map<String, Object> indexILMState = explainIndex(client(), index);
            assertThat(indexILMState.get("action"), is("wait_for_snapshot"));
            assertThat(indexILMState.get("failed_step"), is("wait-for-snapshot"));
        }, slmPolicy);
        String snapshotRepo = randomAlphaOfLengthBetween(4, 10);
        createSnapshotRepo(client(), snapshotRepo, randomBoolean());
        createSlmPolicy(slmPolicy, snapshotRepo);
        assertBusy(() -> {
            Map<String, Object> indexILMState = explainIndex(client(), index);
            //wait for step to notice that the slm policy is created and to get out of error
            assertThat(indexILMState.get("failed_step"), nullValue());
            assertThat(indexILMState.get("action"), is("wait_for_snapshot"));
            assertThat(indexILMState.get("step"), is("wait-for-snapshot"));
        }, slmPolicy);

        Request request = new Request("PUT", "/_slm/policy/" + slmPolicy + "/_execute");
        assertOK(client().performRequest(request));
        assertBusy(() -> {
            Step.StepKey stepKey = getStepKeyForIndex(client(), index);
            logger.info("step key for index {} is {}", index, stepKey);
            assertThat(stepKey.getAction(), equalTo("complete"));
        }, slmPolicy);
    }

    public void testWaitForSnapshotSlmExecutedBefore() throws Exception {
        createIndexWithSettings(client(), index, alias, Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0));
        String slmPolicy = randomAlphaOfLengthBetween(4, 10);
        createNewSingletonPolicy(client(), policy, "delete", new WaitForSnapshotAction(slmPolicy));
        waitForPhaseTime();

        String snapshotRepo = randomAlphaOfLengthBetween(4, 10);
        createSnapshotRepo(client(), snapshotRepo, randomBoolean());
        createSlmPolicy(slmPolicy, snapshotRepo);

        Request request = new Request("PUT", "/_slm/policy/" + slmPolicy + "/_execute");
        assertOK(client().performRequest(request));

        //wait for slm to finish execution
        assertBusy(() -> {
            Response response = client().performRequest(new Request("GET", "/_slm/policy/" + slmPolicy));
            try (InputStream is = response.getEntity().getContent()) {
                Map<String, Object> responseMap = XContentHelper.convertToMap(XContentType.JSON.xContent(), is, true);
                assertEquals(1, ((Map<?, ?>) ((Map<?, ?>) responseMap.get(slmPolicy)).get("stats")).get("snapshots_taken"));
            }
        }, slmPolicy);

        updatePolicy(client(), index, policy);

        assertBusy(() -> {
            Map<String, Object> indexILMState = explainIndex(client(), index);
            assertThat(indexILMState.get("failed_step"), nullValue());
            assertThat(indexILMState.get("action"), is("wait_for_snapshot"));
            assertThat(indexILMState.get("step"), is("wait-for-snapshot"));
        }, slmPolicy);

        request = new Request("PUT", "/_slm/policy/" + slmPolicy + "/_execute");
        assertOK(client().performRequest(request));

        //wait for slm to finish execution
        assertBusy(() -> {
            Response response = client().performRequest(new Request("GET", "/_slm/policy/" + slmPolicy));
            try (InputStream is = response.getEntity().getContent()) {
                Map<String, Object> responseMap = XContentHelper.convertToMap(XContentType.JSON.xContent(), is, true);
                assertEquals(2, ((Map<?, ?>) ((Map<?, ?>) responseMap.get(slmPolicy)).get("stats")).get("snapshots_taken"));
            }
        }, slmPolicy);

        assertBusy(() -> {
            Step.StepKey stepKey = getStepKeyForIndex(client(), index);
            logger.info("stepKey for index {} is {}", index, stepKey);
            assertThat(stepKey.getAction(), equalTo("complete"));
        }, slmPolicy);
        assertBusy(() -> assertThat(getStepKeyForIndex(client(), index).getAction(), equalTo("complete")), slmPolicy);
    }

    /*
     * This method waits until phase_time gets set in the state store for the delete phase. Otherwise we can wind up starting a snapshot
     * before the ILM policy is ready.
     */
    @SuppressWarnings("unchecked")
    private void waitForPhaseTime() throws Exception {
        assertBusy(() -> {
            Request request = new Request("GET", "/_cluster/state/metadata/" + index);
            Map<String, Object> response = entityAsMap(client().performRequest(request));
            Map<String, Object> metadata = (Map<String, Object>) response.get("metadata");
            Map<String, Object> indices = (Map<String, Object>) metadata.get("indices");
            Map<String, Object> indexMap = (Map<String, Object>) indices.get(index);
            Map<String, Object> ilm = (Map<String, Object>) indexMap.get("ilm");
            assertNotNull(ilm);
            Object phase = ilm.get("phase");
            assertEquals("delete", phase);
            Object phase_time = ilm.get("phase_time");
            assertNotNull(phase_time);
            logger.info("Found phase time for delete phase: {}", phase_time);
        });
    }

    public void testDelete() throws Exception {
        createIndexWithSettings(client(), index, alias, Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0));
        createNewSingletonPolicy(client(), policy, "delete", new DeleteAction());
        updatePolicy(client(), index, policy);
        assertBusy(() -> assertFalse(indexExists(index)));
    }

    public void testDeleteOnlyShouldNotMakeIndexReadonly() throws Exception {
        createIndexWithSettings(client(), index, alias, Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0));
        createNewSingletonPolicy(client(), policy, "delete", new DeleteAction(), TimeValue.timeValueHours(1));
        updatePolicy(client(), index, policy);
        assertBusy(() -> {
            assertThat(getStepKeyForIndex(client(), index).getAction(), equalTo("complete"));
            Map<String, Object> settings = getOnlyIndexSettings(client(), index);
            assertThat(settings.get(IndexMetadata.INDEX_BLOCKS_WRITE_SETTING.getKey()), not("true"));
        });
        indexDocument(client(), index);
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
        createNewSingletonPolicy(client(), policy, "delete", new DeleteAction(), TimeValue.timeValueMillis(0));
        // create index without policy
        createIndexWithSettings(client(), index, alias, Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0));
        // index document so snapshot actually does something
        indexDocument(client(), index);
        // start snapshot
        String snapName = "snapshot-" + randomAlphaOfLength(6).toLowerCase(Locale.ROOT);
        request = new Request("PUT", "/_snapshot/repo/" + snapName);
        request.addParameter("wait_for_completion", "false");
        request.setJsonEntity("{\"indices\": \"" + index + "\"}");
        assertOK(client().performRequest(request));
        // add policy and expect it to trigger delete immediately (while snapshot in progress)
        updatePolicy(client(), index, policy);
        // assert that index was deleted
        assertBusy(() -> assertFalse(indexExists(index)), 2, TimeUnit.MINUTES);
        // assert that snapshot is still in progress and clean up
        assertThat(getSnapshotState(client(), snapName), equalTo("SUCCESS"));
        assertOK(client().performRequest(new Request("DELETE", "/_snapshot/repo/" + snapName)));
    }

    public void forceMergeActionWithCodec(String codec) throws Exception {
        createIndexWithSettings(client(), index, alias, Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0));
        for (int i = 0; i < randomIntBetween(2, 10); i++) {
            Request request = new Request("PUT", index + "/_doc/" + i);
            request.addParameter("refresh", "true");
            request.setEntity(new StringEntity("{\"a\": \"test\"}", ContentType.APPLICATION_JSON));
            client().performRequest(request);
        }

        assertThat(getNumberOfSegments(client(), index), greaterThanOrEqualTo(1));
        createNewSingletonPolicy(client(), policy, "warm", new ForceMergeAction(1, codec));
        updatePolicy(client(), index, policy);

        assertBusy(() -> {
            assertThat(getStepKeyForIndex(client(), index), equalTo(PhaseCompleteStep.finalStep("warm").getKey()));
            Map<String, Object> settings = getOnlyIndexSettings(client(), index);
            assertThat(settings.get(EngineConfig.INDEX_CODEC_SETTING.getKey()), equalTo(codec));
            assertThat(settings.get(IndexMetadata.INDEX_BLOCKS_WRITE_SETTING.getKey()), equalTo("true"));
        }, 30, TimeUnit.SECONDS);
        expectThrows(ResponseException.class, () -> indexDocument(client(), index));
    }

    public void testForceMergeAction() throws Exception {
        forceMergeActionWithCodec(null);
    }

    public void testForceMergeActionWithCompressionCodec() throws Exception {
        forceMergeActionWithCodec("best_compression");
    }

    public void testFreezeAction() throws Exception {
        createIndexWithSettings(client(), index, alias, Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0));
        createNewSingletonPolicy(client(), policy, "cold", new FreezeAction());
        updatePolicy(client(), index, policy);
        assertBusy(() -> {
            Map<String, Object> settings = getOnlyIndexSettings(client(), index);
            assertThat(getStepKeyForIndex(client(), index), equalTo(PhaseCompleteStep.finalStep("cold").getKey()));
            assertThat(settings.get(IndexMetadata.INDEX_BLOCKS_WRITE_SETTING.getKey()), equalTo("true"));
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
        createNewSingletonPolicy(client(), policy, "cold", new FreezeAction(), TimeValue.timeValueMillis(0));
        // create index without policy
        createIndexWithSettings(client(), index, alias, Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0));
        // index document so snapshot actually does something
        indexDocument(client(), index);
        // start snapshot
        request = new Request("PUT", "/_snapshot/repo/snapshot");
        request.addParameter("wait_for_completion", "false");
        request.setJsonEntity("{\"indices\": \"" + index + "\"}");
        assertOK(client().performRequest(request));
        // add policy and expect it to trigger delete immediately (while snapshot in progress)
        updatePolicy(client(), index, policy);
        // assert that the index froze
        assertBusy(() -> {
            Map<String, Object> settings = getOnlyIndexSettings(client(), index);
            assertThat(getStepKeyForIndex(client(), index), equalTo(PhaseCompleteStep.finalStep("cold").getKey()));
            assertThat(settings.get(IndexMetadata.INDEX_BLOCKS_WRITE_SETTING.getKey()), equalTo("true"));
            assertThat(settings.get(IndexSettings.INDEX_SEARCH_THROTTLED.getKey()), equalTo("true"));
            assertThat(settings.get("index.frozen"), equalTo("true"));
        }, 2, TimeUnit.MINUTES);
        // assert that snapshot is still in progress and clean up
        assertThat(getSnapshotState(client(), "snapshot"), equalTo("SUCCESS"));
        assertOK(client().performRequest(new Request("DELETE", "/_snapshot/repo/snapshot")));
    }

    public void testSetPriority() throws Exception {
        createIndexWithSettings(client(), index, alias, Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0).put(IndexMetadata.INDEX_PRIORITY_SETTING.getKey(), 100));
        int priority = randomIntBetween(0, 99);
        createNewSingletonPolicy(client(), policy, "warm", new SetPriorityAction(priority));
        updatePolicy(client(), index, policy);
        assertBusy(() -> {
            Map<String, Object> settings = getOnlyIndexSettings(client(), index);
            assertThat(getStepKeyForIndex(client(), index), equalTo(PhaseCompleteStep.finalStep("warm").getKey()));
            assertThat(settings.get(IndexMetadata.INDEX_PRIORITY_SETTING.getKey()), equalTo(String.valueOf(priority)));
        });
    }

    public void testSetNullPriority() throws Exception {
        createIndexWithSettings(client(), index, alias, Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0).put(IndexMetadata.INDEX_PRIORITY_SETTING.getKey(), 100));
        createNewSingletonPolicy(client(), policy, "warm", new SetPriorityAction((Integer) null));
        updatePolicy(client(), index, policy);
        assertBusy(() -> {
            Map<String, Object> settings = getOnlyIndexSettings(client(), index);
            assertThat(getStepKeyForIndex(client(), index), equalTo(PhaseCompleteStep.finalStep("warm").getKey()));
            assertNull(settings.get(IndexMetadata.INDEX_PRIORITY_SETTING.getKey()));
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
        templateRequest.setOptions(expectWarnings(RestPutIndexTemplateAction.DEPRECATION_WARNING));
        client().performRequest(templateRequest);

        policy = randomAlphaOfLengthBetween(5,20);
        createNewSingletonPolicy(client(), policy, "hot", new RolloverAction(null, null, null, 1L));

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
        indexDocument(client(), index);

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
        ex = expectThrows(ResponseException.class, () -> createNewSingletonPolicy(client(), policy, "delete", new DeleteAction()));
        assertThat(ex.getMessage(), containsString("invalid policy name"));

        policy = randomAlphaOfLengthBetween(0,10) + "%20" + randomAlphaOfLengthBetween(0,10);
        ex = expectThrows(ResponseException.class, () -> createNewSingletonPolicy(client(), policy, "delete", new DeleteAction()));
        assertThat(ex.getMessage(), containsString("invalid policy name"));

        policy = "_" + randomAlphaOfLengthBetween(1, 20);
        ex = expectThrows(ResponseException.class, () -> createNewSingletonPolicy(client(), policy, "delete", new DeleteAction()));
        assertThat(ex.getMessage(), containsString("invalid policy name"));

        policy = randomAlphaOfLengthBetween(256, 1000);
        ex = expectThrows(ResponseException.class, () -> createNewSingletonPolicy(client(), policy, "delete", new DeleteAction()));
        assertThat(ex.getMessage(), containsString("invalid policy name"));
    }

    public void testDeletePolicyInUse() throws IOException {
        String managedIndex1 = randomAlphaOfLength(7).toLowerCase(Locale.ROOT);
        String managedIndex2 = randomAlphaOfLength(8).toLowerCase(Locale.ROOT);
        String unmanagedIndex = randomAlphaOfLength(9).toLowerCase(Locale.ROOT);
        String managedByOtherPolicyIndex = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);

        createNewSingletonPolicy(client(), policy, "delete", new DeleteAction(), TimeValue.timeValueHours(12));
        String originalPolicy = policy;
        String otherPolicy = randomValueOtherThan(policy, () -> randomAlphaOfLength(5));
        policy = otherPolicy;
        createNewSingletonPolicy(client(), policy, "delete", new DeleteAction(), TimeValue.timeValueHours(13));

        createIndexWithSettings(client(), managedIndex1, Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, randomIntBetween(1, 10))
            .put(LifecycleSettings.LIFECYCLE_NAME_SETTING.getKey(), originalPolicy));
        createIndexWithSettings(client(), managedIndex2, Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, randomIntBetween(1, 10))
            .put(LifecycleSettings.LIFECYCLE_NAME_SETTING.getKey(), originalPolicy));
        createIndexWithSettings(client(), unmanagedIndex, Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, randomIntBetween(1, 10)));
        createIndexWithSettings(client(), managedByOtherPolicyIndex, Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, randomIntBetween(1, 10))
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
        createNewSingletonPolicy(client(), policy, "hot", new RolloverAction(null, null, null, 1L));
        createIndexWithSettings(
            client(),
            originalIndex,
            alias,
            Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
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
        assertBusy(() -> assertTrue((boolean) explainIndex(client(), originalIndex).getOrDefault("managed", false)));

        // Wait for everything to be copacetic
        assertBusy(() -> assertThat(getStepKeyForIndex(client(), originalIndex), equalTo(PhaseCompleteStep.finalStep("hot").getKey())));
    }

    public void testCanStopILMWithPolicyUsingNonexistentPolicy() throws Exception {
        createIndexWithSettings(client(), index, alias, Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
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
        Request startILMRequest = new Request("POST", "_ilm/start");
        assertOK(client().performRequest(startILMRequest));
    }

    public void testWaitForActiveShardsStep() throws Exception {
        String originalIndex = index + "-000001";
        String secondIndex = index + "-000002";
        createIndexWithSettings(client(), originalIndex, alias, Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put(RolloverAction.LIFECYCLE_ROLLOVER_ALIAS, alias),
            true);

        // create policy
        createNewSingletonPolicy(client(), policy, "hot", new RolloverAction(null, null, null, 1L));
        // update policy on index
        updatePolicy(client(), originalIndex, policy);
        Request createIndexTemplate = new Request("PUT", "_template/rolling_indexes");
        createIndexTemplate.setJsonEntity("{" +
            "\"index_patterns\": [\""+ index + "-*\"], \n" +
            "  \"settings\": {\n" +
            "    \"number_of_shards\": 1,\n" +
            "    \"number_of_replicas\": 142,\n" +
            "    \"index.write.wait_for_active_shards\": \"all\"\n" +
            "  }\n" +
            "}");
        createIndexTemplate.setOptions(expectWarnings(RestPutIndexTemplateAction.DEPRECATION_WARNING));
        client().performRequest(createIndexTemplate);

        // index document to trigger rollover
        index(client(), originalIndex, "_id", "foo", "bar");
        assertBusy(() -> assertTrue(indexExists(secondIndex)));

        assertBusy(() -> assertThat(getStepKeyForIndex(client(), originalIndex).getName(), equalTo(WaitForActiveShardsStep.NAME)));

        // reset the number of replicas to 0 so that the second index wait for active shard condition can be met
        updateIndexSettings(secondIndex, Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0));

        assertBusy(() -> assertThat(getStepKeyForIndex(client(), originalIndex), equalTo(PhaseCompleteStep.finalStep("hot").getKey())));
    }

    public void testHistoryIsWrittenWithSuccess() throws Exception {
        createNewSingletonPolicy(client(), policy, "hot", new RolloverAction(null, null, null, 1L));
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
        createIndexTemplate.setOptions(expectWarnings(RestPutIndexTemplateAction.DEPRECATION_WARNING));
        client().performRequest(createIndexTemplate);

        createIndexWithSettings(client(), index + "-1", alias, Settings.builder(), true);

        // Index a document
        index(client(), index + "-1", "1", "foo", "bar");
        Request refreshIndex = new Request("POST", "/" + index + "-1/_refresh");
        client().performRequest(refreshIndex);

        assertBusy(() -> assertThat(getStepKeyForIndex(client(), index + "-1"), equalTo(PhaseCompleteStep.finalStep("hot").getKey())));

        assertBusy(() -> assertHistoryIsPresent(policy, index + "-1", true, "check-rollover-ready"), 30, TimeUnit.SECONDS);
        assertBusy(() -> assertHistoryIsPresent(policy, index + "-1", true, "attempt-rollover"), 30, TimeUnit.SECONDS);
        assertBusy(() -> assertHistoryIsPresent(policy, index + "-1", true, "set-indexing-complete"), 30, TimeUnit.SECONDS);
        assertBusy(() -> assertHistoryIsPresent(policy, index + "-1", true, "complete"), 30, TimeUnit.SECONDS);

        assertBusy(() -> assertHistoryIsPresent(policy, index + "-000002", true, "check-rollover-ready"), 30, TimeUnit.SECONDS);
    }

    public void testHistoryIsWrittenWithFailure() throws Exception {
        createIndexWithSettings(client(), index + "-1", alias, Settings.builder(), false);
        createNewSingletonPolicy(client(), policy, "hot", new RolloverAction(null, null, null, 1L));
        updatePolicy(client(), index + "-1", policy);

        // Index a document
        index(client(), index + "-1", "1", "foo", "bar");
        Request refreshIndex = new Request("POST", "/" + index + "-1/_refresh");
        client().performRequest(refreshIndex);

        // Check that we've had error and auto retried
        assertBusy(() -> assertThat((Integer) explainIndex(client(), index + "-1").get("failed_step_retry_count"),
            greaterThanOrEqualTo(1)));

        assertBusy(() -> assertHistoryIsPresent(policy, index + "-1", false, "ERROR"), 30, TimeUnit.SECONDS);
    }

    public void testHistoryIsWrittenWithDeletion() throws Exception {
        // Index should be created and then deleted by ILM
        createIndexWithSettings(client(), index, alias, Settings.builder(), false);
        createNewSingletonPolicy(client(), policy, "delete", new DeleteAction());
        updatePolicy(client(), index, policy);

        assertBusy(() -> assertFalse(indexExists(index)));

        assertBusy(() -> {
            assertHistoryIsPresent(policy, index, true, "delete", "delete", "wait-for-shard-history-leases");
            assertHistoryIsPresent(policy, index, true, "delete", "delete", "complete");
        }, 30, TimeUnit.SECONDS);
    }

    public void testRetryableInitializationStep() throws Exception {
        String index = "retryinit-20xx-01-10";
        Request stopReq = new Request("POST", "/_ilm/stop");
        Request startReq = new Request("POST", "/_ilm/start");

        createNewSingletonPolicy(client(), policy, "hot", new SetPriorityAction(1));

        // Stop ILM so that the initialize step doesn't run
        assertOK(client().performRequest(stopReq));

        // Create the index with the origination parsing turn *off* so it doesn't prevent creation
        createIndexWithSettings(
            client(),
            index,
            alias,
            Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put(LifecycleSettings.LIFECYCLE_NAME, policy)
                .put(LifecycleSettings.LIFECYCLE_PARSE_ORIGINATION_DATE, false));

        updateIndexSettings(index, Settings.builder()
            .put(LifecycleSettings.LIFECYCLE_PARSE_ORIGINATION_DATE, true));

        assertOK(client().performRequest(startReq));

        assertBusy(() -> assertThat((Integer) explainIndex(client(), index).get(FAILED_STEP_RETRY_COUNT_FIELD), greaterThanOrEqualTo(1)),
            30, TimeUnit.SECONDS);

        // Turn origination date parsing back off
        updateIndexSettings(index, Settings.builder()
            .put(LifecycleSettings.LIFECYCLE_PARSE_ORIGINATION_DATE, false));

        assertBusy(() -> {
            Map<String, Object> explainResp = explainIndex(client(), index);
            String phase = (String) explainResp.get("phase");
            assertThat(phase, equalTo("hot"));
        });
    }

    public void testRefreshablePhaseJson() throws Exception {
        String index = "refresh-index";

        createNewSingletonPolicy(client(), policy, "hot", new RolloverAction(null, null, null, 100L));
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
        createIndexTemplate.setOptions(expectWarnings(RestPutIndexTemplateAction.DEPRECATION_WARNING));
        client().performRequest(createIndexTemplate);

        createIndexWithSettings(client(), index + "-1", alias,
            Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0),
            true);

        // Index a document
        index(client(), index + "-1", "1", "foo", "bar");

        // Wait for the index to enter the check-rollover-ready step
        assertBusy(() -> assertThat(getStepKeyForIndex(client(), index + "-1").getName(), equalTo(WaitForRolloverReadyStep.NAME)));

        // Update the policy to allow rollover at 1 document instead of 100
        createNewSingletonPolicy(client(), policy, "hot", new RolloverAction(null, null, null, 1L));

        // Index should now have been able to roll over, creating the new index and proceeding to the "complete" step
        assertBusy(() -> assertThat(indexExists(index + "-000002"), is(true)));
        assertBusy(() -> assertThat(getStepKeyForIndex(client(), index + "-1").getName(), equalTo(PhaseCompleteStep.NAME)));
    }

    public void testHaltAtEndOfPhase() throws Exception {
        String index = "halt-index";

        createNewSingletonPolicy(client(), policy, "hot", new SetPriorityAction(100));

        createIndexWithSettings(client(), index, alias,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put(LifecycleSettings.LIFECYCLE_NAME, policy),
            randomBoolean());

        // Wait for the index to finish the "hot" phase
        assertBusy(() -> assertThat(getStepKeyForIndex(client(), index), equalTo(PhaseCompleteStep.finalStep("hot").getKey())));

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

    @SuppressWarnings("unchecked")
    public void testDeleteActionDoesntDeleteSearchableSnapshot() throws Exception {
        String snapshotRepo = randomAlphaOfLengthBetween(4, 10);
        createSnapshotRepo(client(), snapshotRepo, randomBoolean());

        // create policy with cold and delete phases
        Map<String, LifecycleAction> coldActions =
            Map.of(SearchableSnapshotAction.NAME, new SearchableSnapshotAction(snapshotRepo));
        Map<String, Phase> phases = new HashMap<>();
        phases.put("cold", new Phase("cold", TimeValue.ZERO, coldActions));
        phases.put("delete", new Phase("delete", TimeValue.timeValueMillis(10000), singletonMap(DeleteAction.NAME,
            new DeleteAction(false))));
        LifecyclePolicy lifecyclePolicy = new LifecyclePolicy(policy, phases);
        // PUT policy
        XContentBuilder builder = jsonBuilder();
        lifecyclePolicy.toXContent(builder, null);
        final StringEntity entity = new StringEntity(
            "{ \"policy\":" + Strings.toString(builder) + "}", ContentType.APPLICATION_JSON);
        Request createPolicyRequest = new Request("PUT", "_ilm/policy/" + policy);
        createPolicyRequest.setEntity(entity);
        assertOK(client().performRequest(createPolicyRequest));

        createIndexWithSettings(client(), index, alias,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put(LifecycleSettings.LIFECYCLE_NAME, policy),
            randomBoolean());

        String[] snapshotName = new String[1];
        String restoredIndexName = SearchableSnapshotAction.FULL_RESTORED_INDEX_PREFIX + this.index;
        assertTrue(waitUntil(() -> {
            try {
                Map<String, Object> explainIndex = explainIndex(client(), index);
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

        // wait for **both** the restored and the original managed index to not exist anymore
        // (making sure the ILM policy finished - asserting only on the restored index has the
        // problem of a fast executing test code that will have the
        // `assertFalse(indexExists(restoredIndexName))` assertion pass before the restored index
        // ever existed)
        assertBusy(() -> {
            assertFalse(indexExists(index));
            assertFalse(indexExists(restoredIndexName));
        }, 90, TimeUnit.SECONDS);

        assertTrue("the snapshot we generate in the cold phase should not be deleted by the delete phase", waitUntil(() -> {
            try {
                Request getSnapshotsRequest = new Request("GET", "_snapshot/" + snapshotRepo + "/" + snapshotName[0]);
                Response getSnapshotsResponse = client().performRequest(getSnapshotsRequest);
                Map<String, Object> snapshotsResponseMap;
                try (InputStream is = getSnapshotsResponse.getEntity().getContent()) {
                    snapshotsResponseMap = XContentHelper.convertToMap(XContentType.JSON.xContent(), is, true);
                }
                if (snapshotsResponseMap.get("snapshots") != null) {
                    ArrayList<Object> snapshots = (ArrayList<Object>) snapshotsResponseMap.get("snapshots");
                    for (Object snapshot : snapshots) {
                        Map<String, Object> snapshotInfoMap = (Map<String, Object>) snapshot;
                        if (snapshotInfoMap.get("snapshot").equals(snapshotName[0]) &&
                                // wait for the snapshot to be completed (successfully or not) otherwise the teardown might fail
                                SnapshotState.valueOf((String) snapshotInfoMap.get("state")).completed()) {
                            return true;
                        }
                    }
                }
                return false;
            } catch (IOException e) {
                return false;
            }
        }, 30, TimeUnit.SECONDS));
    }

    // This method should be called inside an assertBusy, it has no retry logic of its own
    private void assertHistoryIsPresent(String policyName, String indexName, boolean success, String stepName) throws IOException {
        assertHistoryIsPresent(policyName, indexName, success, null, null, stepName);
    }

    // This method should be called inside an assertBusy, it has no retry logic of its own
    @SuppressWarnings("unchecked")
    private void assertHistoryIsPresent(String policyName, String indexName, boolean success,
                                        @Nullable String phase, @Nullable String action, String stepName) throws IOException {
        logger.info("--> checking for history item [{}], [{}], success: [{}], phase: [{}], action: [{}], step: [{}]",
            policyName, indexName, success, phase, action, stepName);
        final Request historySearchRequest = new Request("GET", "ilm-history*/_search?expand_wildcards=all");
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
                logger.info("--> expected at least 1 hit, got 0. All history for index [{}]: {}", indexName, allResultsMap);
            }
            assertThat(hits, greaterThanOrEqualTo(1));
        } catch (ResponseException e) {
            // Throw AssertionError instead of an exception if the search fails so that assertBusy works as expected
            logger.error(e);
            fail("failed to perform search:" + e.getMessage());
        }

        // Finally, check that the history index is in a good state
        Step.StepKey stepKey = getStepKeyForIndex(client(), DataStream.getDefaultBackingIndexName("ilm-history-5", 1));
        assertEquals("hot", stepKey.getPhase());
        assertEquals(RolloverAction.NAME, stepKey.getAction());
        assertEquals(WaitForRolloverReadyStep.NAME, stepKey.getName());
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

    //adds debug information for waitForSnapshot tests
    private void assertBusy(CheckedRunnable<Exception> runnable, String slmPolicy) throws Exception {
        assertBusy(() -> {
            try {
                runnable.run();
            } catch (AssertionError e) {
                Map<String, Object> slm;
                try (InputStream is = client().performRequest(new Request("GET", "/_slm/policy/" + slmPolicy)).getEntity().getContent()) {
                    slm = XContentHelper.convertToMap(XContentType.JSON.xContent(), is, false);
                } catch (Exception ignored) {
                    slm = new HashMap<>();
                }
                throw new AssertionError("Index:" + explainIndex(client(), index) + "\nSLM:" + slm, e);
            }
        });
    }
}
