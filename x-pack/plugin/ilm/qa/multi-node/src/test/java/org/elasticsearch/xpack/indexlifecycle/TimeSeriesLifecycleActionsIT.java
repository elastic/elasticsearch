/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.indexlifecycle;

import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xpack.core.indexlifecycle.AllocateAction;
import org.elasticsearch.xpack.core.indexlifecycle.DeleteAction;
import org.elasticsearch.xpack.core.indexlifecycle.ErrorStep;
import org.elasticsearch.xpack.core.indexlifecycle.ForceMergeAction;
import org.elasticsearch.xpack.core.indexlifecycle.LifecycleAction;
import org.elasticsearch.xpack.core.indexlifecycle.LifecyclePolicy;
import org.elasticsearch.xpack.core.indexlifecycle.Phase;
import org.elasticsearch.xpack.core.indexlifecycle.ReadOnlyAction;
import org.elasticsearch.xpack.core.indexlifecycle.RolloverAction;
import org.elasticsearch.xpack.core.indexlifecycle.ShrinkAction;
import org.elasticsearch.xpack.core.indexlifecycle.ShrinkStep;
import org.elasticsearch.xpack.core.indexlifecycle.Step.StepKey;
import org.elasticsearch.xpack.core.indexlifecycle.TerminalPolicyStep;
import org.elasticsearch.xpack.core.indexlifecycle.WaitForRolloverReadyStep;
import org.junit.Before;

import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Supplier;

import static java.util.Collections.singletonMap;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.not;

public class TimeSeriesLifecycleActionsIT extends ESRestTestCase {
    private String index;
    private String policy;

    @Before
    public void refreshIndex() {
        index = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        policy = randomAlphaOfLength(5);
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
            .put("index.routing.allocation.include._name", "node-0")
            .put(RolloverAction.LIFECYCLE_ROLLOVER_ALIAS, "alias"));

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
        assertBusy(() -> assertFalse(indexExists(originalIndex)));
        // asserts that the delete phase completed for the managed shrunken index
        assertBusy(() -> assertFalse(indexExists(shrunkenOriginalIndex)));
    }

    public void testMoveToAllocateStep() throws Exception {
        String originalIndex = index + "-000001";
        createIndexWithSettings(originalIndex, Settings.builder().put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 4)
            .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0)
            .put("index.routing.allocation.include._name", "node-0")
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
            .put("index.routing.allocation.include._name", "node-0")
            .put(RolloverAction.LIFECYCLE_ROLLOVER_ALIAS, "alias"));

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
        assertBusy(() -> assertFalse(indexExists(originalIndex)));
        // asserts that the delete phase completed for the managed shrunken index
        assertBusy(() -> assertFalse(indexExists(shrunkenOriginalIndex)));
    }

    public void testRetryFailedShrinkAction() throws Exception {
        int numShards = 6;
        int divisor = randomFrom(2, 3, 6);
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
        assertBusy(() -> {
            logger.error(explainIndex(index));
            assertTrue(indexExists(shrunkenIndex));
            assertTrue(aliasExists(shrunkenIndex, index));
            Map<String, Object> settings = getOnlyIndexSettings(shrunkenIndex);
            assertThat(getStepKeyForIndex(shrunkenIndex), equalTo(TerminalPolicyStep.KEY));
            assertThat(settings.get(IndexMetaData.SETTING_NUMBER_OF_SHARDS), equalTo(String.valueOf(expectedFinalShards)));
            assertThat(settings.get(IndexMetaData.INDEX_BLOCKS_WRITE_SETTING.getKey()), equalTo("true"));
        });
        expectThrows(ResponseException.class, this::indexDocument);
    }

    public void testRolloverAction() throws Exception {
        String originalIndex = index + "-000001";
        String secondIndex = index + "-000002";
        createIndexWithSettings(originalIndex, Settings.builder().put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(RolloverAction.LIFECYCLE_ROLLOVER_ALIAS, "alias"));

        // create policy
        createNewSingletonPolicy("hot", new RolloverAction(null, null, 1L));
        // update policy on index
        updatePolicy(originalIndex, policy);
        // index document {"foo": "bar"} to trigger rollover
        index(client(), originalIndex, "_id", "foo", "bar");
        assertBusy(() -> assertTrue(indexExists(secondIndex)));
        assertBusy(() -> assertTrue(indexExists(originalIndex)));
    }

    public void testRolloverAlreadyExists() throws Exception {
        String originalIndex = index + "-000001";
        String secondIndex = index + "-000002";
        createIndexWithSettings(originalIndex, Settings.builder().put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(RolloverAction.LIFECYCLE_ROLLOVER_ALIAS, "alias"));
        // create policy
        createNewSingletonPolicy("hot", new RolloverAction(null, null, 1L));
        // update policy on index
        updatePolicy(originalIndex, policy);
        // Manually create the new index
        Request request = new Request("PUT", "/" + secondIndex);
        request.setJsonEntity("{\n \"settings\": " + Strings.toString(Settings.builder().put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0).build()) + "}");
        client().performRequest(request);
        // wait for the shards to initialize
        ensureGreen(secondIndex);
        // index another doc to trigger the policy
        index(client(), originalIndex, "_id", "foo", "bar");
        assertBusy(() -> {
            logger.info(originalIndex + ": " + getStepKeyForIndex(originalIndex));
            logger.info(secondIndex + ": " + getStepKeyForIndex(secondIndex));
            assertThat(getStepKeyForIndex(originalIndex), equalTo(new StepKey("hot", RolloverAction.NAME, ErrorStep.NAME)));
            assertThat(getFailedStepForIndex(originalIndex), equalTo(WaitForRolloverReadyStep.NAME));
            assertThat(getReasonForIndex(originalIndex), containsString("already exists"));
        });
    }

    public void testAllocateOnlyAllocation() throws Exception {
        createIndexWithSettings(index, Settings.builder().put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 2)
            .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0));
        String allocateNodeName = "node-" + randomFrom(0, 1);
        AllocateAction allocateAction = new AllocateAction(null, null, null, singletonMap("_name", allocateNodeName));
        createNewSingletonPolicy(randomFrom("warm", "cold"), allocateAction);
        updatePolicy(index, policy);
        assertBusy(() -> {
            assertThat(getStepKeyForIndex(index), equalTo(TerminalPolicyStep.KEY));
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
        createNewSingletonPolicy(randomFrom("warm", "cold"), allocateAction);
        updatePolicy(index, policy);
        assertBusy(() -> {
            Map<String, Object> settings = getOnlyIndexSettings(index);
            assertThat(getStepKeyForIndex(index), equalTo(TerminalPolicyStep.KEY));
            assertThat(settings.get(IndexMetaData.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey()), equalTo(String.valueOf(finalNumReplicas)));
        });
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

    public void testReadOnly() throws Exception {
        createIndexWithSettings(index, Settings.builder().put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0));
        createNewSingletonPolicy("warm", new ReadOnlyAction());
        updatePolicy(index, policy);
        assertBusy(() -> {
            Map<String, Object> settings = getOnlyIndexSettings(index);
            assertThat(getStepKeyForIndex(index), equalTo(TerminalPolicyStep.KEY));
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
            assertThat(getStepKeyForIndex(index), equalTo(TerminalPolicyStep.KEY));
            Map<String, Object> settings = getOnlyIndexSettings(index);
            assertThat(numSegments.get(), equalTo(1));
            assertThat(settings.get(IndexMetaData.INDEX_BLOCKS_WRITE_SETTING.getKey()), equalTo("true"));
        });
        expectThrows(ResponseException.class, this::indexDocument);
    }

    public void testShrinkAction() throws Exception {
        int numShards = 6;
        int divisor = randomFrom(2, 3, 6);
        int expectedFinalShards = numShards / divisor;
        String shrunkenIndex = ShrinkAction.SHRUNKEN_INDEX_PREFIX + index;
        createIndexWithSettings(index, Settings.builder().put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, numShards)
            .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0));
        createNewSingletonPolicy("warm", new ShrinkAction(expectedFinalShards));
        updatePolicy(index, policy);
        assertBusy(() -> {
            assertTrue(indexExists(shrunkenIndex));
            assertTrue(aliasExists(shrunkenIndex, index));
            Map<String, Object> settings = getOnlyIndexSettings(shrunkenIndex);
            assertThat(getStepKeyForIndex(shrunkenIndex), equalTo(TerminalPolicyStep.KEY));
            assertThat(settings.get(IndexMetaData.SETTING_NUMBER_OF_SHARDS), equalTo(String.valueOf(expectedFinalShards)));
            assertThat(settings.get(IndexMetaData.INDEX_BLOCKS_WRITE_SETTING.getKey()), equalTo("true"));
        });
        expectThrows(ResponseException.class, this::indexDocument);
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

    public void testInvalidPolicyNames() throws UnsupportedEncodingException {
        ResponseException ex;

        policy = randomAlphaOfLengthBetween(0,10) + "," + randomAlphaOfLengthBetween(0,10);
        ex = expectThrows(ResponseException.class, () -> createNewSingletonPolicy("delete", new DeleteAction()));
        assertThat(ex.getCause().getMessage(), containsString("invalid policy name"));
        
        policy = randomAlphaOfLengthBetween(0,10) + "%20" + randomAlphaOfLengthBetween(0,10);
        ex = expectThrows(ResponseException.class, () -> createNewSingletonPolicy("delete", new DeleteAction()));
        assertThat(ex.getCause().getMessage(), containsString("invalid policy name"));

        policy = "_" + randomAlphaOfLengthBetween(1, 20);
        ex = expectThrows(ResponseException.class, () -> createNewSingletonPolicy("delete", new DeleteAction()));
        assertThat(ex.getMessage(), containsString("invalid policy name"));

        policy = randomAlphaOfLengthBetween(256, 1000);
        ex = expectThrows(ResponseException.class, () -> createNewSingletonPolicy("delete", new DeleteAction()));
        assertThat(ex.getMessage(), containsString("invalid policy name"));
    }

    private void createFullPolicy(TimeValue hotTime) throws IOException {
        Map<String, LifecycleAction> warmActions = new HashMap<>();
        warmActions.put(ForceMergeAction.NAME, new ForceMergeAction(1));
        warmActions.put(AllocateAction.NAME, new AllocateAction(1, singletonMap("_name", "node-1,node-2"), null, null));
        warmActions.put(ShrinkAction.NAME, new ShrinkAction(1));
        Map<String, Phase> phases = new HashMap<>();
        phases.put("hot", new Phase("hot", hotTime, singletonMap(RolloverAction.NAME,
            new RolloverAction(null, null, 1L))));
        phases.put("warm", new Phase("warm", TimeValue.ZERO, warmActions));
        phases.put("cold", new Phase("cold", TimeValue.ZERO, singletonMap(AllocateAction.NAME,
            new AllocateAction(0, singletonMap("_name", "node-3"), null, null))));
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

    private void createIndexWithSettings(String index, Settings.Builder settings) throws IOException {
        // create the test-index index
        Request request = new Request("PUT", "/" + index);
        request.setJsonEntity("{\n \"settings\": " + Strings.toString(settings.build())
            + ", \"aliases\" : { \"alias\": { \"is_write_index\": true } } }");
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

    private StepKey getStepKeyForIndex(String indexName) throws IOException {
        Map<String, Object> indexResponse = explainIndex(indexName);
        if (indexResponse == null) {
            return new StepKey(null, null, null);
        }

        String phase = (String) indexResponse.get("phase");
        String action = (String) indexResponse.get("action");
        String step = (String) indexResponse.get("step");
        return new StepKey(phase, action, step);
    }

    private String getFailedStepForIndex(String indexName) throws IOException {
        Map<String, Object> indexResponse = explainIndex(indexName);
        if (indexResponse == null) return null;

        return (String) indexResponse.get("failed_step");
    }

    @SuppressWarnings("unchecked")
    private String getReasonForIndex(String indexName) throws IOException {
        Map<String, Object> indexResponse = explainIndex(indexName);
        if (indexResponse == null) return null;

        return ((Map<String, String>) indexResponse.get("step_info")).get("reason");
    }

    private Map<String, Object> explainIndex(String indexName) throws IOException {
        Request explainRequest = new Request("GET", indexName + "/_ilm/explain");
        Response response = client().performRequest(explainRequest);
        Map<String, Object> responseMap;
        try (InputStream is = response.getEntity().getContent()) {
            responseMap = XContentHelper.convertToMap(XContentType.JSON.xContent(), is, true);
        }

        @SuppressWarnings("unchecked") Map<String, Object> indexResponse = ((Map<String, Map<String, Object>>) responseMap.get("indices"))
            .get(indexName);
        return indexResponse;
    }

    private void indexDocument() throws IOException {
        Request indexRequest = new Request("POST", index + "/_doc");
        indexRequest.setEntity(new StringEntity("{\"a\": \"test\"}", ContentType.APPLICATION_JSON));
        Response response = client().performRequest(indexRequest);
        logger.info(response.getStatusLine());
    }
}
