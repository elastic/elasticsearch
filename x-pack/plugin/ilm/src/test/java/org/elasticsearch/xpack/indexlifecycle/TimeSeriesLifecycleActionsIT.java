/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.indexlifecycle;

import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.elasticsearch.client.Request;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xpack.core.indexlifecycle.AllocateAction;
import org.elasticsearch.xpack.core.indexlifecycle.DeleteAction;
import org.elasticsearch.xpack.core.indexlifecycle.ForceMergeAction;
import org.elasticsearch.xpack.core.indexlifecycle.LifecycleAction;
import org.elasticsearch.xpack.core.indexlifecycle.LifecyclePolicy;
import org.elasticsearch.xpack.core.indexlifecycle.LifecycleSettings;
import org.elasticsearch.xpack.core.indexlifecycle.Phase;
import org.elasticsearch.xpack.core.indexlifecycle.ReadOnlyAction;
import org.elasticsearch.xpack.core.indexlifecycle.ShrinkAction;
import org.elasticsearch.xpack.core.indexlifecycle.Step.StepKey;
import org.elasticsearch.xpack.core.indexlifecycle.TerminalPolicyStep;
import org.junit.Before;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Supplier;

import static java.util.Collections.singletonMap;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

public class TimeSeriesLifecycleActionsIT extends ESRestTestCase {
    private String index;
    private String policy;

    @Before
    public void refreshIndex() throws IOException {
        index = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        policy = randomAlphaOfLength(5);
        Request request = new Request("PUT", "/_cluster/settings");
        XContentBuilder pollIntervalEntity = JsonXContent.contentBuilder();
        pollIntervalEntity.startObject();
        {
            pollIntervalEntity.startObject("transient");
            {
                pollIntervalEntity.field(LifecycleSettings.LIFECYCLE_POLL_INTERVAL, "1s");
            }pollIntervalEntity.endObject();
        } pollIntervalEntity.endObject();
        request.setJsonEntity(Strings.toString(pollIntervalEntity));
        assertOK(adminClient().performRequest(request));
    }

    public static void updatePolicy(String indexName, String policy) throws IOException {
        Request request = new Request("PUT", "/" + indexName + "/_ilm/" + policy);
        client().performRequest(request);
    }

    public void testAllocateOnlyAllocation() throws Exception {
        createIndexWithSettings(index, Settings.builder().put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 2)
            .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0));
        String allocateNodeName = "node-" + randomFrom(0, 1);
        AllocateAction allocateAction = new AllocateAction(null, null, null, singletonMap("_name", allocateNodeName));
        createNewSingletonPolicy(randomFrom("warm", "cold"), allocateAction);
        updatePolicy(index, policy);
        assertBusy(() -> {
            Map<String, Object> settings = getOnlyIndexSettings(index);
            assertThat(getStepKey(settings), equalTo(TerminalPolicyStep.KEY));
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
            assertThat(getStepKey(settings), equalTo(TerminalPolicyStep.KEY));
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

    public void testReadOnly() throws Exception {
        createIndexWithSettings(index, Settings.builder().put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0));
        createNewSingletonPolicy("warm", new ReadOnlyAction());
        updatePolicy(index, policy);
        assertBusy(() -> {
            Map<String, Object> settings = getOnlyIndexSettings(index);
            assertThat(getStepKey(settings), equalTo(TerminalPolicyStep.KEY));
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
            assertThat(getStepKey(getOnlyIndexSettings(index)), equalTo(TerminalPolicyStep.KEY));
            assertThat(numSegments.get(), equalTo(1));
        });
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
            assertThat(getStepKey(settings), equalTo(TerminalPolicyStep.KEY));
            assertThat(settings.get(IndexMetaData.SETTING_NUMBER_OF_SHARDS), equalTo(String.valueOf(expectedFinalShards)));
        });
    }

    private void createNewSingletonPolicy(String phaseName, LifecycleAction action) throws IOException {
        Phase phase = new Phase(phaseName, TimeValue.ZERO, singletonMap(action.getWriteableName(), action));
        LifecyclePolicy lifecyclePolicy = new LifecyclePolicy(policy, singletonMap(phase.getName(), phase));
        XContentBuilder builder = jsonBuilder();
        lifecyclePolicy.toXContent(builder, null);
        final StringEntity entity = new StringEntity(
            "{ \"policy\":" + Strings.toString(builder) + "}", ContentType.APPLICATION_JSON);
        Request request = new Request("PUT", "_ilm/" + policy);
        request.setEntity(entity);
        client().performRequest(request);
    }

    private void createIndexWithSettings(String index, Settings.Builder settings) throws IOException {
        // create the test-index index
        createIndex(index, settings.build());
        // wait for the shards to initialize
        ensureGreen(index);

    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> getOnlyIndexSettings(String index) throws IOException {
        Map<String, Object> response = (Map<String, Object>) getIndexSettings(index).get(index);
        if (response == null) {
            return Collections.emptyMap();
        }
        return (Map<String, Object>) response.get("settings");
    }

    private StepKey getStepKey(Map<String, Object> settings) {
        String phase = (String) settings.get(LifecycleSettings.LIFECYCLE_PHASE);
        String action = (String) settings.get(LifecycleSettings.LIFECYCLE_ACTION);
        String step = (String) settings.get(LifecycleSettings.LIFECYCLE_STEP);
        return new StepKey(phase, action, step);
    }
}
