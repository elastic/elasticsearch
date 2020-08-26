/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ilm;

import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xpack.core.ilm.AllocateAction;
import org.elasticsearch.xpack.core.ilm.LifecyclePolicy;
import org.elasticsearch.xpack.core.ilm.LifecycleSettings;
import org.elasticsearch.xpack.core.ilm.Phase;
import org.elasticsearch.xpack.core.ilm.PhaseCompleteStep;
import org.elasticsearch.xpack.core.ilm.RolloverAction;
import org.elasticsearch.xpack.core.ilm.Step.StepKey;
import org.elasticsearch.xpack.core.ilm.WaitForRolloverReadyStep;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static java.util.Collections.singletonMap;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

public class ChangePolicyforIndexIT extends ESRestTestCase {

    /**
     * This test aims to prove that an index will finish the current phase on an
     * existing definition when the policy is changed for that index, and that
     * after completing the current phase the new policy will be used for
     * subsequent phases.
     *
     * The test creates two policies, one with a hot phase requiring 1 document
     * to rollover and a warm phase with an impossible allocation action. The
     * second policy has a rollover action requiring 1000 document and a warm
     * phase that moves the index to known nodes that will succeed. An index is
     * created with the first policy set and the test ensures the policy is in
     * the rollover step. It then changes the policy for the index to the second
     * policy. It indexes a single document and checks that the index moves past
     * the hot phase and through the warm phase (proving the hot phase
     * definition from the first policy was used) and then checks the allocation
     * settings from the second policy are set ont he index (proving the second
     * policy was used for the warm phase)
     */
    public void testChangePolicyForIndex() throws Exception {
        String indexName = "test-000001";
        // create policy_1 and policy_2
        Map<String, Phase> phases1 = new HashMap<>();
        phases1.put("hot", new Phase("hot", TimeValue.ZERO, singletonMap(RolloverAction.NAME, new RolloverAction(null, null, 1L))));
        phases1.put("warm", new Phase("warm", TimeValue.ZERO,
                singletonMap(AllocateAction.NAME, new AllocateAction(1, singletonMap("_name", "foobarbaz"), null, null))));
        LifecyclePolicy lifecyclePolicy1 = new LifecyclePolicy("policy_1", phases1);
        Map<String, Phase> phases2 = new HashMap<>();
        phases2.put("hot", new Phase("hot", TimeValue.ZERO, singletonMap(RolloverAction.NAME, new RolloverAction(null, null, 1000L))));
        phases2.put("warm", new Phase("warm", TimeValue.ZERO,
                singletonMap(AllocateAction.NAME, new AllocateAction(1, singletonMap("_name", "integTest-1,integTest-2"), null, null))));
        LifecyclePolicy lifecyclePolicy2 = new LifecyclePolicy("policy_1", phases2);
        // PUT policy_1 and policy_2
        XContentBuilder builder1 = jsonBuilder();
        lifecyclePolicy1.toXContent(builder1, null);
        final StringEntity entity1 = new StringEntity("{ \"policy\":" + Strings.toString(builder1) + "}", ContentType.APPLICATION_JSON);
        Request request1 = new Request("PUT", "_ilm/policy/" + "policy_1");
        request1.setEntity(entity1);
        assertOK(client().performRequest(request1));
        XContentBuilder builder2 = jsonBuilder();
        lifecyclePolicy2.toXContent(builder2, null);
        final StringEntity entity2 = new StringEntity("{ \"policy\":" + Strings.toString(builder2) + "}", ContentType.APPLICATION_JSON);
        Request request2 = new Request("PUT", "_ilm/policy/" + "policy_2");
        request2.setEntity(entity2);
        assertOK(client().performRequest(request2));

        // create the test-index index and set the policy to policy_1
        Settings settings = Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 4)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0).put("index.routing.allocation.include._name", "integTest-0")
                .put(RolloverAction.LIFECYCLE_ROLLOVER_ALIAS, "alias").put(LifecycleSettings.LIFECYCLE_NAME, "policy_1").build();
        Request createIndexRequest = new Request("PUT", "/" + indexName);
        createIndexRequest.setJsonEntity(
                "{\n \"settings\": " + Strings.toString(settings) + ", \"aliases\" : { \"alias\": { \"is_write_index\": true } } }");
        client().performRequest(createIndexRequest);
        // wait for the shards to initialize
        ensureGreen(indexName);

        // Check the index is on the attempt rollover step
        assertBusy(() -> assertStep(indexName, new StepKey("hot", RolloverAction.NAME, WaitForRolloverReadyStep.NAME)));

        // Change the policy to policy_2
        Request changePolicyRequest = new Request("PUT", "/" + indexName + "/_settings");
        final StringEntity changePolicyEntity = new StringEntity("{ \"index.lifecycle.name\": \"policy_2\" }",
                ContentType.APPLICATION_JSON);
        changePolicyRequest.setEntity(changePolicyEntity);
        assertOK(client().performRequest(changePolicyRequest));

        // Check the index is still on the attempt rollover step
        assertBusy(() -> assertStep(indexName, new StepKey("hot", RolloverAction.NAME, WaitForRolloverReadyStep.NAME)));

        // Index a single document
        XContentBuilder document = jsonBuilder().startObject();
        document.field("foo", "bar");
        document.endObject();
        final Request request = new Request("POST", "/" + indexName + "/_doc/1");
        request.setJsonEntity(Strings.toString(document));
        assertOK(client().performRequest(request));

        // Check the index goes to the warm phase and completes
        assertBusy(() -> assertStep(indexName, PhaseCompleteStep.finalStep("warm").getKey()), 30, TimeUnit.SECONDS);

        // Check index is allocated on integTest-1 and integTest-2 as per policy_2
        Map<String, Object> indexSettings = getIndexSettingsAsMap(indexName);
        String includesAllocation = (String) indexSettings.get("index.routing.allocation.include._name");
        assertEquals("integTest-1,integTest-2", includesAllocation);
    }

    private void assertStep(String indexName, StepKey expectedStep) throws IOException {
        Response explainResponse = client().performRequest(new Request("GET", "/" + indexName + "/_ilm/explain"));
        assertOK(explainResponse);
        Map<String, Object> explainResponseMap = entityAsMap(explainResponse);
        @SuppressWarnings("unchecked")
        Map<String, Object> indexExplainResponse = (Map<String, Object>) ((Map<String, Object>) explainResponseMap.get("indices"))
                .get(indexName);
        assertEquals(expectedStep.getPhase(), indexExplainResponse.get("phase"));
        assertEquals(expectedStep.getAction(), indexExplainResponse.get("action"));
        assertEquals(expectedStep.getName(), indexExplainResponse.get("step"));
    }
}
