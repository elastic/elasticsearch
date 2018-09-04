/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.client;

import org.elasticsearch.action.admin.indices.settings.get.GetSettingsRequest;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.indexlifecycle.AllocateAction;
import org.elasticsearch.client.indexlifecycle.DeleteAction;
import org.elasticsearch.client.indexlifecycle.DeleteLifecyclePolicyRequest;
import org.elasticsearch.client.indexlifecycle.ForceMergeAction;
import org.elasticsearch.client.indexlifecycle.LifecycleAction;
import org.elasticsearch.client.indexlifecycle.LifecycleManagementStatusRequest;
import org.elasticsearch.client.indexlifecycle.LifecycleManagementStatusResponse;
import org.elasticsearch.client.indexlifecycle.LifecyclePolicy;
import org.elasticsearch.client.indexlifecycle.OperationMode;
import org.elasticsearch.client.indexlifecycle.Phase;
import org.elasticsearch.client.indexlifecycle.PutLifecyclePolicyRequest;
import org.elasticsearch.client.indexlifecycle.RolloverAction;
import org.elasticsearch.client.indexlifecycle.ShrinkAction;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.protocol.xpack.indexlifecycle.ExplainLifecycleRequest;
import org.elasticsearch.protocol.xpack.indexlifecycle.ExplainLifecycleResponse;
import org.elasticsearch.protocol.xpack.indexlifecycle.IndexLifecycleExplainResponse;
import org.elasticsearch.protocol.xpack.indexlifecycle.SetIndexLifecyclePolicyRequest;
import org.elasticsearch.protocol.xpack.indexlifecycle.SetIndexLifecyclePolicyResponse;
import org.elasticsearch.protocol.xpack.indexlifecycle.StartILMRequest;
import org.elasticsearch.protocol.xpack.indexlifecycle.StopILMRequest;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.client.indexlifecycle.LifecyclePolicyTests.createRandomPolicy;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class IndexLifecycleIT extends ESRestHighLevelClientTestCase {

    public void testSetIndexLifecyclePolicy() throws Exception {
        String policyName = randomAlphaOfLength(10);
        LifecyclePolicy policy = createRandomPolicy(policyName);
        PutLifecyclePolicyRequest putRequest = new PutLifecyclePolicyRequest(policy);
        assertAcked(execute(putRequest, highLevelClient().indexLifecycle()::putLifecyclePolicy,
            highLevelClient().indexLifecycle()::putLifecyclePolicyAsync));

        createIndex("foo", Settings.builder().put("index.lifecycle.name", "bar").build());
        createIndex("baz", Settings.builder().put("index.lifecycle.name", "eggplant").build());
        SetIndexLifecyclePolicyRequest req = new SetIndexLifecyclePolicyRequest(policyName, "foo", "baz");
        SetIndexLifecyclePolicyResponse response = execute(req, highLevelClient().indexLifecycle()::setIndexLifecyclePolicy,
                highLevelClient().indexLifecycle()::setIndexLifecyclePolicyAsync);
        assertThat(response.hasFailures(), is(false));
        assertThat(response.getFailedIndexes().isEmpty(), is(true));

        GetSettingsRequest getSettingsRequest = new GetSettingsRequest().indices("foo", "baz");
        GetSettingsResponse settingsResponse = highLevelClient().indices().getSettings(getSettingsRequest, RequestOptions.DEFAULT);
        assertThat(settingsResponse.getSetting("foo", "index.lifecycle.name"), equalTo(policyName));
        assertThat(settingsResponse.getSetting("baz", "index.lifecycle.name"), equalTo(policyName));
    }

    public void testStartStopILM() throws Exception {
        String policyName = randomAlphaOfLength(10);
        LifecyclePolicy policy = createRandomPolicy(policyName);
        PutLifecyclePolicyRequest putRequest = new PutLifecyclePolicyRequest(policy);
        assertAcked(execute(putRequest, highLevelClient().indexLifecycle()::putLifecyclePolicy,
            highLevelClient().indexLifecycle()::putLifecyclePolicyAsync));

        createIndex("foo", Settings.builder().put("index.lifecycle.name", "bar").build());
        createIndex("baz", Settings.builder().put("index.lifecycle.name", "eggplant").build());
        createIndex("squash", Settings.EMPTY);

        LifecycleManagementStatusRequest statusRequest = new LifecycleManagementStatusRequest();
        LifecycleManagementStatusResponse statusResponse = execute(
            statusRequest,
            highLevelClient().indexLifecycle()::lifecycleManagementStatus,
            highLevelClient().indexLifecycle()::lifecycleManagementStatusAsync);
        assertEquals(statusResponse.getOperationMode(), OperationMode.RUNNING);

        StopILMRequest stopReq = new StopILMRequest();
        AcknowledgedResponse stopResponse = execute(stopReq, highLevelClient().indexLifecycle()::stopILM,
                highLevelClient().indexLifecycle()::stopILMAsync);
        assertTrue(stopResponse.isAcknowledged());


        statusResponse = execute(statusRequest, highLevelClient().indexLifecycle()::lifecycleManagementStatus,
            highLevelClient().indexLifecycle()::lifecycleManagementStatusAsync);
        assertThat(statusResponse.getOperationMode(),
                Matchers.anyOf(equalTo(OperationMode.STOPPING),
                    equalTo(OperationMode.STOPPED)));

        StartILMRequest startReq = new StartILMRequest();
        AcknowledgedResponse startResponse = execute(startReq, highLevelClient().indexLifecycle()::startILM,
                highLevelClient().indexLifecycle()::startILMAsync);
        assertTrue(startResponse.isAcknowledged());

        statusResponse = execute(statusRequest, highLevelClient().indexLifecycle()::lifecycleManagementStatus,
            highLevelClient().indexLifecycle()::lifecycleManagementStatusAsync);
        assertEquals(statusResponse.getOperationMode(), OperationMode.RUNNING);
    }

    public void testExplainLifecycle() throws Exception {
        Map<String, Phase> lifecyclePhases = new HashMap<>();
        Map<String, LifecycleAction> hotActions = Collections.singletonMap(
            RolloverAction.NAME,
            new RolloverAction(null, TimeValue.timeValueHours(50 * 24), null));
        lifecyclePhases.put("hot", new Phase("hot", randomFrom(TimeValue.ZERO, null), hotActions));

        Map<String, LifecycleAction> warmActions = new HashMap<>();
        warmActions.put(AllocateAction.NAME, new AllocateAction(null, null, null, Collections.singletonMap("_name", "node-1")));
        warmActions.put(ShrinkAction.NAME, new ShrinkAction(1));
        warmActions.put(ForceMergeAction.NAME, new ForceMergeAction(1000));
        lifecyclePhases.put("warm", new Phase("warm", TimeValue.timeValueSeconds(1000), warmActions));

        Map<String, LifecycleAction> coldActions = new HashMap<>();
        coldActions.put(AllocateAction.NAME, new AllocateAction(0, null, null, null));
        lifecyclePhases.put("cold", new Phase("cold", TimeValue.timeValueSeconds(2000), coldActions));

        Map<String, LifecycleAction> deleteActions = Collections.singletonMap(DeleteAction.NAME, new DeleteAction());
        lifecyclePhases.put("delete", new Phase("delete", TimeValue.timeValueSeconds(3000), deleteActions));

        LifecyclePolicy policy = new LifecyclePolicy(randomAlphaOfLength(10), lifecyclePhases);
        PutLifecyclePolicyRequest putRequest = new PutLifecyclePolicyRequest(policy);
        AcknowledgedResponse putResponse = execute(putRequest, highLevelClient().indexLifecycle()::putLifecyclePolicy,
            highLevelClient().indexLifecycle()::putLifecyclePolicyAsync);
        assertTrue(putResponse.isAcknowledged());

        createIndex("foo", Settings.builder().put("index.lifecycle.name", policy.getName()).build());
        createIndex("baz", Settings.builder().put("index.lifecycle.name", policy.getName()).build());
        createIndex("squash", Settings.EMPTY);
        assertBusy(() -> {
            GetSettingsRequest getSettingsRequest = new GetSettingsRequest().indices("foo", "baz");
            GetSettingsResponse settingsResponse = highLevelClient().indices().getSettings(getSettingsRequest, RequestOptions.DEFAULT);
            assertThat(settingsResponse.getSetting("foo", "index.lifecycle.name"), equalTo(policy.getName()));
            assertThat(settingsResponse.getSetting("baz", "index.lifecycle.name"), equalTo(policy.getName()));
            assertThat(settingsResponse.getSetting("foo", "index.lifecycle.phase"), equalTo("hot"));
            assertThat(settingsResponse.getSetting("baz", "index.lifecycle.phase"), equalTo("hot"));
        });

        ExplainLifecycleRequest req = new ExplainLifecycleRequest();
        req.indices("foo", "baz", "squash");
        ExplainLifecycleResponse response = execute(req, highLevelClient().indexLifecycle()::explainLifecycle,
                highLevelClient().indexLifecycle()::explainLifecycleAsync);
        Map<String, IndexLifecycleExplainResponse> indexResponses = response.getIndexResponses();
        assertEquals(3, indexResponses.size());
        IndexLifecycleExplainResponse fooResponse = indexResponses.get("foo");
        assertNotNull(fooResponse);
        assertTrue(fooResponse.managedByILM());
        assertEquals("foo", fooResponse.getIndex());
        assertEquals("hot", fooResponse.getPhase());
        assertEquals("rollover", fooResponse.getAction());
        assertEquals("attempt_rollover", fooResponse.getStep());
        IndexLifecycleExplainResponse bazResponse = indexResponses.get("baz");
        assertNotNull(bazResponse);
        assertTrue(bazResponse.managedByILM());
        assertEquals("baz", bazResponse.getIndex());
        assertEquals("hot", bazResponse.getPhase());
        assertEquals("rollover", bazResponse.getAction());
        assertEquals("attempt_rollover", bazResponse.getStep());
        IndexLifecycleExplainResponse squashResponse = indexResponses.get("squash");
        assertNotNull(squashResponse);
        assertFalse(squashResponse.managedByILM());
        assertEquals("squash", squashResponse.getIndex());
    }

    public void testDeleteLifecycle() throws IOException {
        String policyName = randomAlphaOfLength(10);
        LifecyclePolicy policy = createRandomPolicy(policyName);
        PutLifecyclePolicyRequest putRequest = new PutLifecyclePolicyRequest(policy);
        assertAcked(execute(putRequest, highLevelClient().indexLifecycle()::putLifecyclePolicy,
            highLevelClient().indexLifecycle()::putLifecyclePolicyAsync));

        DeleteLifecyclePolicyRequest deleteRequest = new DeleteLifecyclePolicyRequest(policy.getName());
        assertAcked(execute(deleteRequest, highLevelClient().indexLifecycle()::deleteLifecyclePolicy,
            highLevelClient().indexLifecycle()::deleteLifecyclePolicyAsync));

        // TODO: NORELEASE convert this to using the high level client once there are APIs for it
        Request getLifecycle = new Request("GET", "/_ilm/" + policyName);
        try {
            client().performRequest(getLifecycle);
            fail("index should not exist after being deleted");
        } catch (ResponseException ex) {
            assertEquals(404, ex.getResponse().getStatusLine().getStatusCode());
        }
    }

    public void testPutLifecycle() throws IOException {
        String name = randomAlphaOfLengthBetween(5, 20);
        LifecyclePolicy policy = createRandomPolicy(name);
        PutLifecyclePolicyRequest putRequest = new PutLifecyclePolicyRequest(policy);

        assertAcked(execute(putRequest, highLevelClient().indexLifecycle()::putLifecyclePolicy,
            highLevelClient().indexLifecycle()::putLifecyclePolicyAsync));

        // TODO: NORELEASE convert this to using the high level client once there are APIs for it
        Request getLifecycle = new Request("GET", "/_ilm/" + name);
        Response response = client().performRequest(getLifecycle);
        assertEquals(200, response.getStatusLine().getStatusCode());
    }
}
