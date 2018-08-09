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

import org.apache.http.HttpEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsRequest;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.protocol.xpack.indexlifecycle.ExplainLifecycleRequest;
import org.elasticsearch.protocol.xpack.indexlifecycle.ExplainLifecycleResponse;
import org.elasticsearch.protocol.xpack.indexlifecycle.IndexLifecycleExplainResponse;
import org.elasticsearch.protocol.xpack.indexlifecycle.SetIndexLifecyclePolicyRequest;
import org.elasticsearch.protocol.xpack.indexlifecycle.SetIndexLifecyclePolicyResponse;

import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class IndexLifecycleIT extends ESRestHighLevelClientTestCase {

    public void testSetIndexLifecyclePolicy() throws Exception {
        String policy = randomAlphaOfLength(10);

        // TODO: NORELEASE convert this to using the high level client once there are APIs for it
        String jsonString = "{\n" +
            "   \"policy\": {\n" +
            "     \"type\": \"timeseries\",\n" +
            "     \"phases\": {\n" +
            "       \"hot\": {\n" +
            "         \"after\": \"60s\",\n" +
            "         \"actions\": {\n" +
            "          \"rollover\": {\n" +
            "            \"max_age\": \"500s\"\n" +
            "          }        \n" +
            "         }\n" +
            "       },\n" +
            "       \"warm\": {\n" +
            "         \"after\": \"1000s\",\n" +
            "         \"actions\": {\n" +
            "           \"allocate\": {\n" +
            "             \"require\": { \"_name\": \"node-1\" },\n" +
            "             \"include\": {},\n" +
            "             \"exclude\": {}\n" +
            "           },\n" +
            "           \"shrink\": {\n" +
            "             \"number_of_shards\": 1\n" +
            "           },\n" +
            "           \"forcemerge\": {\n" +
            "             \"max_num_segments\": 1000\n" +
            "           }\n" +
            "         }\n" +
            "       },\n" +
            "       \"cold\": {\n" +
            "         \"after\": \"2000s\",\n" +
            "         \"actions\": {\n" +
            "          \"allocate\": {\n" +
            "            \"number_of_replicas\": 0\n" +
            "          }\n" +
            "         }\n" +
            "       },\n" +
            "       \"delete\": {\n" +
            "         \"after\": \"3000s\",\n" +
            "         \"actions\": {\n" +
            "           \"delete\": {}\n" +
            "         }\n" +
            "       }\n" +
            "     }\n" +
            "   }\n" +
            "}";
        HttpEntity entity = new NStringEntity(jsonString, ContentType.APPLICATION_JSON);
        Request request = new Request("PUT", "/_ilm/" + policy);
        request.setEntity(entity);
        client().performRequest(request);

        createIndex("foo", Settings.builder().put("index.lifecycle.name", "bar").build());
        createIndex("baz", Settings.builder().put("index.lifecycle.name", "eggplant").build());
        SetIndexLifecyclePolicyRequest req = new SetIndexLifecyclePolicyRequest(policy, "foo", "baz");
        SetIndexLifecyclePolicyResponse response = execute(req, highLevelClient().indexLifecycle()::setIndexLifecyclePolicy,
                highLevelClient().indexLifecycle()::setIndexLifecyclePolicyAsync);
        assertThat(response.hasFailures(), is(false));
        assertThat(response.getFailedIndexes().isEmpty(), is(true));

        GetSettingsRequest getSettingsRequest = new GetSettingsRequest().indices("foo", "baz");
        GetSettingsResponse settingsResponse = highLevelClient().indices().getSettings(getSettingsRequest, RequestOptions.DEFAULT);
        assertThat(settingsResponse.getSetting("foo", "index.lifecycle.name"), equalTo(policy));
        assertThat(settingsResponse.getSetting("baz", "index.lifecycle.name"), equalTo(policy));
    }
    
    public void testExplainLifecycle() throws Exception {
        String policy = randomAlphaOfLength(10);

        // TODO: NORELEASE convert this to using the high level client once there are APIs for it
        String jsonString = "{\n" +
            "   \"policy\": {\n" +
            "     \"type\": \"timeseries\",\n" +
            "     \"phases\": {\n" +
            "       \"hot\": {\n" +
            "         \"actions\": {\n" +
            "          \"rollover\": {\n" +
            "            \"max_age\": \"50d\"\n" +
            "          }        \n" +
            "         }\n" +
            "       },\n" +
            "       \"warm\": {\n" +
            "         \"after\": \"1000s\",\n" +
            "         \"actions\": {\n" +
            "           \"allocate\": {\n" +
            "             \"require\": { \"_name\": \"node-1\" },\n" +
            "             \"include\": {},\n" +
            "             \"exclude\": {}\n" +
            "           },\n" +
            "           \"shrink\": {\n" +
            "             \"number_of_shards\": 1\n" +
            "           },\n" +
            "           \"forcemerge\": {\n" +
            "             \"max_num_segments\": 1000\n" +
            "           }\n" +
            "         }\n" +
            "       },\n" +
            "       \"cold\": {\n" +
            "         \"after\": \"2000s\",\n" +
            "         \"actions\": {\n" +
            "          \"allocate\": {\n" +
            "            \"number_of_replicas\": 0\n" +
            "          }\n" +
            "         }\n" +
            "       },\n" +
            "       \"delete\": {\n" +
            "         \"after\": \"3000s\",\n" +
            "         \"actions\": {\n" +
            "           \"delete\": {}\n" +
            "         }\n" +
            "       }\n" +
            "     }\n" +
            "   }\n" +
            "}";
        HttpEntity entity = new NStringEntity(jsonString, ContentType.APPLICATION_JSON);
        Request request = new Request("PUT", "/_ilm/" + policy);
        request.setEntity(entity);
        client().performRequest(request);

        createIndex("foo", Settings.builder().put("index.lifecycle.name", policy).build());
        createIndex("baz", Settings.builder().put("index.lifecycle.name", policy).build());
        createIndex("squash", Settings.EMPTY);
        assertBusy(() -> {
            GetSettingsRequest getSettingsRequest = new GetSettingsRequest().indices("foo", "baz");
            GetSettingsResponse settingsResponse = highLevelClient().indices().getSettings(getSettingsRequest, RequestOptions.DEFAULT);
            assertThat(settingsResponse.getSetting("foo", "index.lifecycle.name"), equalTo(policy));
            assertThat(settingsResponse.getSetting("baz", "index.lifecycle.name"), equalTo(policy));
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
}
