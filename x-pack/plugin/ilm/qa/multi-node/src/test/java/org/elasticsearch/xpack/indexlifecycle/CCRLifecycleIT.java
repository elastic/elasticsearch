/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.indexlifecycle;

import org.apache.http.util.EntityUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.test.rest.ESRestTestCase;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class CCRLifecycleIT extends ESRestTestCase {

    private static final Logger LOGGER = LogManager.getLogger(CCRLifecycleIT.class);

    public void testBasicCCRAndILMIntegration() throws Exception {
        setupLocalRemoteCluster();
        putILMPolicy();

        String leaderIndex = "logs-leader-1";
        String followerIndex = "logs-follower-1";

        Settings indexSettings = Settings.builder()
            .put("index.soft_deletes.enabled", true)
            .put("index.number_of_shards", 1)
            .put("index.number_of_replicas", 0)
            .put("index.lifecycle.name", "my_policy")
            .build();
        createIndex(leaderIndex, indexSettings);
        ensureGreen(leaderIndex);
        followIndex(leaderIndex, followerIndex);

        index(leaderIndex, "1");
        assertDocumentExists(leaderIndex, "1");
        assertBusy(() -> {
            assertDocumentExists(followerIndex, "1");
            // Sanity check that following_index setting has been set, so that we can verify later that this setting has been unset:
            assertThat(getIndexSetting(followerIndex, "index.xpack.ccr.following_index"), equalTo("true"));

            assertILMPolicy(leaderIndex, "hot");
            assertILMPolicy(followerIndex, "hot");
        });

        updateIndexSettings(leaderIndex, Settings.builder()
            .put("index.lifecycle.indexing_complete", true)
            .build()
        );
        assertBusy(() -> {
            assertILMPolicy(leaderIndex, "warm");
            assertILMPolicy(followerIndex, "warm");

            // ILM should have placed both indices in the warm phase and there these indices are read-only:
            assertThat(getIndexSetting(leaderIndex, "index.blocks.write"), equalTo("true"));
            assertThat(getIndexSetting(followerIndex, "index.blocks.write"), equalTo("true"));
            // ILM should have unfollowed the follower index, so the following_index setting should have been removed:
            // (this controls whether the follow engine is used)
            assertThat(getIndexSetting(followerIndex, "index.xpack.ccr.following_index"), nullValue());
        });
    }

    private static void putILMPolicy() throws IOException {
        final Request request = new Request("PUT", "_ilm/policy/my_policy");
        XContentBuilder builder = jsonBuilder();
        builder.startObject();
        {
            builder.startObject("policy");
            {
                builder.startObject("phases");
                {
                    builder.startObject("hot");
                    {
                        builder.startObject("actions");
                        {
                            builder.startObject("unfollow");
                            builder.endObject();
                        }
                        builder.endObject();
                    }
                    builder.endObject();
                    builder.startObject("warm");
                    {
                        builder.startObject("actions");
                        {
                            builder.startObject("readonly");
                            builder.endObject();
                        }
                        builder.endObject();
                    }
                    builder.endObject();
                    builder.startObject("delete");
                    {
                        builder.field("min_age", "7d");
                        builder.startObject("actions");
                        {
                            builder.startObject("delete");
                            builder.endObject();
                        }
                        builder.endObject();
                    }
                    builder.endObject();
                }
                builder.endObject();
            }
            builder.endObject();
        }
        builder.endObject();
        request.setJsonEntity(Strings.toString(builder));
        assertOK(client().performRequest(request));
    }

    private static void assertILMPolicy(String index, String expectedPhase) throws IOException {
        final Request request = new Request("GET", "/" + index + "/_ilm/explain");
        Map<String, Object> response = toMap(client().performRequest(request));
        LOGGER.info("response={}", response);
        Map<?, ?> explanation = (Map<?, ?>) ((Map<?, ?>) response.get("indices")).get(index);
        assertThat(explanation.get("managed"), is(true));
        assertThat(explanation.get("policy"), equalTo("my_policy"));
        assertThat(explanation.get("phase"), equalTo(expectedPhase));
    }

    private static void followIndex(String leaderIndex, String followIndex) throws IOException {
        final Request request = new Request("PUT", "/" + followIndex + "/_ccr/follow");
        request.setJsonEntity("{\"remote_cluster\": \"local\", \"leader_index\": \"" + leaderIndex +
            "\", \"read_poll_timeout\": \"10ms\"}");
        assertOK(client().performRequest(request));
    }

    private static void updateIndexSettings(String index, Settings settings) throws IOException {
        final Request request = new Request("PUT", "/" + index + "/_settings");
        request.setJsonEntity(Strings.toString(settings));
        assertOK(client().performRequest(request));
    }

    private static Object getIndexSetting(String index, String setting) throws IOException {
        Request request = new Request("GET", "/" + index + "/_settings");
        request.addParameter("flat_settings", "true");
        Map<String, Object> response = toMap(client().performRequest(request));
        Map<?, ?> settings = (Map<?, ?>) ((Map<?, ?>) response.get(index)).get("settings");
        return settings.get(setting);
    }

    private static void index(String index, String id) throws IOException {
        Request request = new Request("POST", "/" + index + "/_doc/" + id);
        request.setJsonEntity("{}");
        assertOK(client().performRequest(request));
    }

    private static void assertDocumentExists(String index, String id) throws IOException {
        Request request = new Request("HEAD", "/" + index + "/_doc/" + id);
        Response response = client().performRequest(request);
        assertThat(response.getStatusLine().getStatusCode(), equalTo(200));
    }

    private static void setupLocalRemoteCluster() throws IOException {
        Request request = new Request("GET", "/_nodes");
        Map<?, ?> nodesResponse = (Map<?, ?>) toMap(client().performRequest(request)).get("nodes");
        // Select node info of first node (we don't know the node id):
        nodesResponse = (Map<?, ?>) nodesResponse.get(nodesResponse.keySet().iterator().next());
        String transportAddress = (String) nodesResponse.get("transport_address");

        LOGGER.info("Configuring local remote cluster [{}]", transportAddress);
        request = new Request("PUT", "/_cluster/settings");
        request.setJsonEntity("{\"persistent\": {\"cluster.remote.local.seeds\": \"" + transportAddress + "\"}}");
        assertThat(client().performRequest(request).getStatusLine().getStatusCode(), equalTo(200));
    }

    private static Map<String, Object> toMap(Response response) throws IOException {
        return XContentHelper.convertToMap(JsonXContent.jsonXContent, EntityUtils.toString(response.getEntity()), false);
    }

}
