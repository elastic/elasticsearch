/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.indexlifecycle;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.xpack.ccr.ESCCRRestTestCase;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class CCRIndexLifecycleIT extends ESCCRRestTestCase {

    private static final Logger LOGGER = LogManager.getLogger(CCRIndexLifecycleIT.class);

    public void testBasicCCRAndILMIntegration() throws Exception {
        String indexName = "logs-1";

        if ("leader".equals(targetCluster)) {
            putILMPolicy();
            Settings indexSettings = Settings.builder()
                .put("index.soft_deletes.enabled", true)
                .put("index.number_of_shards", 1)
                .put("index.number_of_replicas", 0)
                .put("index.lifecycle.name", "my_policy")
                .put("index.lifecycle.rollover_alias", "logs")
                .build();
            createIndex(indexName, indexSettings, "", "\"logs\": { }");
            ensureGreen(indexName);
        } else if ("follow".equals(targetCluster)) {
            // Policy with the same name must exist in follower cluster too:
            putILMPolicy();
            followIndex(indexName, indexName);
            // Aliases are not copied from leader index, so we need to add that for the rollover action in follower cluster:
            client().performRequest(new Request("PUT", "/" + indexName + "/_alias/logs"));

            try (RestClient leaderClient = buildLeaderClient()) {
                index(leaderClient, indexName, "1");
                assertDocumentExists(leaderClient, indexName, "1");

                assertBusy(() -> {
                    assertDocumentExists(client(), indexName, "1");
                    // Sanity check that following_index setting has been set, so that we can verify later that this setting has been unset:
                    assertThat(getIndexSetting(client(), indexName, "index.xpack.ccr.following_index"), equalTo("true"));

                    assertILMPolicy(leaderClient, indexName, "hot");
                    assertILMPolicy(client(), indexName, "hot");
                });

                updateIndexSettings(leaderClient, indexName, Settings.builder()
                    .put("index.lifecycle.indexing_complete", true)
                    .build()
                );

                assertBusy(() -> {
                    // Ensure that 'index.lifecycle.indexing_complete' is replicated:
                    assertThat(getIndexSetting(leaderClient, indexName, "index.lifecycle.indexing_complete"), equalTo("true"));
                    assertThat(getIndexSetting(client(), indexName, "index.lifecycle.indexing_complete"), equalTo("true"));

                    assertILMPolicy(leaderClient, indexName, "warm");
                    assertILMPolicy(client(), indexName, "warm");

                    // ILM should have placed both indices in the warm phase and there these indices are read-only:
                    assertThat(getIndexSetting(leaderClient, indexName, "index.blocks.write"), equalTo("true"));
                    assertThat(getIndexSetting(client(), indexName, "index.blocks.write"), equalTo("true"));
                    // ILM should have unfollowed the follower index, so the following_index setting should have been removed:
                    // (this controls whether the follow engine is used)
                    assertThat(getIndexSetting(client(), indexName, "index.xpack.ccr.following_index"), nullValue());
                });
            }
        } else {
            fail("unexpected target cluster [" + targetCluster + "]");
        }
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
                            builder.startObject("rollover");
                            builder.field("max_size", "50GB");
                            builder.field("max_age", "7d");
                            builder.endObject();
                        }
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

    private static void assertILMPolicy(RestClient client, String index, String expectedPhase) throws IOException {
        final Request request = new Request("GET", "/" + index + "/_ilm/explain");
        Map<String, Object> response = toMap(client.performRequest(request));
        LOGGER.info("response={}", response);
        Map<?, ?> explanation = (Map<?, ?>) ((Map<?, ?>) response.get("indices")).get(index);
        assertThat(explanation.get("managed"), is(true));
        assertThat(explanation.get("policy"), equalTo("my_policy"));
        assertThat(explanation.get("phase"), equalTo(expectedPhase));
    }

    private static void updateIndexSettings(RestClient client, String index, Settings settings) throws IOException {
        final Request request = new Request("PUT", "/" + index + "/_settings");
        request.setJsonEntity(Strings.toString(settings));
        assertOK(client.performRequest(request));
    }

    private static Object getIndexSetting(RestClient client, String index, String setting) throws IOException {
        Request request = new Request("GET", "/" + index + "/_settings");
        request.addParameter("flat_settings", "true");
        Map<String, Object> response = toMap(client.performRequest(request));
        Map<?, ?> settings = (Map<?, ?>) ((Map<?, ?>) response.get(index)).get("settings");
        return settings.get(setting);
    }

    private static void assertDocumentExists(RestClient client, String index, String id) throws IOException {
        Request request = new Request("HEAD", "/" + index + "/_doc/" + id);
        Response response = client.performRequest(request);
        assertThat(response.getStatusLine().getStatusCode(), equalTo(200));
    }

}
