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
import org.elasticsearch.common.unit.TimeValue;
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

        String policyName = "basic-test";
        if ("leader".equals(targetCluster)) {
            putILMPolicy(policyName, "50GB", null, TimeValue.timeValueHours(7*24));
            Settings indexSettings = Settings.builder()
                .put("index.soft_deletes.enabled", true)
                .put("index.number_of_shards", 1)
                .put("index.number_of_replicas", 0)
                .put("index.lifecycle.name", policyName)
                .put("index.lifecycle.rollover_alias", "logs")
                .build();
            createIndex(indexName, indexSettings, "", "\"logs\": { }");
            ensureGreen(indexName);
        } else if ("follow".equals(targetCluster)) {
            // Policy with the same name must exist in follower cluster too:
            putILMPolicy(policyName, "50GB", null, TimeValue.timeValueHours(7*24));
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

                    assertILMPolicy(leaderClient, indexName, policyName, "hot");
                    assertILMPolicy(client(), indexName, policyName, "hot");
                });

                updateIndexSettings(leaderClient, indexName, Settings.builder()
                    .put("index.lifecycle.indexing_complete", true)
                    .build()
                );

                assertBusy(() -> {
                    // Ensure that 'index.lifecycle.indexing_complete' is replicated:
                    assertThat(getIndexSetting(leaderClient, indexName, "index.lifecycle.indexing_complete"), equalTo("true"));
                    assertThat(getIndexSetting(client(), indexName, "index.lifecycle.indexing_complete"), equalTo("true"));

                    assertILMPolicy(leaderClient, indexName, policyName, "warm");
                    assertILMPolicy(client(), indexName, policyName, "warm");

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

    public void testCcrAndIlmWithRollover() throws Exception {
        String alias = "metrics";
        String indexName = "metrics-000001";
        String nextIndexName = "metrics-000002";
        String policyName = "rollover-test";

        if ("leader".equals(targetCluster)) {
            // Create a policy on the leader
            putILMPolicy(policyName, null, 1, null);
            Request templateRequest = new Request("PUT", "_template/my_template");
            Settings indexSettings = Settings.builder()
                .put("index.soft_deletes.enabled", true)
                .put("index.number_of_shards", 1)
                .put("index.number_of_replicas", 0)
                .put("index.lifecycle.name", policyName)
                .put("index.lifecycle.rollover_alias", alias)
                .build();
            templateRequest.setJsonEntity("{\"index_patterns\":  [\"metrics-*\"], \"settings\":  " + Strings.toString(indexSettings) + "}");
            assertOK(client().performRequest(templateRequest));
        } else if ("follow".equals(targetCluster)) {
            // Policy with the same name must exist in follower cluster too:
            putILMPolicy(policyName, null, 1, null);

            // Set up an auto-follow pattern
            Request createAutoFollowRequest = new Request("PUT", "/_ccr/auto_follow/my_auto_follow_pattern");
            createAutoFollowRequest.setJsonEntity("{\"leader_index_patterns\": [\"metrics-*\"], " +
                "\"remote_cluster\": \"leader_cluster\", \"read_poll_timeout\": \"1000ms\"}");
            assertOK(client().performRequest(createAutoFollowRequest));

            try (RestClient leaderClient = buildLeaderClient()) {
                // Create an index on the leader using the template set up above
                Request createIndexRequest = new Request("PUT", "/" + indexName);
                createIndexRequest.setJsonEntity("{" +
                    "\"mappings\": {\"_doc\": {\"properties\": {\"field\": {\"type\": \"keyword\"}}}}, " +
                    "\"aliases\": {\"" + alias + "\":  {\"is_write_index\":  true}} }");
                assertOK(leaderClient.performRequest(createIndexRequest));
                // Check that the new index is creeg
                Request checkIndexRequest = new Request("GET", "/_cluster/health/" + indexName);
                checkIndexRequest.addParameter("wait_for_status", "green");
                checkIndexRequest.addParameter("timeout", "70s");
                checkIndexRequest.addParameter("level", "shards");
                assertOK(leaderClient.performRequest(checkIndexRequest));

                // Check that it got replicated to the follower
                assertBusy(() -> assertTrue(indexExists(indexName)));

                // Aliases are not copied from leader index, so we need to add that for the rollover action in follower cluster:
                client().performRequest(new Request("PUT", "/" + indexName + "/_alias/" + alias));

                index(leaderClient, indexName, "1");
                assertDocumentExists(leaderClient, indexName, "1");

                assertBusy(() -> {
                    assertDocumentExists(client(), indexName, "1");
                    // Sanity check that following_index setting has been set, so that we can verify later that this setting has been unset:
                    assertThat(getIndexSetting(client(), indexName, "index.xpack.ccr.following_index"), equalTo("true"));
                });

                // Wait for the index to roll over on the leader
                assertBusy(() -> {
                    assertOK(leaderClient.performRequest(new Request("HEAD", "/" + nextIndexName)));
                    assertThat(getIndexSetting(leaderClient, indexName, "index.lifecycle.indexing_complete"), equalTo("true"));

                });

                assertBusy(() -> {
                    // Wait for the next index should have been created on the leader
                    assertOK(leaderClient.performRequest(new Request("HEAD", "/" + nextIndexName)));
                    // And the old index should have a write block and indexing complete set
                    assertThat(getIndexSetting(leaderClient, indexName, "index.blocks.write"), equalTo("true"));
                    assertThat(getIndexSetting(leaderClient, indexName, "index.lifecycle.indexing_complete"), equalTo("true"));

                });

                assertBusy(() -> {
                    // Wait for the setting to get replicated to the follower
                    assertThat(getIndexSetting(client(), indexName, "index.lifecycle.indexing_complete"), equalTo("true"));
                });

                assertBusy(() -> {
                    // ILM should have unfollowed the follower index, so the following_index setting should have been removed:
                    // (this controls whether the follow engine is used)
                    assertThat(getIndexSetting(client(), indexName, "index.xpack.ccr.following_index"), nullValue());
                    // The next index should have been created on the follower as well
                    indexExists(nextIndexName);
                });

                assertBusy(() -> {
                    // And the previously-follower index should be in the warm phase
                    assertILMPolicy(client(), indexName, policyName, "warm");
                });

                // Clean up
                leaderClient.performRequest(new Request("DELETE", "/_template/my_template"));
            }
        } else {
            fail("unexpected target cluster [" + targetCluster + "]");
        }
    }

    public void testUnfollowInjectedBeforeShrink() throws Exception {
        final String indexName = "shrink-test";
        final String shrunkenIndexName = "shrink-" + indexName;
        final String policyName = "shrink-test-policy";

        if ("leader".equals(targetCluster)) {
            Settings indexSettings = Settings.builder()
                .put("index.soft_deletes.enabled", true)
                .put("index.number_of_shards", 3)
                .put("index.number_of_replicas", 0)
                .put("index.lifecycle.name", policyName) // this policy won't exist on the leader, that's fine
                .build();
            createIndex(indexName, indexSettings, "", "");
            ensureGreen(indexName);
        } else if ("follow".equals(targetCluster)) {
            // Create a policy with just a Shrink action on the follower
            final XContentBuilder builder = jsonBuilder();
            builder.startObject();
            {
                builder.startObject("policy");
                {
                    builder.startObject("phases");
                    {
                        builder.startObject("warm");
                        {
                            builder.startObject("actions");
                            {
                                builder.startObject("shrink");
                                {
                                    builder.field("number_of_shards", 1);
                                }
                                builder.endObject();
                            }
                            builder.endObject();
                        }
                        builder.endObject();

                        // Sometimes throw in an extraneous unfollow just to check it doesn't break anything
                        if (randomBoolean()) {
                            builder.startObject("cold");
                            {
                                builder.startObject("actions");
                                {
                                    builder.startObject("unfollow");
                                    builder.endObject();
                                }
                                builder.endObject();
                            }
                            builder.endObject();
                        }
                    }
                    builder.endObject();
                }
                builder.endObject();
            }
            builder.endObject();

            final Request request = new Request("PUT", "_ilm/policy/" + policyName);
            request.setJsonEntity(Strings.toString(builder));
            assertOK(client().performRequest(request));

            // Follow the index
            followIndex(indexName, indexName);
            // Make sure it actually took
            assertBusy(() -> assertTrue(indexExists(indexName)));
            // This should now be in the "warm" phase waiting for the index to be ready to unfollow
            assertBusy(() -> assertILMPolicy(client(), indexName, policyName, "warm", "unfollow", "wait-for-indexing-complete"));

            // Set the indexing_complete flag on the leader so the index will actually unfollow
            try (RestClient leaderClient = buildLeaderClient()) {
                updateIndexSettings(leaderClient, indexName, Settings.builder()
                    .put("index.lifecycle.indexing_complete", true)
                    .build()
                );
            }

            // Wait for the setting to get replicated
            assertBusy(() -> assertThat(getIndexSetting(client(), indexName, "index.lifecycle.indexing_complete"), equalTo("true")));

            // We can't reliably check that the index is unfollowed, because ILM
            // moves through the unfollow and shrink actions so fast that the
            // index often disappears between assertBusy checks

            // Wait for the index to continue with its lifecycle and be shrunk
            assertBusy(() -> assertTrue(indexExists(shrunkenIndexName)));

            // Wait for the index to complete its policy
            assertBusy(() -> assertILMPolicy(client(), shrunkenIndexName, policyName, "completed", "completed", "completed"));
        }
    }

    private static void putILMPolicy(String name, String maxSize, Integer maxDocs, TimeValue maxAge) throws IOException {
        final Request request = new Request("PUT", "_ilm/policy/" + name);
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
                            if (maxSize != null) {
                                builder.field("max_size", maxSize);
                            }
                            if (maxAge != null) {
                                builder.field("max_age", maxAge);
                            }
                            if (maxDocs != null) {
                                builder.field("max_docs", maxDocs);
                            }
                            builder.endObject();
                        }
                        if (randomBoolean()) {
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
                            // Sometimes throw in an extraneous unfollow just to check it doesn't break anything
                            if (randomBoolean()) {
                                builder.startObject("unfollow");
                                builder.endObject();
                            }
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

    private static void assertILMPolicy(RestClient client, String index, String policy, String expectedPhase) throws IOException {
        assertILMPolicy(client, index, policy, expectedPhase, null, null);
    }

    private static void assertILMPolicy(RestClient client, String index, String policy, String expectedPhase,
                                        String expectedAction, String expectedStep) throws IOException {
        final Request request = new Request("GET", "/" + index + "/_ilm/explain");
        Map<String, Object> response = toMap(client.performRequest(request));
        LOGGER.info("response={}", response);
        Map<?, ?> explanation = (Map<?, ?>) ((Map<?, ?>) response.get("indices")).get(index);
        assertThat(explanation.get("managed"), is(true));
        assertThat(explanation.get("policy"), equalTo(policy));
        if (expectedPhase != null) {
            assertThat(explanation.get("phase"), equalTo(expectedPhase));
        }
        if (expectedAction != null) {
            assertThat(explanation.get("action"), equalTo(expectedAction));
        }
        if (expectedStep != null) {
            assertThat(explanation.get("step"), equalTo(expectedStep));
        }
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
