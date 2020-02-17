/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ilm;

import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.util.EntityUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ObjectPath;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.ccr.ESCCRRestTestCase;
import org.elasticsearch.xpack.core.ilm.LifecycleAction;
import org.elasticsearch.xpack.core.ilm.LifecyclePolicy;
import org.elasticsearch.xpack.core.ilm.Phase;
import org.elasticsearch.xpack.core.ilm.UnfollowAction;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static java.util.Collections.singletonMap;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class CCRIndexLifecycleIT extends ESCCRRestTestCase {

    private static final Logger LOGGER = LogManager.getLogger(CCRIndexLifecycleIT.class);

    public void testBasicCCRAndILMIntegration() throws Exception {
        String indexName = "logs-1";

        String policyName = "basic-test";
        if ("leader".equals(targetCluster)) {
            putILMPolicy(policyName, "50GB", null, TimeValue.timeValueHours(7*24));
            Settings indexSettings = Settings.builder()
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
            ensureGreen(indexName);

            assertBusy(() -> assertOK(client().performRequest(new Request("HEAD", "/" + indexName + "/_alias/logs"))));

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

    public void testCCRUnfollowDuringSnapshot() throws Exception {
        String indexName = "unfollow-test-index";
        if ("leader".equals(targetCluster)) {
            Settings indexSettings = Settings.builder()
                .put("index.number_of_shards", 2)
                .put("index.number_of_replicas", 0)
                .build();
            createIndex(indexName, indexSettings);
            ensureGreen(indexName);
        } else if ("follow".equals(targetCluster)) {
            createNewSingletonPolicy("unfollow-only", "hot", new UnfollowAction(), TimeValue.ZERO);
            followIndex(indexName, indexName);
            ensureGreen(indexName);

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

            try (RestClient leaderClient = buildLeaderClient()) {
                index(leaderClient, indexName, "1");
                assertDocumentExists(leaderClient, indexName, "1");

                updateIndexSettings(leaderClient, indexName, Settings.builder()
                    .put("index.lifecycle.indexing_complete", true)
                    .build());

                // start snapshot
                String snapName = "snapshot-" + randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
                request = new Request("PUT", "/_snapshot/repo/" + snapName);
                request.addParameter("wait_for_completion", "false");
                request.setJsonEntity("{\"indices\": \"" + indexName + "\"}");
                assertOK(client().performRequest(request));

                // add policy and expect it to trigger unfollow immediately (while snapshot in progress)
                logger.info("--> starting unfollow");
                updatePolicy(indexName, "unfollow-only");

                assertBusy(() -> {
                    // Ensure that 'index.lifecycle.indexing_complete' is replicated:
                    assertThat(getIndexSetting(leaderClient, indexName, "index.lifecycle.indexing_complete"), equalTo("true"));
                    assertThat(getIndexSetting(client(), indexName, "index.lifecycle.indexing_complete"), equalTo("true"));
                    // ILM should have unfollowed the follower index, so the following_index setting should have been removed:
                    // (this controls whether the follow engine is used)
                    assertThat(getIndexSetting(client(), indexName, "index.xpack.ccr.following_index"), nullValue());
                    // Following index should have the document
                    assertDocumentExists(client(), indexName, "1");
                    // ILM should have completed the unfollow
                    assertILMPolicy(client(), indexName, "unfollow-only", "hot", "complete", "complete");
                }, 2, TimeUnit.MINUTES);

                // assert that snapshot succeeded
                assertThat(getSnapshotState(snapName), equalTo("SUCCESS"));
                assertOK(client().performRequest(new Request("DELETE", "/_snapshot/repo/" + snapName)));
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
                    "\"mappings\": {\"properties\": {\"field\": {\"type\": \"keyword\"}}}, " +
                    "\"aliases\": {\"" + alias + "\":  {\"is_write_index\":  true}} }");
                assertOK(leaderClient.performRequest(createIndexRequest));
                // Check that the new index is created
                Request checkIndexRequest = new Request("GET", "/_cluster/health/" + indexName);
                checkIndexRequest.addParameter("wait_for_status", "green");
                checkIndexRequest.addParameter("timeout", "70s");
                checkIndexRequest.addParameter("level", "shards");
                assertOK(leaderClient.performRequest(checkIndexRequest));

                // Check that it got replicated to the follower
                assertBusy(() -> assertTrue(indexExists(indexName)));

                // check that the alias was replicated
                assertBusy(() -> assertOK(client().performRequest(new Request("HEAD", "/" + indexName + "/_alias/" + alias))));

                index(leaderClient, indexName, "1");
                assertDocumentExists(leaderClient, indexName, "1");

                ensureGreen(indexName);
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
                    // and the alias should be on the next index
                    assertOK(client().performRequest(new Request("HEAD", "/" + nextIndexName + "/_alias/" + alias)));
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

    public void testAliasReplicatedOnShrink() throws Exception {
        final String indexName = "shrink-alias-test";
        final String shrunkenIndexName = "shrink-" + indexName;
        final String policyName = "shrink-test-policy";

        final int numberOfAliases = randomIntBetween(0, 4);

        if ("leader".equals(targetCluster)) {
            Settings indexSettings = Settings.builder()
                    .put("index.number_of_shards", 3)
                    .put("index.number_of_replicas", 0)
                    .put("index.lifecycle.name", policyName) // this policy won't exist on the leader, that's fine
                    .build();
            final StringBuilder aliases = new StringBuilder();
            boolean first = true;
            for (int i = 0; i < numberOfAliases; i++) {
                if (first == false) {
                    aliases.append(",");
                }
                final Boolean isWriteIndex = randomFrom(new Boolean[] { null, false, true });
                if (isWriteIndex == null) {
                    aliases.append("\"alias_").append(i).append("\":{}");
                } else {
                    aliases.append("\"alias_").append(i).append("\":{\"is_write_index\":").append(isWriteIndex).append("}");
                }
                first = false;
            }
            createIndex(indexName, indexSettings, "", aliases.toString());
            ensureGreen(indexName);
        } else if ("follow".equals(targetCluster)) {
            // Create a policy with just a Shrink action on the follower
            putShrinkOnlyPolicy(client(), policyName);

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

            // Wait for the index to continue with its lifecycle and be shrunk
            assertBusy(() -> assertTrue(indexExists(shrunkenIndexName)));

            // assert the aliases were replicated
            assertBusy(() -> {
                for (int i = 0; i < numberOfAliases; i++) {
                    assertOK(client().performRequest(new Request("HEAD", "/" + shrunkenIndexName + "/_alias/alias_" + i)));
                }
            });
            assertBusy(() -> assertOK(client().performRequest(new Request("HEAD", "/" + shrunkenIndexName + "/_alias/" + indexName))));

            // Wait for the index to complete its policy
            assertBusy(() -> assertILMPolicy(client(), shrunkenIndexName, policyName, null, "complete", "complete"));
        }
    }

    public void testUnfollowInjectedBeforeShrink() throws Exception {
        final String indexName = "shrink-test";
        final String shrunkenIndexName = "shrink-" + indexName;
        final String policyName = "shrink-test-policy";

        if ("leader".equals(targetCluster)) {
            Settings indexSettings = Settings.builder()
                .put("index.number_of_shards", 3)
                .put("index.number_of_replicas", 0)
                .put("index.lifecycle.name", policyName) // this policy won't exist on the leader, that's fine
                .build();
            createIndex(indexName, indexSettings, "", "");
            ensureGreen(indexName);
        } else if ("follow".equals(targetCluster)) {
            // Create a policy with just a Shrink action on the follower
            putShrinkOnlyPolicy(client(), policyName);

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
            assertBusy(() -> assertILMPolicy(client(), shrunkenIndexName, policyName, null, "complete", "complete"));
        }
    }

    public void testCannotShrinkLeaderIndex() throws Exception {
        String indexName = "shrink-leader-test";
        String shrunkenIndexName = "shrink-" + indexName;

        String policyName = "shrink-leader-test-policy";
        if ("leader".equals(targetCluster)) {
            // Set up the policy and index, but don't attach the policy yet,
            // otherwise it'll proceed through shrink before we can set up the
            // follower
            putShrinkOnlyPolicy(client(), policyName);
            Settings indexSettings = Settings.builder()
                .put("index.number_of_shards", 2)
                .put("index.number_of_replicas", 0)
                .build();
            createIndex(indexName, indexSettings, "", "");
            ensureGreen(indexName);
        } else if ("follow".equals(targetCluster)) {

            try (RestClient leaderClient = buildLeaderClient()) {
                // Policy with the same name must exist in follower cluster too:
                putUnfollowOnlyPolicy(client(), policyName);
                followIndex(indexName, indexName);
                ensureGreen(indexName);

                // Now we can set up the leader to use the policy
                Request changePolicyRequest = new Request("PUT", "/" + indexName + "/_settings");
                final StringEntity changePolicyEntity = new StringEntity("{ \"index.lifecycle.name\": \"" + policyName + "\" }",
                    ContentType.APPLICATION_JSON);
                changePolicyRequest.setEntity(changePolicyEntity);
                assertOK(leaderClient.performRequest(changePolicyRequest));

                assertBusy(() -> {
                    // Sanity check that following_index setting has been set, so that we can verify later that this setting has been unset:
                    assertThat(getIndexSetting(client(), indexName, "index.xpack.ccr.following_index"), equalTo("true"));

                    // We should get into a state with these policies where both leader and followers are waiting on each other
                    assertILMPolicy(leaderClient, indexName, policyName, "warm", "shrink", "wait-for-shard-history-leases");
                    assertILMPolicy(client(), indexName, policyName, "hot", "unfollow", "wait-for-indexing-complete");
                });

                // Index a bunch of documents and wait for them to be replicated
                for (int i = 0; i < 50; i++) {
                    index(leaderClient, indexName, Integer.toString(i));
                }
                assertBusy(() -> {
                    for (int i = 0; i < 50; i++) {
                        assertDocumentExists(client(), indexName, Integer.toString(i));
                    }
                });

                // Then make sure both leader and follower are still both waiting
                assertILMPolicy(leaderClient, indexName, policyName, "warm", "shrink", "wait-for-shard-history-leases");
                assertILMPolicy(client(), indexName, policyName, "hot", "unfollow", "wait-for-indexing-complete");

                // Manually set this to kick the process
                updateIndexSettings(leaderClient, indexName, Settings.builder()
                    .put("index.lifecycle.indexing_complete", true)
                    .build()
                );

                assertBusy(() -> {
                    // The shrunken index should now be created on the leader...
                    Response shrunkenIndexExistsResponse = leaderClient.performRequest(new Request("HEAD", "/" + shrunkenIndexName));
                    assertEquals(RestStatus.OK.getStatus(), shrunkenIndexExistsResponse.getStatusLine().getStatusCode());

                    // And both of these should now finish their policies
                    assertILMPolicy(leaderClient, shrunkenIndexName, policyName, null, "complete", "complete");
                    assertILMPolicy(client(), indexName, policyName, "hot", "complete", "complete");
                });
            }
        } else {
            fail("unexpected target cluster [" + targetCluster + "]");
        }

    }

    public void testILMUnfollowFailsToRemoveRetentionLeases() throws Exception {
        final String leaderIndex = "leader";
        final String followerIndex = "follower";
        final String policyName = "unfollow_only_policy";

        if ("leader".equals(targetCluster)) {
            Settings indexSettings = Settings.builder()
                .put("index.number_of_shards", 1)
                .put("index.number_of_replicas", 0)
                .put("index.lifecycle.name", policyName) // this policy won't exist on the leader, that's fine
                .build();
            createIndex(leaderIndex, indexSettings, "", "");
            ensureGreen(leaderIndex);
        } else if ("follow".equals(targetCluster)) {
            try (RestClient leaderClient = buildLeaderClient()) {
                String leaderRemoteClusterSeed = System.getProperty("tests.leader_remote_cluster_seed");
                configureRemoteClusters("other_remote", leaderRemoteClusterSeed);
                assertBusy(() -> {
                    Map<?, ?> localConnection = (Map<?, ?>) toMap(client()
                        .performRequest(new Request("GET", "/_remote/info")))
                        .get("other_remote");
                    assertThat(localConnection, notNullValue());
                    assertThat(localConnection.get("connected"), is(true));
                });
                putUnfollowOnlyPolicy(client(), policyName);
                // Set up the follower
                followIndex("other_remote", leaderIndex, followerIndex);
                ensureGreen(followerIndex);
                // Pause ILM so that this policy doesn't proceed until we want it to
                client().performRequest(new Request("POST", "/_ilm/stop"));

                // Set indexing complete and wait for it to be replicated
                updateIndexSettings(leaderClient, leaderIndex, Settings.builder()
                    .put("index.lifecycle.indexing_complete", true)
                    .build()
                );
                assertBusy(() -> {
                    assertThat(getIndexSetting(client(), followerIndex, "index.lifecycle.indexing_complete"), is("true"));
                });

                // Remove remote cluster alias:
                configureRemoteClusters("other_remote", null);
                assertBusy(() -> {
                    Map<?, ?> localConnection = (Map<?, ?>) toMap(client()
                        .performRequest(new Request("GET", "/_remote/info")))
                        .get("other_remote");
                    assertThat(localConnection, nullValue());
                });
                // Then add it back with an incorrect seed node:
                // (unfollow api needs a remote cluster alias)
                configureRemoteClusters("other_remote", "localhost:9999");
                assertBusy(() -> {
                    Map<?, ?> localConnection = (Map<?, ?>) toMap(client()
                        .performRequest(new Request("GET", "/_remote/info")))
                        .get("other_remote");
                    assertThat(localConnection, notNullValue());
                    assertThat(localConnection.get("connected"), is(false));

                    Request statsRequest = new Request("GET", "/" + followerIndex + "/_ccr/stats");
                    Map<?, ?> response = toMap(client().performRequest(statsRequest));
                    logger.info("follow shards response={}", response);
                    String expectedIndex = ObjectPath.eval("indices.0.index", response);
                    assertThat(expectedIndex, equalTo(followerIndex));
                    Object fatalError = ObjectPath.eval("indices.0.shards.0.read_exceptions.0", response);
                    assertThat(fatalError, notNullValue());
                });

                // Start ILM back up and let it unfollow
                client().performRequest(new Request("POST", "/_ilm/start"));
                // Wait for the policy to be complete
                assertBusy(() -> {
                    assertILMPolicy(client(), followerIndex, policyName, "hot", "complete", "complete");
                });

                // Ensure the "follower" index has successfully unfollowed
                assertBusy(() -> {
                    assertThat(getIndexSetting(client(), followerIndex, "index.xpack.ccr.following_index"), nullValue());
                });
            }
        }
    }

    private void configureRemoteClusters(String name, String leaderRemoteClusterSeed) throws IOException {
        logger.info("Configuring leader remote cluster [{}]", leaderRemoteClusterSeed);
        Request request = new Request("PUT", "/_cluster/settings");
        request.setJsonEntity("{\"persistent\": {\"cluster.remote." + name + ".seeds\": " +
            (leaderRemoteClusterSeed != null ? String.format(Locale.ROOT, "\"%s\"", leaderRemoteClusterSeed) : null) + "}}");
        assertThat(client().performRequest(request).getStatusLine().getStatusCode(), equalTo(200));
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

    private void putShrinkOnlyPolicy(RestClient client, String policyName) throws IOException {
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
        assertOK(client.performRequest(request));
    }

    private void putUnfollowOnlyPolicy(RestClient client, String policyName) throws Exception {
        final XContentBuilder builder = jsonBuilder();
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
                }
                builder.endObject();
            }
            builder.endObject();
        }
        builder.endObject();

        final Request request = new Request("PUT", "_ilm/policy/" + policyName);
        request.setJsonEntity(Strings.toString(builder));
        assertOK(client.performRequest(request));
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
        return Optional.ofNullable((Map<?, ?>) response.get(index))
            .map(m -> (Map<?, ?>) m.get("settings"))
            .map(m -> m.get(setting))
            .orElse(null);
    }

    private void assertDocumentExists(RestClient client, String index, String id) throws IOException {
        Request request = new Request("GET", "/" + index + "/_doc/" + id);
        Response response;
        try {
            response = client.performRequest(request);
            if (response.getStatusLine().getStatusCode() != 200) {
                if (response.getEntity() != null) {
                    logger.error(EntityUtils.toString(response.getEntity()));
                } else {
                    logger.error("response body was null");
                }
                fail("HTTP response code expected to be [200] but was [" + response.getStatusLine().getStatusCode() + "]");
            }
        } catch (ResponseException ex) {
            if (ex.getResponse().getEntity() != null) {
                logger.error(EntityUtils.toString(ex.getResponse().getEntity()), ex);
            } else {
                logger.error("response body was null");
            }
            fail("HTTP response code expected to be [200] but was [" + ex.getResponse().getStatusLine().getStatusCode() + "]");
        }
    }

    private void createNewSingletonPolicy(String policyName, String phaseName, LifecycleAction action, TimeValue after) throws IOException {
        Phase phase = new Phase(phaseName, after, singletonMap(action.getWriteableName(), action));
        LifecyclePolicy lifecyclePolicy = new LifecyclePolicy(policyName, singletonMap(phase.getName(), phase));
        XContentBuilder builder = jsonBuilder();
        lifecyclePolicy.toXContent(builder, null);
        final StringEntity entity = new StringEntity(
            "{ \"policy\":" + Strings.toString(builder) + "}", ContentType.APPLICATION_JSON);
        Request request = new Request("PUT", "_ilm/policy/" + policyName);
        request.setEntity(entity);
        client().performRequest(request);
    }

    public static void updatePolicy(String indexName, String policy) throws IOException {

        Request changePolicyRequest = new Request("PUT", "/" + indexName + "/_settings");
        final StringEntity changePolicyEntity = new StringEntity("{ \"index.lifecycle.name\": \"" + policy + "\" }",
            ContentType.APPLICATION_JSON);
        changePolicyRequest.setEntity(changePolicyEntity);
        assertOK(client().performRequest(changePolicyRequest));
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
}
