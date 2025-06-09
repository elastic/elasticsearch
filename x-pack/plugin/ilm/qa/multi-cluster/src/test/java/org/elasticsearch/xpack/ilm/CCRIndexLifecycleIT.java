/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ilm;

import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.util.EntityUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.time.FormatNames;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.ObjectPath;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.ilm.LifecycleAction;
import org.elasticsearch.xpack.core.ilm.LifecyclePolicy;
import org.elasticsearch.xpack.core.ilm.Phase;
import org.elasticsearch.xpack.core.ilm.UnfollowAction;
import org.elasticsearch.xpack.core.ilm.WaitUntilTimeSeriesEndTimePassesStep;

import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.xpack.core.ilm.ShrinkIndexNameSupplier.SHRUNKEN_INDEX_PREFIX;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class CCRIndexLifecycleIT extends ESCCRRestTestCase {

    private static final Logger LOGGER = LogManager.getLogger(CCRIndexLifecycleIT.class);
    private static final String TSDB_INDEX_TEMPLATE = """
        {
            "index_patterns": ["%s*"],
            "data_stream": {},
            "template": {
                "settings":{
                    "index": {
                        "number_of_replicas": 0,
                        "number_of_shards": 1,
                        "routing_path": ["metricset"],
                        "mode": "time_series"
                    },
                    "index.lifecycle.name": "%s"
                },
                "mappings":{
                    "properties": {
                        "@timestamp" : {
                            "type": "date"
                        },
                        "metricset": {
                            "type": "keyword",
                            "time_series_dimension": true
                        },
                        "volume": {
                            "type": "double",
                            "time_series_metric": "gauge"
                        }
                    }
                }
            }
        }""";

    public void testBasicCCRAndILMIntegration() throws Exception {
        String indexName = "logs-1";

        String policyName = "basic-test";
        if ("leader".equals(targetCluster)) {
            putILMPolicy(policyName, "50GB", null, TimeValue.timeValueHours(7 * 24));
            Settings indexSettings = indexSettings(1, 0).put("index.lifecycle.name", policyName)
                .put("index.lifecycle.rollover_alias", "logs")
                .build();
            createIndex(indexName, indexSettings, "", "\"logs\": { }");
            ensureGreen(indexName);
        } else if ("follow".equals(targetCluster)) {
            // Policy with the same name must exist in follower cluster too:
            putILMPolicy(policyName, "50GB", null, TimeValue.timeValueHours(7 * 24));
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

                updateIndexSettings(leaderClient, indexName, Settings.builder().put("index.lifecycle.indexing_complete", true).build());

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
            createIndex(adminClient(), indexName, indexSettings(2, 0).build());
            ensureGreen(indexName);
        } else if ("follow".equals(targetCluster)) {
            createNewSingletonPolicy("unfollow-only", "hot", UnfollowAction.INSTANCE, TimeValue.ZERO);
            followIndex(indexName, indexName);
            ensureGreen(indexName);

            // Create the repository before taking the snapshot.
            Request request = new Request("PUT", "/_snapshot/repo");
            request.setJsonEntity(
                Strings.toString(
                    JsonXContent.contentBuilder()
                        .startObject()
                        .field("type", "fs")
                        .startObject("settings")
                        .field("compress", randomBoolean())
                        .field("location", System.getProperty("tests.path.repo"))
                        .field("max_snapshot_bytes_per_sec", "256b")
                        .endObject()
                        .endObject()
                )
            );
            assertOK(client().performRequest(request));

            try (RestClient leaderClient = buildLeaderClient()) {
                index(leaderClient, indexName, "1");
                assertDocumentExists(leaderClient, indexName, "1");

                updateIndexSettings(leaderClient, indexName, Settings.builder().put("index.lifecycle.indexing_complete", true).build());

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
        String alias = "mymetrics";
        String indexName = "mymetrics-000001";
        String nextIndexName = "mymetrics-000002";
        String policyName = "rollover-test";

        if ("leader".equals(targetCluster)) {
            // Create a policy on the leader
            putILMPolicy(policyName, null, 1, null);
            Request templateRequest = new Request("PUT", "/_index_template/my_template");
            Settings indexSettings = indexSettings(1, 0).put("index.lifecycle.name", policyName)
                .put("index.lifecycle.rollover_alias", alias)
                .build();
            templateRequest.setJsonEntity(
                "{\"index_patterns\":  [\"mymetrics-*\"], \"template\":{\"settings\":  " + Strings.toString(indexSettings) + "}}"
            );
            assertOK(client().performRequest(templateRequest));
        } else if ("follow".equals(targetCluster)) {
            // Policy with the same name must exist in follower cluster too:
            putILMPolicy(policyName, null, 1, null);

            // Set up an auto-follow pattern
            Request createAutoFollowRequest = new Request("PUT", "/_ccr/auto_follow/my_auto_follow_pattern");
            createAutoFollowRequest.setJsonEntity("""
                {
                  "leader_index_patterns": [ "mymetrics-*" ],
                  "remote_cluster": "leader_cluster",
                  "read_poll_timeout": "1000ms"
                }""");
            assertOK(client().performRequest(createAutoFollowRequest));

            try (RestClient leaderClient = buildLeaderClient()) {
                // Create an index on the leader using the template set up above
                Request createIndexRequest = new Request("PUT", "/" + indexName);
                createIndexRequest.setJsonEntity(Strings.format("""
                    {
                      "mappings": {
                        "properties": {
                          "field": {
                            "type": "keyword"
                          }
                        }
                      },
                      "aliases": {
                        "%s": {
                          "is_write_index": true
                        }
                      }
                    }""", alias));
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
                leaderClient.performRequest(new Request("DELETE", "/_index_template/my_template"));
            }
        } else {
            fail("unexpected target cluster [" + targetCluster + "]");
        }
    }

    public void testAliasReplicatedOnShrink() throws Exception {
        final String indexName = "shrink-alias-test";
        final String policyName = "shrink-test-policy";
        final int numberOfAliases = randomIntBetween(0, 4);

        if ("leader".equals(targetCluster)) {
            // this policy won't exist on the leader, that's fine
            Settings indexSettings = indexSettings(3, 0).put("index.lifecycle.name", policyName).build();
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
                updateIndexSettings(leaderClient, indexName, Settings.builder().put("index.lifecycle.indexing_complete", true).build());
            }

            // Wait for the setting to get replicated
            assertBusy(() -> assertThat(getIndexSetting(client(), indexName, "index.lifecycle.indexing_complete"), equalTo("true")));

            assertBusy(() -> assertThat(getShrinkIndexName(client(), indexName), notNullValue()), 30, TimeUnit.SECONDS);
            String shrunkenIndexName = getShrinkIndexName(client(), indexName);

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
        final String policyName = "shrink-test-policy";

        if ("leader".equals(targetCluster)) {
            // this policy won't exist on the leader, that's fine
            Settings indexSettings = indexSettings(3, 0).put("index.lifecycle.name", policyName).build();
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
                updateIndexSettings(leaderClient, indexName, Settings.builder().put("index.lifecycle.indexing_complete", true).build());
            }

            // Wait for the setting to get replicated
            assertBusy(() -> assertThat(getIndexSetting(client(), indexName, "index.lifecycle.indexing_complete"), equalTo("true")));

            // We can't reliably check that the index is unfollowed, because ILM
            // moves through the unfollow and shrink actions so fast that the
            // index often disappears between assertBusy checks

            assertBusy(() -> assertThat(getShrinkIndexName(client(), indexName), notNullValue()), 1, TimeUnit.MINUTES);
            String shrunkenIndexName = getShrinkIndexName(client(), indexName);

            // Wait for the index to continue with its lifecycle and be shrunk
            assertBusy(() -> assertTrue(indexExists(shrunkenIndexName)));

            // Wait for the index to complete its policy
            assertBusy(() -> assertILMPolicy(client(), shrunkenIndexName, policyName, null, "complete", "complete"));
        }
    }

    public void testCannotShrinkLeaderIndex() throws Exception {
        String indexName = "shrink-leader-test";
        String policyName = "shrink-leader-test-policy";
        if ("leader".equals(targetCluster)) {
            // Set up the policy and index, but don't attach the policy yet,
            // otherwise it'll proceed through shrink before we can set up the
            // follower
            putShrinkOnlyPolicy(client(), policyName);
            createIndex(indexName, indexSettings(2, 0).build(), "", "");
            ensureGreen(indexName);
        } else if ("follow".equals(targetCluster)) {

            try (RestClient leaderClient = buildLeaderClient()) {
                // Policy with the same name must exist in follower cluster too:
                putUnfollowOnlyPolicy(client(), policyName);
                followIndex(indexName, indexName);
                ensureGreen(indexName);

                // Now we can set up the leader to use the policy
                Request changePolicyRequest = new Request("PUT", "/" + indexName + "/_settings");
                final StringEntity changePolicyEntity = new StringEntity(
                    "{ \"index.lifecycle.name\": \"" + policyName + "\" }",
                    ContentType.APPLICATION_JSON
                );
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
                updateIndexSettings(leaderClient, indexName, Settings.builder().put("index.lifecycle.indexing_complete", true).build());

                assertBusy(() -> assertThat(getShrinkIndexName(leaderClient, indexName), notNullValue()), 30, TimeUnit.SECONDS);
                String shrunkenIndexName = getShrinkIndexName(leaderClient, indexName);
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
            Settings indexSettings = indexSettings(1, 0).put("index.lifecycle.name", policyName) // this policy won't exist on the leader,
                                                                                                 // that's fine
                .build();
            createIndex(leaderIndex, indexSettings, "", "");
            ensureGreen(leaderIndex);
        } else if ("follow".equals(targetCluster)) {
            try (RestClient leaderClient = buildLeaderClient()) {
                String leaderRemoteClusterSeed = System.getProperty("tests.leader_remote_cluster_seed");
                configureRemoteClusters("other_remote", leaderRemoteClusterSeed);
                assertBusy(() -> {
                    Map<?, ?> localConnection = (Map<?, ?>) toMap(client().performRequest(new Request("GET", "/_remote/info"))).get(
                        "other_remote"
                    );
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
                updateIndexSettings(leaderClient, leaderIndex, Settings.builder().put("index.lifecycle.indexing_complete", true).build());
                assertBusy(
                    () -> { assertThat(getIndexSetting(client(), followerIndex, "index.lifecycle.indexing_complete"), is("true")); }
                );

                // Remove remote cluster alias:
                configureRemoteClusters("other_remote", null);
                assertBusy(() -> {
                    Map<?, ?> localConnection = (Map<?, ?>) toMap(client().performRequest(new Request("GET", "/_remote/info"))).get(
                        "other_remote"
                    );
                    assertThat(localConnection, nullValue());
                });
                // Then add it back with an incorrect seed node:
                // (unfollow api needs a remote cluster alias)
                configureRemoteClusters("other_remote", "localhost:9999");
                assertBusy(() -> {
                    Map<?, ?> localConnection = (Map<?, ?>) toMap(client().performRequest(new Request("GET", "/_remote/info"))).get(
                        "other_remote"
                    );
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
                assertBusy(() -> { assertILMPolicy(client(), followerIndex, policyName, "hot", "complete", "complete"); });

                // Ensure the "follower" index has successfully unfollowed
                assertBusy(() -> { assertThat(getIndexSetting(client(), followerIndex, "index.xpack.ccr.following_index"), nullValue()); });
            }
        }
    }

    @SuppressWarnings("unchecked")
    public void testTsdbLeaderIndexRolloverAndSyncAfterWaitUntilEndTime() throws Exception {
        String indexPattern = "tsdb-index-";
        String dataStream = "tsdb-index-cpu";
        String policyName = "tsdb-policy";

        if ("leader".equals(targetCluster)) {
            putILMPolicy(policyName, null, 1, null);
            Request templateRequest = new Request("PUT", "/_index_template/tsdb_template");
            templateRequest.setJsonEntity(Strings.format(TSDB_INDEX_TEMPLATE, indexPattern, policyName));
            assertOK(client().performRequest(templateRequest));
        } else if ("follow".equals(targetCluster)) {
            putILMPolicy(policyName, null, 1, null);

            Request createAutoFollowRequest = new Request("PUT", "/_ccr/auto_follow/tsdb_index_auto_follow_pattern");
            createAutoFollowRequest.setJsonEntity("""
                {
                    "leader_index_patterns": [ ".ds-tsdb-index-*" ],
                    "remote_cluster": "leader_cluster",
                    "read_poll_timeout": "1000ms",
                    "follow_index_pattern": "{{leader_index}}"
                }""");
            assertOK(client().performRequest(createAutoFollowRequest));

            try (RestClient leaderClient = buildLeaderClient()) {
                String now = DateFormatter.forPattern(FormatNames.STRICT_DATE_OPTIONAL_TIME.getName()).format(Instant.now());
                index(leaderClient, dataStream, "", "@timestamp", now, "volume", 11.0, "metricset", randomAlphaOfLength(5));

                String backingIndexName = getDataStreamBackingIndexNames(leaderClient, "tsdb-index-cpu").get(0);
                assertBusy(() -> { assertOK(client().performRequest(new Request("HEAD", "/" + backingIndexName))); });

                // rollover
                Request rolloverRequest = new Request("POST", "/" + dataStream + "/_rollover");
                rolloverRequest.setJsonEntity("""
                    {
                        "conditions": {
                        "max_docs": "1"
                        }
                    }""");
                leaderClient.performRequest(rolloverRequest);

                assertBusy(() -> {
                    assertThat(
                        "index must wait in the " + WaitUntilTimeSeriesEndTimePassesStep.NAME + " until its end time lapses",
                        explainIndex(client(), backingIndexName).get("step"),
                        is(WaitUntilTimeSeriesEndTimePassesStep.NAME)
                    );

                    assertThat(explainIndex(client(), backingIndexName).get("step_info"), is(notNullValue()));
                    assertThat(
                        (String) ((Map<String, Object>) explainIndex(client(), backingIndexName).get("step_info")).get("message"),
                        containsString("Waiting until the index's time series end time lapses")
                    );
                }, 30, TimeUnit.SECONDS);

                int initialLeaderDocCount = getDocCount(leaderClient, backingIndexName);

                // Add more documents to the leader index while it's in WaitUntilTimeSeriesEndTimePassesStep
                String futureTimestamp = DateFormatter.forPattern(FormatNames.STRICT_DATE_OPTIONAL_TIME.getName())
                    .format(Instant.now().plusSeconds(30));

                for (int i = 0; i < 5; i++) {
                    index(leaderClient, dataStream, "", "@timestamp", futureTimestamp, "volume", 20.0 + i, "metricset", "test-sync-" + i);
                }

                // Verify that new documents are synced to follower while in WaitUntilTimeSeriesEndTimePassesStep
                assertBusy(() -> {
                    int currentLeaderDocCount = getDocCount(leaderClient, backingIndexName);
                    int currentFollowerDocCount = getDocCount(client(), backingIndexName);

                    assertThat(
                        "Leader should have more documents than initially",
                        currentLeaderDocCount,
                        greaterThan(initialLeaderDocCount)
                    );
                    assertThat("Follower should sync new documents from leader", currentFollowerDocCount, equalTo(currentLeaderDocCount));

                    // Also verify the step is still WaitUntilTimeSeriesEndTimePassesStep
                    assertThat(
                        "Index should still be in WaitUntilTimeSeriesEndTimePassesStep",
                        explainIndex(client(), backingIndexName).get("step"),
                        is(WaitUntilTimeSeriesEndTimePassesStep.NAME)
                    );
                }, 30, TimeUnit.SECONDS);
            }
        }
    }

    private void configureRemoteClusters(String name, String leaderRemoteClusterSeed) throws IOException {
        logger.info("Configuring leader remote cluster [{}]", leaderRemoteClusterSeed);
        Request request = new Request("PUT", "/_cluster/settings");
        request.setJsonEntity(
            "{\"persistent\": {\"cluster.remote."
                + name
                + ".seeds\": "
                + (leaderRemoteClusterSeed != null ? Strings.format("\"%s\"", leaderRemoteClusterSeed) : null)
                + "}}"
        );
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

    private static void assertILMPolicy(
        RestClient client,
        String index,
        String policy,
        String expectedPhase,
        String expectedAction,
        String expectedStep
    ) throws IOException {
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
        Phase phase = new Phase(phaseName, after, Map.of(action.getWriteableName(), action));
        LifecyclePolicy lifecyclePolicy = new LifecyclePolicy(policyName, Map.of(phase.getName(), phase));
        XContentBuilder builder = jsonBuilder();
        lifecyclePolicy.toXContent(builder, null);
        final StringEntity entity = new StringEntity("{ \"policy\":" + Strings.toString(builder) + "}", ContentType.APPLICATION_JSON);
        Request request = new Request("PUT", "_ilm/policy/" + policyName);
        request.setEntity(entity);
        client().performRequest(request);
    }

    public static void updatePolicy(String indexName, String policy) throws IOException {

        Request changePolicyRequest = new Request("PUT", "/" + indexName + "/_settings");
        final StringEntity changePolicyEntity = new StringEntity(
            "{ \"index.lifecycle.name\": \"" + policy + "\" }",
            ContentType.APPLICATION_JSON
        );
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

        Map<String, Object> snapResponse = ((List<Map<String, Object>>) responseMap.get("snapshots")).get(0);
        assertThat(snapResponse.get("snapshot"), equalTo(snapshot));
        return (String) snapResponse.get("state");
    }

    @SuppressWarnings("unchecked")
    private static String getShrinkIndexName(RestClient client, String originalIndex) throws InterruptedException, IOException {
        String[] shrunkenIndexName = new String[1];
        waitUntil(() -> {
            try {
                Request explainRequest = new Request(
                    "GET",
                    SHRUNKEN_INDEX_PREFIX + "*" + originalIndex + "," + originalIndex + "/_ilm/explain"
                );
                explainRequest.addParameter("only_errors", Boolean.toString(false));
                explainRequest.addParameter("only_managed", Boolean.toString(false));
                Response response = client.performRequest(explainRequest);
                Map<String, Object> responseMap;
                try (InputStream is = response.getEntity().getContent()) {
                    responseMap = XContentHelper.convertToMap(XContentType.JSON.xContent(), is, true);
                }

                Map<String, Map<String, Object>> indexResponse = ((Map<String, Map<String, Object>>) responseMap.get("indices"));
                Map<String, Object> explainIndexResponse = indexResponse.get(originalIndex);
                if (explainIndexResponse == null) {
                    // maybe we swapped the alias from the original index to the shrunken one already
                    for (Map.Entry<String, Map<String, Object>> indexToExplainMap : indexResponse.entrySet()) {
                        // we don't know the exact name of the shrunken index, but we know it starts with the configured prefix
                        String indexName = indexToExplainMap.getKey();
                        if (indexName.startsWith(SHRUNKEN_INDEX_PREFIX) && indexName.contains(originalIndex)) {
                            explainIndexResponse = indexToExplainMap.getValue();
                            break;
                        }
                    }
                }

                LOGGER.info("--> index {}, explain {}", originalIndex, explainIndexResponse);
                if (explainIndexResponse == null) {
                    return false;
                }
                shrunkenIndexName[0] = (String) explainIndexResponse.get("shrink_index_name");
                return shrunkenIndexName[0] != null;
            } catch (IOException e) {
                return false;
            }
        }, 30, TimeUnit.SECONDS);
        assert shrunkenIndexName[0] != null
            : "lifecycle execution state must contain the target shrink index name for index [" + originalIndex + "]";
        return shrunkenIndexName[0];
    }

    private static Map<String, Object> explainIndex(RestClient client, String indexName) throws IOException {
        RequestOptions consumeWarningsOptions = RequestOptions.DEFAULT.toBuilder()
            .setWarningsHandler(warnings -> warnings.isEmpty() == false && List.of("""
                [indices.lifecycle.rollover.only_if_has_documents] setting was deprecated in Elasticsearch \
                and will be removed in a future release. \
                See the deprecation documentation for the next major version.""").equals(warnings) == false)
            .build();

        Request explainRequest = new Request("GET", indexName + "/_ilm/explain");
        explainRequest.setOptions(consumeWarningsOptions);
        Response response = client.performRequest(explainRequest);
        Map<String, Object> responseMap;
        try (InputStream is = response.getEntity().getContent()) {
            responseMap = XContentHelper.convertToMap(XContentType.JSON.xContent(), is, true);
        }

        @SuppressWarnings("unchecked")
        Map<String, Map<String, Object>> indexResponse = ((Map<String, Map<String, Object>>) responseMap.get("indices"));
        return indexResponse.get(indexName);
    }

    private static int getDocCount(RestClient client, String indexName) throws IOException {
        Request countRequest = new Request("GET", "/" + indexName + "/_count");
        Response response = client.performRequest(countRequest);
        Map<String, Object> result = entityAsMap(response);
        return (int) result.get("count");
    }
}
