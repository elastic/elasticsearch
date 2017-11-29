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
package org.elasticsearch.upgrades;

import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.elasticsearch.Version;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.Response;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.test.rest.yaml.ObjectPath;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.function.Predicate;

import static com.carrotsearch.randomizedtesting.RandomizedTest.randomAsciiOfLength;
import static java.util.Collections.emptyMap;
import static org.elasticsearch.cluster.routing.UnassignedInfo.INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING;
import static org.elasticsearch.cluster.routing.allocation.decider.EnableAllocationDecider.INDEX_ROUTING_ALLOCATION_ENABLE_SETTING;
import static org.elasticsearch.cluster.routing.allocation.decider.MaxRetryAllocationDecider.SETTING_ALLOCATION_MAX_RETRY;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.notNullValue;

public class RecoveryIT extends ESRestTestCase {

    @Override
    protected boolean preserveIndicesUponCompletion() {
        return true;
    }

    @Override
    protected boolean preserveReposUponCompletion() {
        return true;
    }

    private enum CLUSTER_TYPE {
        OLD,
        MIXED,
        UPGRADED;

        public static CLUSTER_TYPE parse(String value) {
            switch (value) {
                case "old_cluster":
                    return OLD;
                case "mixed_cluster":
                    return MIXED;
                case "upgraded_cluster":
                    return UPGRADED;
                    default:
                        throw new AssertionError("unknown cluster type: " + value);
            }
        }
    }

    private final CLUSTER_TYPE clusterType = CLUSTER_TYPE.parse(System.getProperty("tests.rest.suite"));

    @Override
    protected Settings restClientSettings() {
        return Settings.builder().put(super.restClientSettings())
            // increase the timeout here to 90 seconds to handle long waits for a green
            // cluster health. the waits for green need to be longer than a minute to
            // account for delayed shards
            .put(ESRestTestCase.CLIENT_RETRY_TIMEOUT, "90s")
            .put(ESRestTestCase.CLIENT_SOCKET_TIMEOUT, "90s")
            .build();
    }

    public void testHistoryUUIDIsGenerated() throws Exception {
        final String index = "index_history_uuid";
        if (clusterType == CLUSTER_TYPE.OLD) {
            Settings.Builder settings = Settings.builder()
                .put(IndexMetaData.INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), 1)
                .put(IndexMetaData.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 1)
                // if the node with the replica is the first to be restarted, while a replica is still recovering
                // then delayed allocation will kick in. When the node comes back, the master will search for a copy
                // but the recovering copy will be seen as invalid and the cluster health won't return to GREEN
                // before timing out
                .put(INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING.getKey(), "100ms");
            createIndex(index, settings.build());
        } else if (clusterType == CLUSTER_TYPE.UPGRADED) {
            ensureGreen(index);
            Response response = client().performRequest("GET", index + "/_stats", Collections.singletonMap("level", "shards"));
            assertOK(response);
            ObjectPath objectPath = ObjectPath.createFromResponse(response);
            List<Object> shardStats = objectPath.evaluate("indices." + index + ".shards.0");
            assertThat(shardStats, hasSize(2));
            String expectHistoryUUID = null;
            for (int shard = 0; shard < 2; shard++) {
                String nodeID = objectPath.evaluate("indices." + index + ".shards.0." + shard + ".routing.node");
                String historyUUID = objectPath.evaluate("indices." + index + ".shards.0." + shard + ".commit.user_data.history_uuid");
                assertThat("no history uuid found for shard on " + nodeID, historyUUID, notNullValue());
                if (expectHistoryUUID == null) {
                    expectHistoryUUID = historyUUID;
                } else {
                    assertThat("different history uuid found for shard on " + nodeID, historyUUID, equalTo(expectHistoryUUID));
                }
            }
        }
    }

    private int indexDocs(String index, final int idStart, final int numDocs) throws IOException {
        for (int i = 0; i < numDocs; i++) {
            final int id = idStart + i;
            assertOK(client().performRequest("PUT", index + "/test/" + id, emptyMap(),
                new StringEntity("{\"test\": \"test_" + randomAsciiOfLength(2) + "\"}", ContentType.APPLICATION_JSON)));
        }
        return numDocs;
    }

    private Future<Void> asyncIndexDocs(String index, final int idStart, final int numDocs) throws IOException {
        PlainActionFuture<Void> future = new PlainActionFuture<>();
        Thread background = new Thread(new AbstractRunnable() {
            @Override
            public void onFailure(Exception e) {
                future.onFailure(e);
            }

            @Override
            protected void doRun() throws Exception {
                indexDocs(index, idStart, numDocs);
                future.onResponse(null);
            }
        });
        background.start();
        return future;
    }

    public void testRecoveryWithConcurrentIndexing() throws Exception {
        final String index = "recovery_with_concurrent_indexing";
        switch (clusterType) {
            case OLD:
                Settings.Builder settings = Settings.builder()
                    .put(IndexMetaData.INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), 1)
                    .put(IndexMetaData.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 1)
                    // if the node with the replica is the first to be restarted, while a replica is still recovering
                    // then delayed allocation will kick in. When the node comes back, the master will search for a copy
                    // but the recovering copy will be seen as invalid and the cluster health won't return to GREEN
                    // before timing out
                    .put(INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING.getKey(), "100ms")
                    .put(SETTING_ALLOCATION_MAX_RETRY.getKey(), "0"); // fail faster
                createIndex(index, settings.build());
                indexDocs(index, 0, 10);
                ensureGreen(index);
                // make sure that we can index while the replicas are recovering
                updateIndexSetting(index, Settings.builder().put(INDEX_ROUTING_ALLOCATION_ENABLE_SETTING.getKey(), "primaries"));
                break;
            case MIXED:
                updateIndexSetting(index, Settings.builder().put(INDEX_ROUTING_ALLOCATION_ENABLE_SETTING.getKey(), (String)null));
                asyncIndexDocs(index, 10, 50).get();
                ensureGreen(index);
                assertOK(client().performRequest("POST", index + "/_refresh"));
                assertCount(index, "_primary", 60);
                assertCount(index, "_replica", 60);
                // make sure that we can index while the replicas are recovering
                updateIndexSetting(index, Settings.builder().put(INDEX_ROUTING_ALLOCATION_ENABLE_SETTING.getKey(), "primaries"));
                break;
            case UPGRADED:
                updateIndexSetting(index, Settings.builder().put(INDEX_ROUTING_ALLOCATION_ENABLE_SETTING.getKey(), (String)null));
                asyncIndexDocs(index, 60, 50).get();
                ensureGreen(index);
                assertOK(client().performRequest("POST", index + "/_refresh"));
                assertCount(index, "_primary", 110);
                assertCount(index, "_replica", 110);
                break;
            default:
                throw new IllegalStateException("unknown type " + clusterType);
        }
    }

    private void assertCount(final String index, final String preference, final int expectedCount) throws IOException {
        final Response response = client().performRequest("GET", index + "/_count", Collections.singletonMap("preference", preference));
        assertOK(response);
        final int actualCount = Integer.parseInt(ObjectPath.createFromResponse(response).evaluate("count").toString());
        assertThat(actualCount, equalTo(expectedCount));
    }


    private String getNodeId(Predicate<Version> versionPredicate) throws IOException {
        Response response = client().performRequest("GET", "_nodes");
        ObjectPath objectPath = ObjectPath.createFromResponse(response);
        Map<String, Object> nodesAsMap = objectPath.evaluate("nodes");
        for (String id : nodesAsMap.keySet()) {
            Version version = Version.fromString(objectPath.evaluate("nodes." + id + ".version"));
            if (versionPredicate.test(version)) {
                return id;
            }
        }
        return null;
    }


    public void testRelocationWithConcurrentIndexing() throws Exception {
        final String index = "relocation_with_concurrent_indexing";
        switch (clusterType) {
            case OLD:
                Settings.Builder settings = Settings.builder()
                    .put(IndexMetaData.INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), 1)
                    .put(IndexMetaData.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 1)
                    // if the node with the replica is the first to be restarted, while a replica is still recovering
                    // then delayed allocation will kick in. When the node comes back, the master will search for a copy
                    // but the recovering copy will be seen as invalid and the cluster health won't return to GREEN
                    // before timing out
                    .put(INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING.getKey(), "100ms")
                    .put(SETTING_ALLOCATION_MAX_RETRY.getKey(), "0"); // fail faster
                createIndex(index, settings.build());
                indexDocs(index, 0, 10);
                ensureGreen(index);
                // make sure that no shards are allocated, so we can make sure the primary stays on the old node (when one
                // node stops, we lose the master too, so a replica will not be promoted)
                updateIndexSetting(index, Settings.builder().put(INDEX_ROUTING_ALLOCATION_ENABLE_SETTING.getKey(), "none"));
                break;
            case MIXED:
                final String newNode = getNodeId(v -> v.equals(Version.CURRENT));
                final String oldNode = getNodeId(v -> v.before(Version.CURRENT));
                // remove the replica now that we know that the primary is an old node
                updateIndexSetting(index, Settings.builder()
                    .put(IndexMetaData.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 0)
                    .put(INDEX_ROUTING_ALLOCATION_ENABLE_SETTING.getKey(), (String)null)
                    .put("index.routing.allocation.include._id", oldNode)
                );
                updateIndexSetting(index, Settings.builder().put("index.routing.allocation.include._id", newNode));
                asyncIndexDocs(index, 10, 50).get();
                ensureGreen(index);
                assertOK(client().performRequest("POST", index + "/_refresh"));
                assertCount(index, "_primary", 60);
                break;
            case UPGRADED:
                updateIndexSetting(index, Settings.builder()
                    .put(IndexMetaData.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 1)
                    .put("index.routing.allocation.include._id", (String)null)
                );
                asyncIndexDocs(index, 60, 50).get();
                ensureGreen(index);
                assertOK(client().performRequest("POST", index + "/_refresh"));
                assertCount(index, "_primary", 110);
                assertCount(index, "_replica", 110);
                break;
            default:
                throw new IllegalStateException("unknown type " + clusterType);
        }
    }

}
