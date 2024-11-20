/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.upgrades;

import com.carrotsearch.randomizedtesting.annotations.Name;

import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.test.rest.RestTestLegacyFeatures;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.cluster.routing.UnassignedInfo.INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING;
import static org.elasticsearch.cluster.routing.allocation.decider.MaxRetryAllocationDecider.SETTING_ALLOCATION_MAX_RETRY;
import static org.elasticsearch.upgrades.UpgradeWithOldIndexSettingsIT.updateIndexSettingsPermittingSlowlogDeprecationWarning;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.notNullValue;

public class SnapshotBasedRecoveryIT extends AbstractRollingUpgradeTestCase {

    public SnapshotBasedRecoveryIT(@Name("upgradedNodes") int upgradedNodes) {
        super(upgradedNodes);
    }

    public void testSnapshotBasedRecovery() throws Exception {
        assumeTrue(
            "Cancel shard allocation command is broken for initial versions of the desired_balance allocator",
            oldClusterHasFeature(RestTestLegacyFeatures.DESIRED_BALANCED_ALLOCATOR_SUPPORTED) == false
                || oldClusterHasFeature(RestTestLegacyFeatures.DESIRED_BALANCED_ALLOCATOR_FIXED)
        );

        final String indexName = "snapshot_based_recovery";
        final String repositoryName = "snapshot_based_recovery_repo";
        final int numDocs = 200;
        if (isOldCluster()) {
            Settings.Builder settings = indexSettings(1, 0).put(INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING.getKey(), "100ms")
                .put(SETTING_ALLOCATION_MAX_RETRY.getKey(), "0"); // fail faster
            createIndex(indexName, settings.build());
            ensureGreen(indexName);
            indexDocs(indexName, numDocs);
            flush(indexName, true);
            registerRepository(
                repositoryName,
                "fs",
                true,
                Settings.builder()
                    .put("location", "./snapshot_based_recovery")
                    .put(BlobStoreRepository.USE_FOR_PEER_RECOVERY_SETTING.getKey(), true)
                    .build()
            );
            createSnapshot(repositoryName, "snap", true);
            updateIndexSettings(indexName, Settings.builder().put(IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 1));
            ensureGreen(indexName);
        } else {
            if (isFirstMixedCluster()) {
                List<String> upgradedNodeIds = getUpgradedNodeIds();
                // It's possible that the test simply does a rolling-restart, i.e. it "upgrades" to
                // the same version. In that case we proceed without excluding any node
                if (upgradedNodeIds.isEmpty() == false) {
                    assertThat(upgradedNodeIds.size(), is(equalTo(1)));
                    String upgradedNodeId = upgradedNodeIds.get(0);
                    logger.info("--> excluding [{}] from node [{}]", indexName, upgradedNodeId);
                    updateIndexSettings(indexName, Settings.builder().put("index.routing.allocation.exclude._id", upgradedNodeId));
                    ensureGreen(indexName);
                    logger.info("--> finished excluding [{}] from node [{}]", indexName, upgradedNodeId);
                } else {
                    logger.info("--> no upgrading nodes, not adding any exclusions for [{}]", indexName);
                }

                String primaryNodeId = getPrimaryNodeIdOfShard(indexName, 0);
                String primaryNodeVersion = getNodeVersion(primaryNodeId);

                // Sometimes the primary shard ends on the upgraded node (i.e. after a rebalance)
                // This causes issues when removing and adding replicas, since then we cannot allocate to any of the old nodes.
                // That is an issue only for the first mixed round.
                // In that case we exclude the upgraded node from the shard allocation and cancel the shard to force moving
                // the primary to a node in the old version, this allows adding replicas in the first mixed round.
                logger.info("--> Primary node in first mixed round {} / {}", primaryNodeId, primaryNodeVersion);
                if (isOldClusterVersion(primaryNodeVersion) == false) {
                    logger.info("--> cancelling primary shard on node [{}]", primaryNodeId);
                    cancelShard(indexName, 0, primaryNodeId);
                    logger.info("--> done cancelling primary shard on node [{}]", primaryNodeId);

                    String currentPrimaryNodeId = getPrimaryNodeIdOfShard(indexName, 0);
                    assertTrue(isOldClusterVersion(getNodeVersion(currentPrimaryNodeId)));
                }
            } else {
                logger.info("--> not in first upgrade round, removing exclusions for [{}]", indexName);
                updateIndexSettings(indexName, Settings.builder().putNull("index.routing.allocation.exclude._id"));
                logger.info("--> done removing exclusions for [{}]", indexName);
            }

            // Drop replicas
            logger.info("--> dropping replicas from [{}]", indexName);
            updateIndexSettingsPermittingSlowlogDeprecationWarning(
                indexName,
                Settings.builder().put(IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 0)
            );
            logger.info("--> finished dropping replicas from [{}], adding them back", indexName);
            updateIndexSettingsPermittingSlowlogDeprecationWarning(
                indexName,
                Settings.builder().put(IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 1)
            );
            logger.info("--> finished adding replicas from [{}]", indexName);
            ensureGreen(indexName);

            assertMatchAllReturnsAllDocuments(indexName, numDocs);
            assertMatchQueryReturnsAllDocuments(indexName, numDocs);
        }
    }

    private List<String> getUpgradedNodeIds() throws IOException {
        Request request = new Request(HttpGet.METHOD_NAME, "_nodes/_all");
        Response response = client().performRequest(request);
        Map<String, Object> responseMap = responseAsMap(response);
        Map<String, Map<String, Object>> nodes = extractValue(responseMap, "nodes");
        List<String> upgradedNodes = new ArrayList<>();
        for (Map.Entry<String, Map<String, Object>> nodeInfoEntry : nodes.entrySet()) {
            String nodeVersion = extractValue(nodeInfoEntry.getValue(), "version");
            if (isOldClusterVersion(nodeVersion) == false) {
                upgradedNodes.add(nodeInfoEntry.getKey());
            }
        }
        return upgradedNodes;
    }

    private String getNodeVersion(String primaryNodeId) throws IOException {
        Request request = new Request(HttpGet.METHOD_NAME, "_nodes/" + primaryNodeId);
        Response response = client().performRequest(request);
        return extractValue(responseAsMap(response), "nodes." + primaryNodeId + ".version");
    }

    private String getPrimaryNodeIdOfShard(String indexName, int shard) throws Exception {
        String primaryNodeId;
        try (XContentBuilder builder = jsonBuilder()) {
            builder.startObject();
            {
                builder.field("index", indexName);
                builder.field("shard", shard);
                builder.field("primary", true);
            }
            builder.endObject();

            Request request = new Request(HttpGet.METHOD_NAME, "_cluster/allocation/explain");
            request.setJsonEntity(Strings.toString(builder));

            Response response = client().performRequest(request);
            Map<String, Object> responseMap = responseAsMap(response);
            primaryNodeId = extractValue(responseMap, "current_node.id");
        }
        assertThat(primaryNodeId, is(notNullValue()));

        return primaryNodeId;
    }

    private void cancelShard(String indexName, int shard, String nodeName) throws IOException {
        try (XContentBuilder builder = jsonBuilder()) {
            builder.startObject();
            {
                builder.startArray("commands");
                {
                    builder.startObject();
                    {
                        builder.startObject("cancel");
                        {
                            builder.field("index", indexName);
                            builder.field("shard", shard);
                            builder.field("node", nodeName);
                            builder.field("allow_primary", true);
                        }
                        builder.endObject();
                    }
                    builder.endObject();
                }
                builder.endArray();
            }
            builder.endObject();

            Request request = new Request(HttpPost.METHOD_NAME, "/_cluster/reroute?pretty");
            request.setJsonEntity(Strings.toString(builder));
            Response response = client().performRequest(request);
            logger.info("--> Relocated primary to an older version {}", EntityUtils.toString(response.getEntity()));
            assertOK(response);
        }
    }

    private void assertMatchAllReturnsAllDocuments(String indexName, int numDocs) throws IOException {
        Map<String, Object> searchResults = search(indexName, QueryBuilders.matchAllQuery());
        assertThat(extractValue(searchResults, "hits.total.value"), equalTo(numDocs));
        List<Map<String, Object>> hits = extractValue(searchResults, "hits.hits");
        for (Map<String, Object> hit : hits) {
            String docId = extractValue(hit, "_id");
            assertThat(Integer.parseInt(docId), allOf(greaterThanOrEqualTo(0), lessThan(numDocs)));
            assertThat(extractValue(hit, "_source.field"), equalTo(Integer.parseInt(docId)));
            assertThat(extractValue(hit, "_source.text"), equalTo("Some text " + docId));
        }
    }

    private void assertMatchQueryReturnsAllDocuments(String indexName, int numDocs) throws IOException {
        Map<String, Object> searchResults = search(indexName, QueryBuilders.matchQuery("text", "some"));
        assertThat(extractValue(searchResults, "hits.total.value"), equalTo(numDocs));
    }

    private static Map<String, Object> search(String index, QueryBuilder query) throws IOException {
        final Request request = new Request(HttpPost.METHOD_NAME, '/' + index + "/_search");
        request.setJsonEntity(new SearchSourceBuilder().trackTotalHits(true).query(query).toString());

        final Response response = client().performRequest(request);
        assertOK(response);

        final Map<String, Object> responseAsMap = responseAsMap(response);
        assertThat(extractValue(responseAsMap, "_shards.failed"), equalTo(0));
        return responseAsMap;
    }

    private void indexDocs(String indexName, int numDocs) throws IOException {
        final StringBuilder bulkBody = new StringBuilder();
        for (int i = 0; i < numDocs; i++) {
            bulkBody.append("{\"index\":{\"_id\":\"").append(i).append("\"}}\n");
            bulkBody.append("{\"field\":").append(i).append(",\"text\":\"Some text ").append(i).append("\"}\n");
        }

        final Request documents = new Request(HttpPost.METHOD_NAME, '/' + indexName + "/_bulk");
        documents.addParameter("refresh", "true");
        documents.setJsonEntity(bulkBody.toString());
        assertOK(client().performRequest(documents));
    }

    @SuppressWarnings("unchecked")
    private static <T> T extractValue(Map<String, Object> map, String path) {
        return (T) XContentMapValues.extractValue(path, map);
    }
}
