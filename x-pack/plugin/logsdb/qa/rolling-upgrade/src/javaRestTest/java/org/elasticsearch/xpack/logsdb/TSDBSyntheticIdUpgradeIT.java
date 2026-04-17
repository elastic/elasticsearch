/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb;

import org.apache.http.util.EntityUtils;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexFeatures;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.test.rest.ObjectPath;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.StringJoiner;

public class TSDBSyntheticIdUpgradeIT extends AbstractLogsdbRollingUpgradeTestCase {
    private static final int DOC_COUNT = 10;

    public void testRollingUpgrade() throws IOException {
        int numNodes = getCluster().getNumNodes();
        boolean isServerless = isServerless();
        boolean oldClusterHasSyntheticId = oldClusterHasFeature(IndexFeatures.TIME_SERIES_SYNTHETIC_ID);

        // This test upgrade all nodes in the cluster one by one,
        // and create one new index in-between every upgrade,
        // before upgrades start and after upgrade has finished.
        // In every gap we write to all existing indices and test
        // that we read the expected number of documents from them.
        // The indices will use synthetic_id setting if the cluster
        // is expected to have support for it at the time of index
        // creation, otherwise it will not.
        // If old cluster has support, then cluster will have support
        // for it all through the upgrade.
        // If old cluster doesn't have support, then cluster will
        // only have support after the upgrade is finished, the
        // last iteration in clusterRollingUpgrade.

        String indexZero = indexName(0);
        assertIndexCanBeCreated(indexZero, oldClusterHasSyntheticId);
        assertCanAddDocuments(indexZero);
        assertIndexRead(indexZero, isServerless, 1, oldClusterHasSyntheticId);

        clusterRollingUpgrade(i -> {
            int nextIndexId = i + 1;
            String indexName = indexName(nextIndexId);
            boolean allNodesUpgraded = nextIndexId == numNodes;
            assertIndexCanBeCreated(indexName, oldClusterHasSyntheticId || allNodesUpgraded);

            // For all indices we have created so far
            for (int j = 0; j <= nextIndexId; j++) {
                assertCanAddDocuments(indexName(j));
                int expectedNbrOfBatchesInIndex = nextIndexId + 1 - j;
                boolean lastIndex = j == nextIndexId && allNodesUpgraded;
                assertIndexRead(indexName(j), isServerless, expectedNbrOfBatchesInIndex, oldClusterHasSyntheticId || lastIndex);
            }
        });
    }

    private static void assertIndexRead(String indexName, boolean isServerless, int nbrOfBatches, boolean expectSyntheticId)
        throws IOException {
        assertTrue("Expected index [" + indexName + "] to exist, but did not", indexExists(indexName));
        Map<String, Object> indexSettingsAsMap = getIndexSettingsAsMap(indexName);
        assertThat(
            Boolean.parseBoolean((String) indexSettingsAsMap.get(IndexSettings.SYNTHETIC_ID.getKey())),
            Matchers.equalTo(expectSyntheticId)
        );
        assertDocCount(client(), indexName, (long) nbrOfBatches * DOC_COUNT);
        if (!isServerless) {
            if (expectSyntheticId) {
                assertThat(invertedIndexSize(indexName), Matchers.equalTo(0));
            } else {
                assertThat(invertedIndexSize(indexName), Matchers.greaterThan(0));
            }
        }
    }

    private static int invertedIndexSize(String indexName) throws IOException {
        var diskUsage = new Request("POST", "/" + indexName + "/_disk_usage?run_expensive_tasks=true");
        Response response = client().performRequest(diskUsage);
        ObjectPath objectPath = ObjectPath.createFromResponse(response);
        return objectPath.evaluate(indexName + ".all_fields.inverted_index.total_in_bytes");
    }

    private void assertIndexCanBeCreated(String indexName, boolean useSyntheticId) throws IOException {
        logClusterStateBeforeCreate(indexName);
        logger.info("--> Create index {} with synthetic id {}", indexName, useSyntheticId);
        CreateIndexResponse response = null;
        try {
            response = createSyntheticIdIndex(indexName, useSyntheticId);
            logger.info(
                "Create index [{}] response: acknowledged={}, shards_acknowledged={}",
                indexName,
                response.isAcknowledged(),
                response.isShardsAcknowledged()
            );
            if (!response.isShardsAcknowledged() || !response.isAcknowledged()) {
                logAllocationDiagnostics(indexName);
            }
            assertTrue("Expected index [" + indexName + "] to be created successfully, but was not", response.isAcknowledged());
            assertTrue(
                "Expected shards of index [" + indexName + "] to be created successfully, but was not",
                response.isShardsAcknowledged()
            );
        } finally {
            if (response != null) {
                response.decRef();
            }
        }
    }

    /**
     * Log cluster nodes and health before an index create to help diagnose allocation timeouts.
     */
    private void logClusterStateBeforeCreate(String indexName) throws IOException {
        try {
            Response nodesResponse = client().performRequest(new Request("GET", "_cat/nodes?v"));
            logger.info("Creating index [{}]; nodes: {}", indexName, EntityUtils.toString(nodesResponse.getEntity()));
        } catch (Exception e) {
            logger.warn("Failed to log nodes before create for [{}]", indexName, e);
        }
        try {
            Response healthResponse = client().performRequest(new Request("GET", "_cluster/health?level=indices"));
            logger.info("Creating index [{}]; cluster health: {}", indexName, entityAsMap(healthResponse));
        } catch (Exception e) {
            logger.warn("Failed to log cluster health before create for [{}]", indexName, e);
        }
        logPendingClusterTasks(indexName, "before create");
    }

    /**
     * Log allocation-related APIs when index create returned shards_acknowledged=false.
     * Calls allocation explain for each shard that is unassigned or initializing (from _cat/shards).
     */
    private void logAllocationDiagnostics(String indexName) throws IOException {
        logPendingClusterTasks(indexName, "after create (shards not ack'd)");
        try {
            Response healthResponse = client().performRequest(new Request("GET", "_cluster/health/" + indexName + "?level=shards"));
            logger.info("Index [{}] shard-level health: {}", indexName, entityAsMap(healthResponse));
        } catch (Exception e) {
            logger.warn("Failed to log shard health for [{}]", indexName, e);
        }
        String catShardsBody = null;
        try {
            Response shardsResponse = client().performRequest(
                new Request("GET", "_cat/shards/" + indexName + "?v&h=index,shard,prirep,state,node,unassigned.reason")
            );
            catShardsBody = EntityUtils.toString(shardsResponse.getEntity());
            logger.info("Index [{}] cat shards: {}", indexName, catShardsBody);
        } catch (Exception e) {
            logger.warn("Failed to log cat shards for [{}]", indexName, e);
        }
        if (catShardsBody != null) {
            List<ShardSlot> unassigned = parseUnassignedOrInitializingShards(catShardsBody);
            for (ShardSlot slot : unassigned) {
                try {
                    Request explainRequest = new Request("POST", "_cluster/allocation/explain");
                    explainRequest.setJsonEntity(
                        String.format(
                            Locale.ROOT,
                            "{\"index\": \"%s\", \"shard\": %d, \"primary\": %s}",
                            indexName,
                            slot.shardId,
                            slot.primary
                        )
                    );
                    Response explainResponse = client().performRequest(explainRequest);
                    logger.info(
                        "Index [{}] allocation explain shard {} primary={}: {}",
                        indexName,
                        slot.shardId,
                        slot.primary,
                        entityAsMap(explainResponse)
                    );
                } catch (Exception e) {
                    logger.warn("Failed to log allocation explain for [{}] shard {} primary={}", indexName, slot.shardId, slot.primary, e);
                }
            }
            if (unassigned.isEmpty()) {
                try {
                    Request explainRequest = new Request("POST", "_cluster/allocation/explain");
                    explainRequest.setJsonEntity("{}");
                    Response explainResponse = client().performRequest(explainRequest);
                    logger.info("Index [{}] allocation explain (first unassigned in cluster): {}", indexName, entityAsMap(explainResponse));
                } catch (Exception e) {
                    logger.warn("Failed to log allocation explain (first unassigned) for [{}]", indexName, e);
                }
            }
        }
    }

    /**
     * Parse _cat/shards output (header: index, shard, prirep, state, node, unassigned.reason) and return
     * shard slots that are UNASSIGNED or INITIALIZING.
     */
    private static List<ShardSlot> parseUnassignedOrInitializingShards(String catShardsBody) {
        List<ShardSlot> result = new ArrayList<>();
        String[] lines = catShardsBody.split("\n");
        for (int i = 0; i < lines.length; i++) {
            if (i == 0) {
                continue;
            }
            String[] parts = lines[i].trim().split("\\s+");
            if (parts.length >= 4) {
                String state = parts[3];
                if ("UNASSIGNED".equals(state) || "INITIALIZING".equals(state)) {
                    try {
                        int shardId = Integer.parseInt(parts[1]);
                        boolean primary = "p".equalsIgnoreCase(parts[2]);
                        result.add(new ShardSlot(shardId, primary));
                    } catch (NumberFormatException e) {
                        // skip malformed line
                    }
                }
            }
        }
        return result;
    }

    private record ShardSlot(int shardId, boolean primary) {}

    /**
     * Log master's pending cluster tasks (e.g. cluster state updates). A backlog can delay shard allocation.
     */
    private void logPendingClusterTasks(String indexName, String when) {
        try {
            Response response = client().performRequest(new Request("GET", "_cluster/pending_tasks"));
            logger.info("Index [{}] pending cluster tasks {}: {}", indexName, when, entityAsMap(response));
        } catch (Exception e) {
            logger.warn("Failed to log pending cluster tasks for [{}] ({})", indexName, when, e);
        }
    }

    private static void assertCanAddDocuments(String indexName) throws IOException {
        StringJoiner joiner = new StringJoiner("\n", "", "\n");
        Instant now = Instant.now();
        for (int i = 0; i < DOC_COUNT; i++) {
            addDocument(joiner, now.plus(i, ChronoUnit.SECONDS));
        }
        var request = new Request("PUT", "/" + indexName + "/_bulk");
        request.setJsonEntity(joiner.toString());
        request.addParameter("refresh", "true");
        Response response = client().performRequest(request);
        assertOK(response);
    }

    private static void addDocument(StringJoiner joiner, Instant timestamp) {
        joiner.add("{\"create\": {}}");
        joiner.add(String.format(Locale.ROOT, """
            {"@timestamp": "%s", "hostname": "host", "metric": {"field": "cpu-load", "value": %d}}
            """, timestamp, randomByte()));
    }

    private static CreateIndexResponse createSyntheticIdIndex(String indexName, boolean useSyntheticId) throws IOException {
        Settings.Builder settingsBuilder = Settings.builder()
            .put(IndexSettings.MODE.getKey(), IndexMode.TIME_SERIES)
            .put(IndexMetadata.INDEX_ROUTING_PATH.getKey(), "hostname");
        if (useSyntheticId) {
            settingsBuilder.put(IndexSettings.SYNTHETIC_ID.getKey(), true);
        }
        Settings settings = settingsBuilder.build();
        final var mapping = """
            {
                "properties": {
                    "@timestamp": {
                        "type": "date"
                    },
                    "hostname": {
                        "type": "keyword",
                        "time_series_dimension": true
                    },
                    "metric": {
                        "properties": {
                            "field": {
                                "type": "keyword",
                                "time_series_dimension": true
                            },
                            "value": {
                                "type": "integer",
                                "time_series_metric": "counter"
                            }
                        }
                    }
                }
            }
            """;
        return createIndex(indexName, settings, mapping);
    }

    private static String indexName(int i) {
        return "index_" + i;
    }

    private static boolean isServerless() throws IOException {
        Map<String, Map<?, ?>> nodesInfo = getNodesInfo(adminClient());
        List<?> buildFlavors = nodesInfo.values().stream().map(nodeInfoMap -> nodeInfoMap.get("build_flavor")).distinct().toList();
        assertThat(buildFlavors.size(), Matchers.equalTo(1));
        String buildFlavor = ESRestTestCase.asInstanceOf(String.class, buildFlavors.getFirst());
        return "serverless".equals(buildFlavor);
    }
}
