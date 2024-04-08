/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.backwards;

import org.apache.http.HttpHost;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.core.RestApiVersion;
import org.elasticsearch.index.seqno.SeqNoStats;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.test.rest.ObjectPath;
import org.elasticsearch.xcontent.MediaType;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.oneOf;

public class IndexingIT extends ESRestTestCase {
    private static final String BWC_NODES_VERSION = System.getProperty("tests.bwc_nodes_version");

    private int indexDocs(String index, final int idStart, final int numDocs) throws IOException {
        for (int i = 0; i < numDocs; i++) {
            final int id = idStart + i;
            Request request = new Request("PUT", index + "/_doc/" + id);
            request.setJsonEntity("{\"test\": \"test_" + randomAlphaOfLength(2) + "\"}");
            assertOK(client().performRequest(request));
        }
        return numDocs;
    }

    /**
     * Indexes a document in <code>index</code> with <code>docId</code> then concurrently updates the same document
     * <code>nUpdates</code> times
     *
     * @return the document version after updates
     */
    private int indexDocWithConcurrentUpdates(String index, final int docId, int nUpdates) throws IOException, InterruptedException {
        indexDocs(index, docId, 1);
        Thread[] indexThreads = new Thread[nUpdates];
        for (int i = 0; i < nUpdates; i++) {
            indexThreads[i] = new Thread(() -> {
                try {
                    indexDocs(index, docId, 1);
                } catch (IOException e) {
                    throw new AssertionError("failed while indexing [" + e.getMessage() + "]");
                }
            });
            indexThreads[i].start();
        }
        for (Thread indexThread : indexThreads) {
            indexThread.join();
        }
        return nUpdates + 1;
    }

    public void testIndexVersionPropagation() throws Exception {
        MixedClusterTestNodes nodes = buildNodeAndVersions();
        assumeFalse("new nodes is empty", nodes.getNewNodes().isEmpty());
        logger.info("cluster discovered: {}", nodes.toString());
        final List<String> bwcNamesList = nodes.getBWCNodes().stream().map(MixedClusterTestNode::nodeName).collect(Collectors.toList());
        final String bwcNames = bwcNamesList.stream().collect(Collectors.joining(","));
        Settings.Builder settings = Settings.builder()
            .put(IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), 1)
            .put(IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 2)
            .put("index.routing.allocation.include._name", bwcNames);
        final String index = "indexversionprop";
        final int minUpdates = 5;
        final int maxUpdates = 10;
        createIndex(index, settings.build());
        try (
            RestClient newNodeClient = buildClient(
                restClientSettings(),
                nodes.getNewNodes().stream().map(MixedClusterTestNode::publishAddress).toArray(HttpHost[]::new)
            )
        ) {

            int nUpdates = randomIntBetween(minUpdates, maxUpdates);
            logger.info("indexing docs with [{}] concurrent updates initially", nUpdates);
            final int finalVersionForDoc1 = indexDocWithConcurrentUpdates(index, 1, nUpdates);
            logger.info("allowing shards on all nodes");
            updateIndexSettings(index, Settings.builder().putNull("index.routing.allocation.include._name"));
            ensureGreen(index);
            assertOK(client().performRequest(new Request("POST", index + "/_refresh")));
            List<Shard> shards = buildShards(index, nodes, newNodeClient);
            Shard primary = buildShards(index, nodes, newNodeClient).stream().filter(Shard::primary).findFirst().get();
            logger.info("primary resolved to: " + primary.node().nodeName());
            for (Shard shard : shards) {
                assertVersion(index, 1, "_only_nodes:" + shard.node().nodeName(), finalVersionForDoc1);
                assertCount(index, "_only_nodes:" + shard.node().nodeName(), 1);
            }

            nUpdates = randomIntBetween(minUpdates, maxUpdates);
            logger.info("indexing docs with [{}] concurrent updates after allowing shards on all nodes", nUpdates);
            final int finalVersionForDoc2 = indexDocWithConcurrentUpdates(index, 2, nUpdates);
            assertOK(client().performRequest(new Request("POST", index + "/_refresh")));
            shards = buildShards(index, nodes, newNodeClient);
            primary = shards.stream().filter(Shard::primary).findFirst().get();
            logger.info("primary resolved to: " + primary.node().nodeName());
            for (Shard shard : shards) {
                assertVersion(index, 2, "_only_nodes:" + shard.node().nodeName(), finalVersionForDoc2);
                assertCount(index, "_only_nodes:" + shard.node().nodeName(), 2);
            }

            primary = shards.stream().filter(Shard::primary).findFirst().get();
            logger.info("moving primary to new node by excluding {}", primary.node().nodeName());
            updateIndexSettings(index, Settings.builder().put("index.routing.allocation.exclude._name", primary.node().nodeName()));
            ensureGreen(index);
            nUpdates = randomIntBetween(minUpdates, maxUpdates);
            logger.info("indexing docs with [{}] concurrent updates after moving primary", nUpdates);
            final int finalVersionForDoc3 = indexDocWithConcurrentUpdates(index, 3, nUpdates);
            assertOK(client().performRequest(new Request("POST", index + "/_refresh")));
            shards = buildShards(index, nodes, newNodeClient);
            for (Shard shard : shards) {
                assertVersion(index, 3, "_only_nodes:" + shard.node().nodeName(), finalVersionForDoc3);
                assertCount(index, "_only_nodes:" + shard.node().nodeName(), 3);
            }

            logger.info("setting number of replicas to 0");
            updateIndexSettings(index, Settings.builder().put("index.number_of_replicas", 0));
            ensureGreen(index);
            nUpdates = randomIntBetween(minUpdates, maxUpdates);
            logger.info("indexing doc with [{}] concurrent updates after setting number of replicas to 0", nUpdates);
            final int finalVersionForDoc4 = indexDocWithConcurrentUpdates(index, 4, nUpdates);
            assertOK(client().performRequest(new Request("POST", index + "/_refresh")));
            shards = buildShards(index, nodes, newNodeClient);
            for (Shard shard : shards) {
                assertVersion(index, 4, "_only_nodes:" + shard.node().nodeName(), finalVersionForDoc4);
                assertCount(index, "_only_nodes:" + shard.node().nodeName(), 4);
            }

            logger.info("setting number of replicas to 1");
            updateIndexSettings(index, Settings.builder().put("index.number_of_replicas", 1));
            ensureGreen(index);
            nUpdates = randomIntBetween(minUpdates, maxUpdates);
            logger.info("indexing doc with [{}] concurrent updates after setting number of replicas to 1", nUpdates);
            final int finalVersionForDoc5 = indexDocWithConcurrentUpdates(index, 5, nUpdates);
            assertOK(client().performRequest(new Request("POST", index + "/_refresh")));
            shards = buildShards(index, nodes, newNodeClient);
            for (Shard shard : shards) {
                assertVersion(index, 5, "_only_nodes:" + shard.node().nodeName(), finalVersionForDoc5);
                assertCount(index, "_only_nodes:" + shard.node().nodeName(), 5);
            }
        }
    }

    public void testSeqNoCheckpoints() throws Exception {
        MixedClusterTestNodes nodes = buildNodeAndVersions();
        assumeFalse("new nodes is empty", nodes.getNewNodes().isEmpty());
        logger.info("cluster discovered: {}", nodes.toString());
        final List<String> bwcNamesList = nodes.getBWCNodes().stream().map(MixedClusterTestNode::nodeName).collect(Collectors.toList());
        final String bwcNames = bwcNamesList.stream().collect(Collectors.joining(","));
        Settings.Builder settings = Settings.builder()
            .put(IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), 1)
            .put(IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 2)
            .put("index.routing.allocation.include._name", bwcNames);

        final String index = "test";
        createIndex(index, settings.build());
        try (
            RestClient newNodeClient = buildClient(
                restClientSettings(),
                nodes.getNewNodes().stream().map(MixedClusterTestNode::publishAddress).toArray(HttpHost[]::new)
            )
        ) {
            int numDocs = 0;
            final int numberOfInitialDocs = 1 + randomInt(5);
            logger.info("indexing [{}] docs initially", numberOfInitialDocs);
            numDocs += indexDocs(index, 0, numberOfInitialDocs);
            assertSeqNoOnShards(index, nodes, numDocs, newNodeClient);
            logger.info("allowing shards on all nodes");
            updateIndexSettings(index, Settings.builder().putNull("index.routing.allocation.include._name"));
            ensureGreen(index);
            assertOK(client().performRequest(new Request("POST", index + "/_refresh")));
            for (final String bwcName : bwcNamesList) {
                assertCount(index, "_only_nodes:" + bwcName, numDocs);
            }
            final int numberOfDocsAfterAllowingShardsOnAllNodes = 1 + randomInt(5);
            logger.info("indexing [{}] docs after allowing shards on all nodes", numberOfDocsAfterAllowingShardsOnAllNodes);
            numDocs += indexDocs(index, numDocs, numberOfDocsAfterAllowingShardsOnAllNodes);
            assertSeqNoOnShards(index, nodes, numDocs, newNodeClient);
            Shard primary = buildShards(index, nodes, newNodeClient).stream().filter(Shard::primary).findFirst().get();
            logger.info("moving primary to new node by excluding {}", primary.node().nodeName());
            updateIndexSettings(index, Settings.builder().put("index.routing.allocation.exclude._name", primary.node().nodeName()));
            ensureGreen(index);
            int numDocsOnNewPrimary = 0;
            final int numberOfDocsAfterMovingPrimary = 1 + randomInt(5);
            logger.info("indexing [{}] docs after moving primary", numberOfDocsAfterMovingPrimary);
            numDocsOnNewPrimary += indexDocs(index, numDocs, numberOfDocsAfterMovingPrimary);
            numDocs += numberOfDocsAfterMovingPrimary;
            assertSeqNoOnShards(index, nodes, numDocs, newNodeClient);
            /*
             * Dropping the number of replicas to zero, and then increasing it to one triggers a recovery thus exercising any BWC-logic in
             * the recovery code.
             */
            logger.info("setting number of replicas to 0");
            updateIndexSettings(index, Settings.builder().put("index.number_of_replicas", 0));
            final int numberOfDocsAfterDroppingReplicas = 1 + randomInt(5);
            logger.info("indexing [{}] docs after setting number of replicas to 0", numberOfDocsAfterDroppingReplicas);
            numDocsOnNewPrimary += indexDocs(index, numDocs, numberOfDocsAfterDroppingReplicas);
            numDocs += numberOfDocsAfterDroppingReplicas;
            logger.info("setting number of replicas to 1");
            updateIndexSettings(index, Settings.builder().put("index.number_of_replicas", 1));
            ensureGreen(index);
            assertOK(client().performRequest(new Request("POST", index + "/_refresh")));

            for (Shard shard : buildShards(index, nodes, newNodeClient)) {
                assertCount(index, "_only_nodes:" + shard.node.nodeName(), numDocs);
            }
            assertSeqNoOnShards(index, nodes, numDocs, newNodeClient);
        }
    }

    public void testUpdateSnapshotStatus() throws Exception {
        MixedClusterTestNodes nodes = buildNodeAndVersions();
        assumeFalse("new nodes is empty", nodes.getNewNodes().isEmpty());
        logger.info("cluster discovered: {}", nodes.toString());

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
                    .endObject()
                    .endObject()
            )
        );

        assertOK(client().performRequest(request));

        String bwcNames = nodes.getBWCNodes().stream().map(MixedClusterTestNode::nodeName).collect(Collectors.joining(","));

        // Allocating shards on the BWC nodes to makes sure that taking snapshot happens on those nodes.
        Settings.Builder settings = Settings.builder()
            .put(IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), between(5, 10))
            .put(IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 1)
            .put("index.routing.allocation.include._name", bwcNames);

        final String index = "test-snapshot-index";
        createIndex(index, settings.build());
        indexDocs(index, 0, between(50, 100));
        ensureGreen(index);
        assertOK(client().performRequest(new Request("POST", index + "/_refresh")));

        request = new Request("PUT", "/_snapshot/repo/bwc-snapshot");
        request.addParameter("wait_for_completion", "true");
        request.setJsonEntity("{\"indices\": \"" + index + "\"}");
        assertOK(client().performRequest(request));

        // Allocating shards on all nodes, taking snapshots should happen on all nodes.
        updateIndexSettings(index, Settings.builder().putNull("index.routing.allocation.include._name"));
        ensureGreen(index);
        assertOK(client().performRequest(new Request("POST", index + "/_refresh")));

        request = new Request("PUT", "/_snapshot/repo/mixed-snapshot");
        request.addParameter("wait_for_completion", "true");
        request.setJsonEntity("{\"indices\": \"" + index + "\"}");
    }

    /**
     * Tries to extract a major version from a version string, if this is in the major.minor.revision format
     * @param version a string representing a version. Can be opaque or semantic
     * @return Optional.empty() if the format is not recognized, or an Optional containing the major Integer otherwise
     */
    private static Optional<Integer> extractLegacyMajorVersion(String version) {
        var semanticVersionMatcher = Pattern.compile("^(\\d+)\\.\\d+\\.\\d+\\D?.*").matcher(version);
        if (semanticVersionMatcher.matches() == false) {
            return Optional.empty();
        }
        var major = Integer.parseInt(semanticVersionMatcher.group(1));
        return Optional.of(major);
    }

    private static boolean syncedFlushDeprecated() {
        // Only versions past 8.10 can be non-semantic, so we can safely assume that non-semantic versions have this "feature"
        return extractLegacyMajorVersion(BWC_NODES_VERSION).map(m -> m >= 7).orElse(true);
    }

    private static boolean syncedFlushRemoved() {
        // Only versions past 8.10 can be non-semantic, so we can safely assume that non-semantic versions have this "feature"
        return extractLegacyMajorVersion(BWC_NODES_VERSION).map(m -> m >= 8).orElse(true);
    }

    public void testSyncedFlushTransition() throws Exception {
        MixedClusterTestNodes nodes = buildNodeAndVersions();
        assumeTrue(
            "bwc version is on 7.x (synced flush deprecated but not removed yet)",
            syncedFlushDeprecated() && syncedFlushRemoved() == false
        );
        assumeFalse("no new node found", nodes.getNewNodes().isEmpty());
        assumeFalse("no bwc node found", nodes.getBWCNodes().isEmpty());
        // Allocate shards to new nodes then verify synced flush requests processed by old nodes/new nodes
        String newNodes = nodes.getNewNodes().stream().map(MixedClusterTestNode::nodeName).collect(Collectors.joining(","));
        int numShards = randomIntBetween(1, 10);
        int numOfReplicas = randomIntBetween(0, nodes.getNewNodes().size() - 1);
        int totalShards = numShards * (numOfReplicas + 1);
        final String index = "test_synced_flush";
        createIndex(
            index,
            Settings.builder()
                .put(IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), numShards)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, numOfReplicas)
                .put("index.routing.allocation.include._name", newNodes)
                .build()
        );
        ensureGreen(index);
        indexDocs(index, randomIntBetween(0, 100), between(1, 100));
        try (
            RestClient oldNodeClient = buildClient(
                restClientSettings(),
                nodes.getBWCNodes().stream().map(MixedClusterTestNode::publishAddress).toArray(HttpHost[]::new)
            )
        ) {
            Request request = new Request("POST", index + "/_flush/synced");
            assertBusy(() -> {
                ResponseException responseException = expectThrows(ResponseException.class, () -> oldNodeClient.performRequest(request));
                assertThat(responseException.getResponse().getStatusLine().getStatusCode(), equalTo(RestStatus.CONFLICT.getStatus()));
                assertThat(
                    responseException.getResponse().getWarnings(),
                    contains(
                        oneOf(
                            "Synced flush is deprecated and will be removed in 8.0. Use flush at _/flush or /{index}/_flush instead.",
                            "Synced flush is deprecated and will be removed in 8.0. Use flush at /_flush or /{index}/_flush instead."
                        )
                    )
                );
                Map<String, Object> result = ObjectPath.createFromResponse(responseException.getResponse()).evaluate("_shards");
                assertThat(result.get("total"), equalTo(totalShards));
                assertThat(result.get("successful"), equalTo(0));
                assertThat(result.get("failed"), equalTo(totalShards));
            });
            Map<String, Object> stats = entityAsMap(client().performRequest(new Request("GET", index + "/_stats?level=shards")));
            assertThat(XContentMapValues.extractValue("indices." + index + ".total.translog.uncommitted_operations", stats), equalTo(0));
        }
        indexDocs(index, randomIntBetween(0, 100), between(1, 100));
        try (
            RestClient newNodeClient = buildClient(
                restClientSettings(),
                nodes.getNewNodes().stream().map(MixedClusterTestNode::publishAddress).toArray(HttpHost[]::new)
            )
        ) {
            Request request = new Request("POST", index + "/_flush/synced");
            final String v7MediaType = XContentType.VND_JSON.toParsedMediaType()
                .responseContentTypeHeader(
                    Map.of(MediaType.COMPATIBLE_WITH_PARAMETER_NAME, String.valueOf(RestApiVersion.minimumSupported().major))
                );
            List<String> warningMsg = List.of(
                "Synced flush is deprecated and will be removed in 8.0." + " Use flush at /_flush or /{index}/_flush instead."
            );
            request.setOptions(
                RequestOptions.DEFAULT.toBuilder()
                    .setWarningsHandler(warnings -> warnings.equals(warningMsg) == false)
                    .addHeader("Accept", v7MediaType)
            );

            assertBusy(() -> {
                Map<String, Object> result = ObjectPath.createFromResponse(newNodeClient.performRequest(request)).evaluate("_shards");
                assertThat(result.get("total"), equalTo(totalShards));
                assertThat(result.get("successful"), equalTo(totalShards));
                assertThat(result.get("failed"), equalTo(0));
            });
            Map<String, Object> stats = entityAsMap(client().performRequest(new Request("GET", index + "/_stats?level=shards")));
            assertThat(XContentMapValues.extractValue("indices." + index + ".total.translog.uncommitted_operations", stats), equalTo(0));
        }
    }

    public void testFlushTransition() throws Exception {
        MixedClusterTestNodes nodes = buildNodeAndVersions();
        assumeFalse("no new node found", nodes.getNewNodes().isEmpty());
        assumeFalse("no bwc node found", nodes.getBWCNodes().isEmpty());
        // Allocate shards to new nodes then verify flush requests processed by old nodes/new nodes
        String newNodes = nodes.getNewNodes().stream().map(MixedClusterTestNode::nodeName).collect(Collectors.joining(","));
        int numShards = randomIntBetween(1, 10);
        int numOfReplicas = randomIntBetween(0, nodes.getNewNodes().size() - 1);
        int totalShards = numShards * (numOfReplicas + 1);
        final String index = "test_flush";
        createIndex(
            index,
            Settings.builder()
                .put(IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), numShards)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, numOfReplicas)
                .put("index.routing.allocation.include._name", newNodes)
                .build()
        );
        ensureGreen(index);
        indexDocs(index, randomIntBetween(0, 100), between(1, 100));
        try (
            RestClient oldNodeClient = buildClient(
                restClientSettings(),
                nodes.getBWCNodes().stream().map(MixedClusterTestNode::publishAddress).toArray(HttpHost[]::new)
            )
        ) {
            Request request = new Request("POST", index + "/_flush");
            assertBusy(() -> {
                Map<String, Object> result = ObjectPath.createFromResponse(oldNodeClient.performRequest(request)).evaluate("_shards");
                assertThat(result.get("total"), equalTo(totalShards));
                assertThat(result.get("successful"), equalTo(totalShards));
                assertThat(result.get("failed"), equalTo(0));
            });
            Map<String, Object> stats = entityAsMap(client().performRequest(new Request("GET", index + "/_stats?level=shards")));
            assertThat(XContentMapValues.extractValue("indices." + index + ".total.translog.uncommitted_operations", stats), equalTo(0));
        }
        indexDocs(index, randomIntBetween(0, 100), between(1, 100));
        try (
            RestClient newNodeClient = buildClient(
                restClientSettings(),
                nodes.getNewNodes().stream().map(MixedClusterTestNode::publishAddress).toArray(HttpHost[]::new)
            )
        ) {
            Request request = new Request("POST", index + "/_flush");
            assertBusy(() -> {
                Map<String, Object> result = ObjectPath.createFromResponse(newNodeClient.performRequest(request)).evaluate("_shards");
                assertThat(result.get("total"), equalTo(totalShards));
                assertThat(result.get("successful"), equalTo(totalShards));
                assertThat(result.get("failed"), equalTo(0));
            });
            Map<String, Object> stats = entityAsMap(client().performRequest(new Request("GET", index + "/_stats?level=shards")));
            assertThat(XContentMapValues.extractValue("indices." + index + ".total.translog.uncommitted_operations", stats), equalTo(0));
        }
    }

    private void assertCount(final String index, final String preference, final int expectedCount) throws IOException {
        Request request = new Request("GET", index + "/_count");
        request.addParameter("preference", preference);
        final Response response = client().performRequest(request);
        assertOK(response);
        final int actualCount = Integer.parseInt(ObjectPath.createFromResponse(response).evaluate("count").toString());
        assertThat(actualCount, equalTo(expectedCount));
    }

    private void assertVersion(final String index, final int docId, final String preference, final int expectedVersion) throws IOException {
        Request request = new Request("GET", index + "/_doc/" + docId);
        request.addParameter("preference", preference);

        final Response response = client().performRequest(request);
        assertOK(response);
        final int actualVersion = Integer.parseInt(ObjectPath.createFromResponse(response).evaluate("_version").toString());
        assertThat("version mismatch for doc [" + docId + "] preference [" + preference + "]", actualVersion, equalTo(expectedVersion));
    }

    private void assertSeqNoOnShards(String index, MixedClusterTestNodes nodes, int numDocs, RestClient client) throws Exception {
        assertBusy(() -> {
            try {
                List<Shard> shards = buildShards(index, nodes, client);
                Shard primaryShard = shards.stream().filter(Shard::primary).findFirst().get();
                assertNotNull("failed to find primary shard", primaryShard);
                final long expectedGlobalCkp = numDocs - 1;
                final long expectMaxSeqNo = numDocs - 1;
                logger.info("primary resolved to node {}", primaryShard.node());
                for (Shard shard : shards) {
                    final SeqNoStats seqNoStats = shard.seqNoStats();
                    logger.info("stats for {}, primary [{}]: [{}]", shard.node(), shard.primary(), seqNoStats);
                    assertThat("max_seq no on " + shard.node() + " is wrong", seqNoStats.getMaxSeqNo(), equalTo(expectMaxSeqNo));
                    assertThat(
                        "localCheckpoint no on " + shard.node() + " is wrong",
                        seqNoStats.getLocalCheckpoint(),
                        equalTo(expectMaxSeqNo)
                    );
                    assertThat(
                        "globalCheckpoint no on " + shard.node() + " is wrong",
                        seqNoStats.getGlobalCheckpoint(),
                        equalTo(expectedGlobalCkp)
                    );
                }
            } catch (IOException e) {
                throw new AssertionError("unexpected io exception", e);
            }
        });
    }

    private List<Shard> buildShards(String index, MixedClusterTestNodes nodes, RestClient client) throws IOException {
        Request request = new Request("GET", index + "/_stats");
        request.addParameter("level", "shards");
        Response response = client.performRequest(request);
        List<Object> shardStats = ObjectPath.createFromResponse(response).evaluate("indices." + index + ".shards.0");
        ArrayList<Shard> shards = new ArrayList<>();
        for (Object shard : shardStats) {
            final String nodeId = ObjectPath.evaluate(shard, "routing.node");
            final Boolean primary = ObjectPath.evaluate(shard, "routing.primary");
            final MixedClusterTestNode node = nodes.getSafe(nodeId);
            final SeqNoStats seqNoStats;
            Integer maxSeqNo = ObjectPath.evaluate(shard, "seq_no.max_seq_no");
            Integer localCheckpoint = ObjectPath.evaluate(shard, "seq_no.local_checkpoint");
            Integer globalCheckpoint = ObjectPath.evaluate(shard, "seq_no.global_checkpoint");
            seqNoStats = new SeqNoStats(maxSeqNo, localCheckpoint, globalCheckpoint);
            shards.add(new Shard(node, primary, seqNoStats));
        }
        logger.info("shards {}", shards);
        return shards;
    }

    private MixedClusterTestNodes buildNodeAndVersions() throws IOException {
        return MixedClusterTestNodes.buildNodes(client(), BWC_NODES_VERSION);
    }

    private record Shard(MixedClusterTestNode node, boolean primary, SeqNoStats seqNoStats) {}
}
