/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.indices.state;

import org.elasticsearch.exception.ExceptionsHelper;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.indices.close.CloseIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.close.CloseIndexResponse;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.MetadataIndexStateService;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.indices.IndexClosedException;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.test.BackgroundIndexer;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalTestCluster;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.util.Collections.emptySet;
import static java.util.stream.Collectors.toList;
import static org.elasticsearch.action.support.IndicesOptions.lenientExpandOpen;
import static org.elasticsearch.search.internal.SearchContext.TRACK_TOTAL_HITS_ACCURATE;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class CloseIndexIT extends ESIntegTestCase {

    private static final int MAX_DOCS = 25_000;

    @Override
    public Settings indexSettings() {
        return Settings.builder()
            .put(super.indexSettings())
            .put(
                IndexSettings.INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE_SETTING.getKey(),
                ByteSizeValue.of(randomIntBetween(1, 4096), ByteSizeUnit.KB)
            )
            .build();
    }

    public void testCloseMissingIndex() {
        IndexNotFoundException e = expectThrows(IndexNotFoundException.class, indicesAdmin().prepareClose("test"));
        assertThat(e.getMessage(), is("no such index [test]"));
    }

    public void testCloseOneMissingIndex() {
        createIndex("test1");
        final IndexNotFoundException e = expectThrows(IndexNotFoundException.class, indicesAdmin().prepareClose("test1", "test2"));
        assertThat(e.getMessage(), is("no such index [test2]"));
    }

    public void testCloseOneMissingIndexIgnoreMissing() throws Exception {
        createIndex("test1");
        assertBusy(() -> assertAcked(indicesAdmin().prepareClose("test1", "test2").setIndicesOptions(lenientExpandOpen())));
        assertIndexIsClosed("test1");
    }

    public void testCloseNoIndex() {
        final ActionRequestValidationException e = expectThrows(ActionRequestValidationException.class, indicesAdmin().prepareClose());
        assertThat(e.getMessage(), containsString("index is missing"));
    }

    public void testCloseNullIndex() {
        final ActionRequestValidationException e = expectThrows(
            ActionRequestValidationException.class,
            indicesAdmin().prepareClose((String[]) null)
        );
        assertThat(e.getMessage(), containsString("index is missing"));
    }

    public void testCloseIndex() throws Exception {
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(indexName);

        final int nbDocs = randomIntBetween(0, 50);
        indexRandom(
            randomBoolean(),
            false,
            randomBoolean(),
            IntStream.range(0, nbDocs).mapToObj(i -> prepareIndex(indexName).setId(String.valueOf(i)).setSource("num", i)).collect(toList())
        );

        assertBusy(() -> closeIndices(indexName));
        assertIndexIsClosed(indexName);

        assertAcked(indicesAdmin().prepareOpen(indexName));
        assertHitCount(prepareSearch(indexName).setSize(0), nbDocs);
    }

    public void testCloseAlreadyClosedIndex() throws Exception {
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(indexName);

        if (randomBoolean()) {
            indexRandom(
                randomBoolean(),
                false,
                randomBoolean(),
                IntStream.range(0, randomIntBetween(1, 10))
                    .mapToObj(i -> prepareIndex(indexName).setId(String.valueOf(i)).setSource("num", i))
                    .collect(toList())
            );
        }
        // First close should be fully acked
        assertBusy(() -> closeIndices(indexName));
        assertIndexIsClosed(indexName);

        // Second close should be acked too
        final ActiveShardCount activeShardCount = randomFrom(ActiveShardCount.NONE, ActiveShardCount.DEFAULT, ActiveShardCount.ALL);
        assertBusy(() -> {
            CloseIndexResponse response = indicesAdmin().prepareClose(indexName).setWaitForActiveShards(activeShardCount).get();
            assertAcked(response);
            assertTrue(response.getIndices().isEmpty());
        });
        assertIndexIsClosed(indexName);
    }

    public void testCloseUnassignedIndex() throws Exception {
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        assertAcked(
            prepareCreate(indexName).setWaitForActiveShards(ActiveShardCount.NONE)
                .setSettings(Settings.builder().put("index.routing.allocation.include._name", "nothing").build())
        );

        final ClusterState clusterState = clusterAdmin().prepareState(TEST_REQUEST_TIMEOUT).get().getState();
        assertThat(clusterState.metadata().getProject().indices().get(indexName).getState(), is(IndexMetadata.State.OPEN));
        assertThat(clusterState.routingTable().allShards().allMatch(ShardRouting::unassigned), is(true));

        assertBusy(() -> closeIndices(indicesAdmin().prepareClose(indexName).setWaitForActiveShards(ActiveShardCount.NONE)));
        assertIndexIsClosed(indexName);
    }

    public void testConcurrentClose() throws InterruptedException, ExecutionException {
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(indexName);

        final int nbDocs = randomIntBetween(10, 50);
        indexRandom(
            randomBoolean(),
            false,
            randomBoolean(),
            IntStream.range(0, nbDocs).mapToObj(i -> prepareIndex(indexName).setId(String.valueOf(i)).setSource("num", i)).collect(toList())
        );

        ClusterHealthResponse healthResponse = clusterAdmin().prepareHealth(TEST_REQUEST_TIMEOUT, indexName)
            .setWaitForYellowStatus()
            .setWaitForEvents(Priority.LANGUID)
            .setWaitForNoRelocatingShards(true)
            .setWaitForNoInitializingShards(true)
            .setWaitForNodes(Integer.toString(cluster().size()))
            .setTimeout(TimeValue.timeValueSeconds(60L))
            .get();
        if (healthResponse.isTimedOut()) {
            logClusterState();
        }
        assertThat(healthResponse.isTimedOut(), equalTo(false));
        assertThat(healthResponse.getIndices().get(indexName).getStatus().value(), lessThanOrEqualTo(ClusterHealthStatus.YELLOW.value()));

        final int tasks = randomIntBetween(2, 5);
        startInParallel(tasks, i -> {
            try {
                indicesAdmin().prepareClose(indexName).get();
            } catch (final Exception e) {
                assertException(e, indexName);
            }
        });
        assertIndexIsClosed(indexName);
    }

    public void testCloseWhileIndexingDocuments() throws Exception {
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(indexName);

        long nbDocs = 0;
        try (BackgroundIndexer indexer = new BackgroundIndexer(indexName, client(), MAX_DOCS)) {
            indexer.setFailureAssertion(t -> assertException(t, indexName));

            waitForDocs(randomIntBetween(10, 50), indexer);
            assertBusy(() -> closeIndices(indexName));
            indexer.stopAndAwaitStopped();
            nbDocs += indexer.totalIndexedDocs();
        }

        assertIndexIsClosed(indexName);
        assertAcked(indicesAdmin().prepareOpen(indexName));
        assertHitCount(prepareSearch(indexName).setSize(0).setTrackTotalHitsUpTo(TRACK_TOTAL_HITS_ACCURATE), nbDocs);
    }

    public void testCloseWhileDeletingIndices() throws Exception {
        final String[] indices = new String[randomIntBetween(3, 10)];
        for (int i = 0; i < indices.length; i++) {
            final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
            createIndex(indexName);
            if (randomBoolean()) {
                indexRandom(
                    randomBoolean(),
                    false,
                    randomBoolean(),
                    IntStream.range(0, 10)
                        .mapToObj(n -> prepareIndex(indexName).setId(String.valueOf(n)).setSource("num", n))
                        .collect(toList())
                );
            }
            indices[i] = indexName;
        }
        assertThat(
            clusterAdmin().prepareState(TEST_REQUEST_TIMEOUT).get().getState().metadata().getProject().indices().size(),
            equalTo(indices.length)
        );

        startInParallel(indices.length * 2, i -> {
            final String index = indices[i % indices.length];
            try {
                if (i < indices.length) {
                    assertAcked(indicesAdmin().prepareDelete(index));
                } else {
                    indicesAdmin().prepareClose(index).get();
                }
            } catch (final Exception e) {
                assertException(e, index);
            }
        });
    }

    public void testConcurrentClosesAndOpens() throws Exception {
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(indexName);

        final BackgroundIndexer indexer = new BackgroundIndexer(indexName, client(), MAX_DOCS);
        indexer.setFailureAssertion(e -> {});
        waitForDocs(1, indexer);

        final int closes = randomIntBetween(1, 3);
        final int opens = randomIntBetween(1, 3);
        final CyclicBarrier barrier = new CyclicBarrier(opens + closes);

        startInParallel(opens + closes, i -> {
            try {
                if (i < closes) {
                    indicesAdmin().prepareClose(indexName).get();
                } else {
                    assertAcked(indicesAdmin().prepareOpen(indexName).get());
                }
            } catch (final Exception e) {
                throw new AssertionError(e);
            }
        });

        indexer.stopAndAwaitStopped();

        final ClusterState clusterState = clusterAdmin().prepareState(TEST_REQUEST_TIMEOUT).get().getState();
        if (clusterState.metadata().getProject().indices().get(indexName).getState() == IndexMetadata.State.CLOSE) {
            assertIndexIsClosed(indexName);
            assertAcked(indicesAdmin().prepareOpen(indexName));
        }
        refresh(indexName);
        assertIndexIsOpened(indexName);
        assertHitCount(prepareSearch(indexName).setSize(0).setTrackTotalHitsUpTo(TRACK_TOTAL_HITS_ACCURATE), indexer.totalIndexedDocs());
    }

    public void testCloseIndexWaitForActiveShards() throws Exception {
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        // no replicas to avoid recoveries that could fail the index closing
        createIndex(indexName, 2, 0);

        final int nbDocs = randomIntBetween(0, 50);
        indexRandom(
            randomBoolean(),
            false,
            randomBoolean(),
            IntStream.range(0, nbDocs).mapToObj(i -> prepareIndex(indexName).setId(String.valueOf(i)).setSource("num", i)).collect(toList())
        );
        ensureGreen(indexName);

        final CloseIndexResponse closeIndexResponse = indicesAdmin().prepareClose(indexName).get();
        assertThat(clusterAdmin().prepareHealth(TEST_REQUEST_TIMEOUT, indexName).get().getStatus(), is(ClusterHealthStatus.GREEN));
        assertTrue(closeIndexResponse.isAcknowledged());
        assertTrue(closeIndexResponse.isShardsAcknowledged());
        assertThat(closeIndexResponse.getIndices().get(0), notNullValue());
        assertThat(closeIndexResponse.getIndices().get(0).hasFailures(), is(false));
        assertThat(closeIndexResponse.getIndices().get(0).getIndex().getName(), equalTo(indexName));
        assertIndexIsClosed(indexName);
    }

    public void testNoopPeerRecoveriesWhenIndexClosed() throws Exception {
        final String indexName = "noop-peer-recovery-test";
        int numberOfReplicas = between(1, 2);
        internalCluster().ensureAtLeastNumDataNodes(numberOfReplicas + between(1, 2));
        createIndex(indexName, indexSettings(1, numberOfReplicas).put("index.routing.rebalance.enable", "none").build());
        int iterations = between(1, 3);
        for (int iter = 0; iter < iterations; iter++) {
            indexRandom(
                randomBoolean(),
                randomBoolean(),
                randomBoolean(),
                IntStream.range(0, randomIntBetween(0, 50)).mapToObj(n -> prepareIndex(indexName).setSource("num", n)).collect(toList())
            );
            ensureGreen(indexName);

            // Closing an index should execute noop peer recovery
            assertAcked(indicesAdmin().prepareClose(indexName).get());
            assertIndexIsClosed(indexName);
            ensureGreen(indexName);
            assertNoFileBasedRecovery(indexName);
            internalCluster().assertSameDocIdsOnShards();

            // Open a closed index should execute noop recovery
            assertAcked(indicesAdmin().prepareOpen(indexName).get());
            assertIndexIsOpened(indexName);
            ensureGreen(indexName);
            assertNoFileBasedRecovery(indexName);
            internalCluster().assertSameDocIdsOnShards();
        }
    }

    /**
     * Ensures that if a replica of a closed index does not have the same content as the primary, then a file-based recovery will occur.
     */
    public void testRecoverExistingReplica() throws Exception {
        final String indexName = "test-recover-existing-replica";
        internalCluster().ensureAtLeastNumDataNodes(2);
        List<String> dataNodes = randomSubsetOf(
            2,
            clusterService().state().nodes().getDataNodes().values().stream().map(DiscoveryNode::getName).collect(Collectors.toSet())
        );
        createIndex(indexName, indexSettings(1, 1).put("index.routing.allocation.include._name", String.join(",", dataNodes)).build());
        indexRandom(
            randomBoolean(),
            randomBoolean(),
            randomBoolean(),
            IntStream.range(0, randomIntBetween(0, 50)).mapToObj(n -> prepareIndex(indexName).setSource("num", n)).collect(toList())
        );
        ensureGreen(indexName);
        indicesAdmin().prepareFlush(indexName).get();

        // index more documents while one shard copy is offline
        internalCluster().restartNode(dataNodes.get(1), new InternalTestCluster.RestartCallback() {
            @Override
            public Settings onNodeStopped(String nodeName) throws Exception {
                Client client = client(dataNodes.get(0));
                int moreDocs = randomIntBetween(1, 50);
                for (int i = 0; i < moreDocs; i++) {
                    client.prepareIndex(indexName).setSource("num", i).get();
                }
                assertAcked(indicesAdmin().prepareClose(indexName));
                return super.onNodeStopped(nodeName);
            }
        });
        assertIndexIsClosed(indexName);
        ensureGreen(indexName);
        internalCluster().assertSameDocIdsOnShards();
        for (RecoveryState recovery : indicesAdmin().prepareRecoveries(indexName).get().shardRecoveryStates().get(indexName)) {
            if (recovery.getPrimary() == false) {
                assertThat(recovery.getIndex().fileDetails(), not(empty()));
            }
        }
    }

    /**
     * Test for https://github.com/elastic/elasticsearch/issues/47276 which checks that the persisted metadata on a data node does not
     * become inconsistent when using replicated closed indices.
     */
    public void testRelocatedClosedIndexIssue() throws Exception {
        final String indexName = "closed-index";
        final List<String> dataNodes = internalCluster().startDataOnlyNodes(2);
        // allocate shard to first data node
        createIndex(indexName, indexSettings(1, 0).put("index.routing.allocation.include._name", dataNodes.get(0)).build());
        indexRandom(
            randomBoolean(),
            randomBoolean(),
            randomBoolean(),
            IntStream.range(0, randomIntBetween(0, 50)).mapToObj(n -> prepareIndex(indexName).setSource("num", n)).collect(toList())
        );
        assertAcked(indicesAdmin().prepareClose(indexName));
        // move single shard to second node
        updateIndexSettings(Settings.builder().put("index.routing.allocation.include._name", dataNodes.get(1)), indexName);
        ensureGreen(indexName);
        internalCluster().fullRestart();
        ensureGreen(indexName);
        assertIndexIsClosed(indexName);
    }

    public void testResyncPropagatePrimaryTerm() throws Exception {
        internalCluster().ensureAtLeastNumDataNodes(3);
        final String indexName = "closed_indices_promotion";
        createIndex(indexName, 1, 2);
        indexRandom(
            randomBoolean(),
            randomBoolean(),
            randomBoolean(),
            IntStream.range(0, randomIntBetween(0, 50)).mapToObj(n -> prepareIndex(indexName).setSource("num", n)).collect(toList())
        );
        ensureGreen(indexName);
        assertAcked(indicesAdmin().prepareClose(indexName));
        assertIndexIsClosed(indexName);
        ensureGreen(indexName);
        String nodeWithPrimary = clusterService().state()
            .nodes()
            .get(clusterService().state().routingTable().index(indexName).shard(0).primaryShard().currentNodeId())
            .getName();
        internalCluster().restartNode(nodeWithPrimary, new InternalTestCluster.RestartCallback());
        ensureGreen(indexName);
        long primaryTerm = clusterService().state().metadata().getProject().index(indexName).primaryTerm(0);
        for (String nodeName : internalCluster().nodesInclude(indexName)) {
            IndexShard shard = internalCluster().getInstance(IndicesService.class, nodeName)
                .indexService(resolveIndex(indexName))
                .getShard(0);
            assertThat(shard.routingEntry().toString(), shard.getOperationPrimaryTerm(), equalTo(primaryTerm));
        }
    }

    public void testSearcherId() throws Exception {
        final String indexName = "test_commit_id";
        final int numberOfShards = randomIntBetween(1, 5);
        createIndex(indexName, numberOfShards, 0);
        indexRandom(
            randomBoolean(),
            randomBoolean(),
            randomBoolean(),
            IntStream.range(0, randomIntBetween(0, 50)).mapToObj(n -> prepareIndex(indexName).setSource("num", n)).collect(toList())
        );
        ensureGreen(indexName);
        assertAcked(indicesAdmin().prepareClose(indexName));
        assertIndexIsClosed(indexName);
        ensureGreen(indexName);
        if (randomBoolean()) {
            setReplicaCount(1, indexName);
            internalCluster().ensureAtLeastNumDataNodes(2);
            ensureGreen(indexName);
        }
        String[] searcherIds = new String[numberOfShards];
        Set<String> allocatedNodes = internalCluster().nodesInclude(indexName);
        for (String node : allocatedNodes) {
            IndexService indexService = internalCluster().getInstance(IndicesService.class, node).indexServiceSafe(resolveIndex(indexName));
            for (IndexShard shard : indexService) {
                try (Engine.SearcherSupplier searcher = shard.acquireSearcherSupplier()) {
                    assertNotNull(searcher.getSearcherId());
                    if (searcherIds[shard.shardId().id()] != null) {
                        assertThat(searcher.getSearcherId(), equalTo(searcherIds[shard.shardId().id()]));
                    } else {
                        searcherIds[shard.shardId().id()] = searcher.getSearcherId();
                    }
                }
            }
        }
        for (String node : allocatedNodes) {
            if (randomBoolean()) {
                internalCluster().restartNode(node);
            }
        }
        ensureGreen(indexName);
        allocatedNodes = internalCluster().nodesInclude(indexName);
        for (String node : allocatedNodes) {
            IndexService indexService = internalCluster().getInstance(IndicesService.class, node).indexServiceSafe(resolveIndex(indexName));
            for (IndexShard shard : indexService) {
                try (Engine.SearcherSupplier searcher = shard.acquireSearcherSupplier()) {
                    assertNotNull(searcher.getSearcherId());
                    assertThat(searcher.getSearcherId(), equalTo(searcherIds[shard.shardId().id()]));
                }
            }
        }
    }

    private static void closeIndices(final String... indices) {
        closeIndices(indicesAdmin().prepareClose(indices));
    }

    private static void closeIndices(final CloseIndexRequestBuilder requestBuilder) {
        final CloseIndexResponse response = requestBuilder.get();
        assertThat(response.isAcknowledged(), is(true));
        assertThat(response.isShardsAcknowledged(), is(true));

        final String[] indices = requestBuilder.request().indices();
        if (indices != null) {
            assertThat(response.getIndices().size(), equalTo(indices.length));
            for (String index : indices) {
                CloseIndexResponse.IndexResult indexResult = response.getIndices()
                    .stream()
                    .filter(result -> index.equals(result.getIndex().getName()))
                    .findFirst()
                    .get();
                assertThat(indexResult, notNullValue());
                assertThat(indexResult.hasFailures(), is(false));
                assertThat(indexResult.getException(), nullValue());
                assertThat(indexResult.getShards(), notNullValue());
                Arrays.stream(indexResult.getShards()).forEach(shardResult -> {
                    assertThat(shardResult.hasFailures(), is(false));
                    assertThat(shardResult.getFailures(), notNullValue());
                    assertThat(shardResult.getFailures().length, equalTo(0));
                });
            }
        } else {
            assertThat(response.getIndices().size(), equalTo(0));
        }
    }

    static void assertIndexIsClosed(final String... indices) {
        final ClusterState clusterState = clusterAdmin().prepareState(TEST_REQUEST_TIMEOUT).get().getState();
        for (String index : indices) {
            final IndexMetadata indexMetadata = clusterState.metadata().getProject().indices().get(index);
            assertThat(indexMetadata.getState(), is(IndexMetadata.State.CLOSE));
            final Settings indexSettings = indexMetadata.getSettings();
            assertThat(indexSettings.hasValue(MetadataIndexStateService.VERIFIED_BEFORE_CLOSE_SETTING.getKey()), is(true));
            assertThat(indexSettings.getAsBoolean(MetadataIndexStateService.VERIFIED_BEFORE_CLOSE_SETTING.getKey(), false), is(true));
            assertThat(clusterState.routingTable().index(index), notNullValue());
            assertThat(
                clusterState.blocks().hasIndexBlock(Metadata.DEFAULT_PROJECT_ID, index, MetadataIndexStateService.INDEX_CLOSED_BLOCK),
                is(true)
            );
            assertThat(
                "Index " + index + " must have only 1 block with [id=" + MetadataIndexStateService.INDEX_CLOSED_BLOCK_ID + "]",
                clusterState.blocks()
                    .indices(Metadata.DEFAULT_PROJECT_ID)
                    .getOrDefault(index, emptySet())
                    .stream()
                    .filter(clusterBlock -> clusterBlock.id() == MetadataIndexStateService.INDEX_CLOSED_BLOCK_ID)
                    .count(),
                equalTo(1L)
            );
        }
    }

    static void assertIndexIsOpened(final String... indices) {
        final ClusterState clusterState = clusterAdmin().prepareState(TEST_REQUEST_TIMEOUT).get().getState();
        for (String index : indices) {
            final IndexMetadata indexMetadata = clusterState.metadata().getProject().indices().get(index);
            assertThat(indexMetadata.getState(), is(IndexMetadata.State.OPEN));
            assertThat(indexMetadata.getSettings().hasValue(MetadataIndexStateService.VERIFIED_BEFORE_CLOSE_SETTING.getKey()), is(false));
            assertThat(clusterState.routingTable().index(index), notNullValue());
            assertThat(clusterState.blocks().hasIndexBlock(index, MetadataIndexStateService.INDEX_CLOSED_BLOCK), is(false));
        }
    }

    static void assertException(final Throwable throwable, final String indexName) {
        final Throwable t = ExceptionsHelper.unwrapCause(throwable);
        if (t instanceof ClusterBlockException clusterBlockException) {
            assertThat(clusterBlockException.blocks(), hasSize(1));
            assertTrue(clusterBlockException.blocks().stream().allMatch(b -> b.id() == MetadataIndexStateService.INDEX_CLOSED_BLOCK_ID));
        } else if (t instanceof IndexClosedException indexClosedException) {
            assertThat(indexClosedException.getIndex(), notNullValue());
            assertThat(indexClosedException.getIndex().getName(), equalTo(indexName));
        } else if (t instanceof IndexNotFoundException indexNotFoundException) {
            assertThat(indexNotFoundException.getIndex(), notNullValue());
            assertThat(indexNotFoundException.getIndex().getName(), equalTo(indexName));
        } else {
            fail("Unexpected exception: " + t);
        }
    }

    void assertNoFileBasedRecovery(String indexName) {
        for (RecoveryState recovery : indicesAdmin().prepareRecoveries(indexName).get().shardRecoveryStates().get(indexName)) {
            if (recovery.getPrimary() == false) {
                assertThat(recovery.getIndex().fileDetails(), empty());
            }
        }
    }
}
