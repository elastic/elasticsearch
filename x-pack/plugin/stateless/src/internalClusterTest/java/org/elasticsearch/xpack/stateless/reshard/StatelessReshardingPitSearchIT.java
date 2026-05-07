/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.reshard;

import org.elasticsearch.action.search.ClosePointInTimeRequest;
import org.elasticsearch.action.search.ClosePointInTimeResponse;
import org.elasticsearch.action.search.OpenPointInTimeRequest;
import org.elasticsearch.action.search.OpenPointInTimeResponse;
import org.elasticsearch.action.search.TransportClosePointInTimeAction;
import org.elasticsearch.action.search.TransportOpenPointInTimeAction;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexReshardingState;
import org.elasticsearch.cluster.routing.IndexRouting;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.PointInTimeBuilder;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.xpack.stateless.AbstractStatelessPluginIntegTestCase;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;

import static org.elasticsearch.index.IndexSettings.INDEX_REFRESH_INTERVAL_SETTING;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.elasticsearch.xpack.stateless.reshard.ReshardingTestHelpers.indexMetadata;
import static org.elasticsearch.xpack.stateless.reshard.ReshardingTestHelpers.makeIdThatRoutesToShard;
import static org.elasticsearch.xpack.stateless.reshard.SplitSourceService.RESHARD_SPLIT_DELETE_UNOWNED_GRACE_PERIOD;

public class StatelessReshardingPitSearchIT extends AbstractStatelessPluginIntegTestCase {
    public void testPitSearchDuringReshard() {
        var masterNode = startMasterOnlyNode();
        String indexNode = startIndexNode();
        startSearchNode();
        String searchCoordinator = startSearchNode();
        ensureStableCluster(4);

        final int multiple = 2;
        final String indexName = randomIndexName();
        // Disable periodic refresh because we want to explicitly control it.
        createIndex(indexName, indexSettings(1, 1).put(INDEX_REFRESH_INTERVAL_SETTING.getKey(), TimeValue.MINUS_ONE).build());
        ensureGreen(indexName);

        var index = resolveIndex(indexName);
        var indexMetadata = indexMetadata(internalCluster().clusterService(masterNode).state(), index);

        // We re-create the metadata directly in test in order to have access to after-reshard routing.
        var wouldBeMetadata = IndexMetadata.builder(indexMetadata).reshardAddShards(multiple).build();
        var wouldBeAfterSplitRouting = IndexRouting.fromIndexMetadata(wouldBeMetadata);

        var allIndexedDocuments = Collections.synchronizedMap(new HashMap<String, Integer>());
        int documentsPerShardPerRound = randomIntBetween(5, 10);

        var initialIndexedDocuments = indexDocuments(indexName, 2, documentsPerShardPerRound, "initial", wouldBeAfterSplitRouting);
        allIndexedDocuments.putAll(initialIndexedDocuments);

        // Search works before resharding.
        refresh(indexName);
        var initialPitId = openPit(searchCoordinator, indexName, 1);
        assertPit(searchCoordinator, initialPitId, 1, initialIndexedDocuments.keySet());

        CountDownLatch handOffStarted = new CountDownLatch(multiple - 1); // (multiple - 1) target shards
        CyclicBarrier stateTransitionBlock = new CyclicBarrier(multiple); // (multiple - 1) target shards + the test itself

        MockTransportService indexTransportService = MockTransportService.getInstance(indexNode);
        indexTransportService.addSendBehavior((connection, requestId, action, request, options) -> {
            try {
                if (TransportUpdateSplitTargetShardStateAction.TYPE.name().equals(action)) {
                    if (handOffStarted.getCount() > 0) {
                        handOffStarted.countDown();
                    }
                    stateTransitionBlock.await();
                }
                connection.sendRequest(requestId, action, request, options);
            } catch (InterruptedException | BrokenBarrierException e) {
                throw new RuntimeException(e);
            }
        });

        ReshardIndexRequest reshardRequest = new ReshardIndexRequest(indexName);
        client(masterNode).execute(TransportReshardAction.TYPE, reshardRequest).actionGet();

        awaitClusterState(searchCoordinator, clusterState -> indexMetadata(clusterState, index).getReshardingMetadata() != null);

        // Wait for all target shards to arrive at handoff point.
        safeAwait(handOffStarted);

        // We can still use the initial PIT.
        assertPit(searchCoordinator, initialPitId, 1, initialIndexedDocuments.keySet());

        // We can't index any documents since the handoff is in progress and can't refresh for the same reason.
        // We won't be testing that here since it is covered in other tests.
        // But we can open a new PIT here which should be identical to the initial one.
        var duringHandoffPitId = openPit(searchCoordinator, indexName, 1);
        assertPit(searchCoordinator, duringHandoffPitId, 1, initialIndexedDocuments.keySet());

        // unblock HANDOFF transition
        safeAwait(stateTransitionBlock);
        awaitClusterState(
            searchCoordinator,
            clusterState -> indexMetadata(clusterState, index).getReshardingMetadata()
                .getSplit()
                .targetStates()
                .allMatch(s -> s == IndexReshardingState.Split.TargetShardState.HANDOFF)
        );

        // Now that the handoff is complete we can do indexing but refresh is still blocked on the target shard.
        // We'll index documents here but won't refresh (this is covered in other tests).
        // We'll assert these documents in SPLIT state once we can refresh.
        var afterHandoffIndexedDocuments = indexDocuments(
            indexName,
            2,
            documentsPerShardPerRound,
            "afterHandoff",
            wouldBeAfterSplitRouting
        );
        allIndexedDocuments.putAll(afterHandoffIndexedDocuments);

        // We can also open another PIT.
        var afterHandoffPitId = openPit(searchCoordinator, indexName, 1);
        assertPit(searchCoordinator, afterHandoffPitId, 1, initialIndexedDocuments.keySet());
        // All existing PITs are still working too.
        assertPit(searchCoordinator, initialPitId, 1, initialIndexedDocuments.keySet());
        assertPit(searchCoordinator, duringHandoffPitId, 1, initialIndexedDocuments.keySet());

        // unblock SPLIT transition
        safeAwait(stateTransitionBlock);

        awaitClusterState(
            searchCoordinator,
            clusterState -> clusterState.getMetadata()
                .indexMetadata(index)
                .getReshardingMetadata()
                .getSplit()
                .targetStates()
                .allMatch(s -> s == IndexReshardingState.Split.TargetShardState.SPLIT)
        );

        // Transition of target shards to DONE state is blocked, all target shards are in SPLIT state.
        // Used for asserts later.
        var allIndexedDocumentsAtSplit = new HashSet<>(allIndexedDocuments.keySet());

        // Now we can refresh.
        refresh(indexName);

        // And open another PIT now with two shards.
        var splitPitId = openPit(searchCoordinator, indexName, 2);
        assertPit(searchCoordinator, splitPitId, 2, allIndexedDocuments.keySet());

        // Indexing also works as expected.
        var splitIndexedDocuments = indexDocuments(indexName, 2, documentsPerShardPerRound, "split", wouldBeAfterSplitRouting);
        allIndexedDocuments.putAll(splitIndexedDocuments);

        // All existing PITs are still working too.
        assertPit(searchCoordinator, initialPitId, 1, initialIndexedDocuments.keySet());
        assertPit(searchCoordinator, duringHandoffPitId, 1, initialIndexedDocuments.keySet());
        assertPit(searchCoordinator, afterHandoffPitId, 1, initialIndexedDocuments.keySet());

        // unblock DONE transition
        safeAwait(stateTransitionBlock);

        awaitClusterState(searchCoordinator, clusterState -> {
            var metadata = indexMetadata(clusterState, index);
            if (metadata.getReshardingMetadata() == null) {
                // all shards are DONE so resharding metadata is removed by master
                return true;
            }

            boolean targetsDone = metadata.getReshardingMetadata()
                .getSplit()
                .targetStates()
                .allMatch(s -> s == IndexReshardingState.Split.TargetShardState.DONE);
            boolean sourceDone = metadata.getReshardingMetadata()
                .getSplit()
                .sourceStates()
                .allMatch(s -> s == IndexReshardingState.Split.SourceShardState.DONE);
            return targetsDone && sourceDone;
        });

        // All target shards and the source shard are DONE.
        refresh(indexName);

        var donePitId = openPit(searchCoordinator, indexName, 2);
        assertPit(searchCoordinator, donePitId, 2, allIndexedDocuments.keySet());

        // All existing PITs are still working too.
        assertPit(searchCoordinator, initialPitId, 1, initialIndexedDocuments.keySet());
        assertPit(searchCoordinator, duringHandoffPitId, 1, initialIndexedDocuments.keySet());
        assertPit(searchCoordinator, afterHandoffPitId, 1, initialIndexedDocuments.keySet());
        assertPit(searchCoordinator, splitPitId, 2, allIndexedDocumentsAtSplit);

        waitForReshardCompletion(index);

        closePit(initialPitId);
        closePit(duringHandoffPitId);
        closePit(afterHandoffPitId);
        closePit(splitPitId);
        closePit(donePitId);
    }

    @Override
    protected Settings.Builder nodeSettings() {
        return super.nodeSettings()
            // These tests are carefully set up and do not hit the situations that the delete unowned grace period prevents.
            .put(RESHARD_SPLIT_DELETE_UNOWNED_GRACE_PERIOD.getKey(), TimeValue.ZERO)
            // Disable so that they don't randomly flush and break our asserts.
            .put(disableIndexingDiskAndMemoryControllersNodeSettings());
    }

    private Map<String, Integer> indexDocuments(
        String indexName,
        int shards,
        int perShardCount,
        String idPrefix,
        IndexRouting finalRouting
    ) {
        var docsPerShard = new HashMap<String, Integer>();

        for (int shard = 0; shard < shards; shard++) {
            for (int documentCount = 0; documentCount < perShardCount; documentCount++) {
                String id = makeIdThatRoutesToShard(finalRouting, shard, idPrefix);
                prepareIndex(indexName).setId(id).setSource(Map.of("field", randomAlphaOfLength(5))).get();
                docsPerShard.put(id, shard);
            }
        }

        return docsPerShard;
    }

    private BytesReference openPit(String searchCoordinator, String indexName, int shards) {
        var request = new OpenPointInTimeRequest(indexName).allowPartialSearchResults(false).keepAlive(TimeValue.timeValueMinutes(2));

        OpenPointInTimeResponse response = client(searchCoordinator).execute(TransportOpenPointInTimeAction.TYPE, request).actionGet();
        assertEquals(0, response.getFailedShards());
        assertEquals(shards, response.getTotalShards());

        return response.getPointInTimeId();
    }

    private void assertPit(String searchCoordinator, BytesReference pitId, int totalShards, Set<String> expectedDocuments) {
        var searchRequest = client(searchCoordinator).prepareSearch()
            .setQuery(QueryBuilders.matchAllQuery())
            .setSize(10000)
            .setTrackTotalHits(true)
            .setAllowPartialSearchResults(false)
            .setPointInTime(new PointInTimeBuilder(pitId));

        assertResponse(searchRequest, response -> {
            final var shardCount = response.getTotalShards();
            assertEquals("unexpected shard count in search response", totalShards, shardCount);
            assertEquals("Wrong number of documents in PIT search response", expectedDocuments.size(), response.getHits().getHits().length);

            var ids = new HashSet<String>();
            for (var hit : response.getHits().getHits()) {
                assertTrue("Duplicate search results in PIT search response", ids.add(hit.getId()));
            }
            assertEquals("unexpected documents in PIT search response", expectedDocuments, ids);
        });
    }

    private void closePit(BytesReference readerId) {
        ClosePointInTimeResponse closeResponse = client().execute(
            TransportClosePointInTimeAction.TYPE,
            new ClosePointInTimeRequest(readerId)
        ).actionGet();
        assertTrue(closeResponse.isSucceeded());
    }

    private void waitForReshardCompletion(Index index) {
        awaitClusterState((state) -> state.metadata().projectFor(index).index(index.getName()).getReshardingMetadata() == null);
    }
}
