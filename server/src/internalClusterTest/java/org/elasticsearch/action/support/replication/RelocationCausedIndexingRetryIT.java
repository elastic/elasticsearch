/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.support.replication;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkShardRequest;
import org.elasticsearch.action.bulk.TransportShardBulkAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.engine.EngineTestCase;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.recovery.PeerRecoveryTargetService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.disruption.BlockClusterStateProcessing;
import org.elasticsearch.test.transport.MockTransportService;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.action.support.replication.TransportReplicationActionTests.getRoutedBasedOnClusterVersion;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.lessThan;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class RelocationCausedIndexingRetryIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(MockTransportService.TestPlugin.class);
    }

    public void testPrimaryRelocationShouldNotMarkIndexRequestAsRetry() throws Exception {
        assertPrimaryRelocationDoesNotMarkIndexRequestAsRetry(false);
    }

    /**
     * The relocation source can apply the cluster state that completes the relocation before the target does. The target then cannot
     * route the request it is sent, and retries it in the reroute phase once it has caught up.
     */
    public void testPrimaryRelocationToTargetWithStaleClusterStateShouldNotMarkIndexRequestAsRetry() throws Exception {
        assertPrimaryRelocationDoesNotMarkIndexRequestAsRetry(true);
    }

    private void assertPrimaryRelocationDoesNotMarkIndexRequestAsRetry(boolean targetHasStaleClusterState) throws Exception {
        String master = internalCluster().startMasterOnlyNode();
        String node1 = internalCluster().startDataOnlyNode();
        String node2 = internalCluster().startDataOnlyNode();
        createIndex("index1", indexSettings(1, 0).put("index.routing.allocation.require._name", node1).build());
        ensureGreen("index1");
        indexRandom(randomBoolean(), "index1", randomIntBetween(10, 100));
        flush("index1");

        final var blockNode2ClusterState = new BlockClusterStateProcessing(node2, random());
        if (targetHasStaleClusterState) {
            internalCluster().setDisruptionScheme(blockNode2ClusterState);
        }

        var handOffReceivedLatch = new CountDownLatch(1);
        var continueRelocationLatch = new CountDownLatch(1);
        var node1ReceivedBulk = new CountDownLatch(1);
        var node2ReceivedTheBulk = new AtomicBoolean(false);

        MockTransportService node1TransportService = MockTransportService.getInstance(node1);
        MockTransportService node2TransportService = MockTransportService.getInstance(node2);
        node1TransportService.addRequestHandlingBehavior(
            // needed to make sure we hook into TransportReplicationAction.handlePrimaryRequest
            TransportShardBulkAction.ACTION_NAME + "[p]",
            (handler, request, channel, task) -> {
                node1ReceivedBulk.countDown();
                handler.messageReceived(request, channel, task);
            }
        );
        // hook into the relocation by waiting for the context hand off request, then relocate the shard
        node2TransportService.addRequestHandlingBehavior(
            PeerRecoveryTargetService.Actions.HANDOFF_PRIMARY_CONTEXT,
            (handler, request, channel, task) -> {
                logger.info("==> received hand off request [{}]", request);
                handOffReceivedLatch.countDown();
                logger.info("==> waiting to continue relocation...");
                safeAwait(continueRelocationLatch);
                if (targetHasStaleClusterState) {
                    // node2 will not apply the cluster state that completes the relocation until this is released
                    blockNode2ClusterState.startDisrupting();
                }
                handler.messageReceived(request, channel, task);
            }
        );
        // hook into bulk shard indexing requests on node2 and assert that they are not a retry
        node2TransportService.addRequestHandlingBehavior(TransportShardBulkAction.ACTION_NAME, (handler, request, channel, task) -> {
            node2ReceivedTheBulk.set(true);
            var bulkShardRequest = (BulkShardRequest) request;
            logger.info("==> node2 received bulk shard request [{}]", bulkShardRequest);
            assertTrue(
                Arrays.stream(bulkShardRequest.items())
                    .filter(r -> r.request() instanceof IndexRequest)
                    .allMatch(r -> ((IndexRequest) r.request()).isRetry() == false)
            );
            if (targetHasStaleClusterState) {
                assertThat(
                    internalCluster().clusterService(node2).state().version(),
                    lessThan(getRoutedBasedOnClusterVersion(bulkShardRequest))
                );
                // the reroute phase waits for a newer cluster state before it retries
                handler.messageReceived(request, channel, task);
                blockNode2ClusterState.stopDisrupting();
                return;
            }
            // We need to wait for the cluster state to advance to the version of the request, otherwise the request will be retried in the
            // reroute phase.
            ClusterServiceUtils.addTemporaryStateListener(
                internalCluster().clusterService(node2),
                state -> state.version() >= getRoutedBasedOnClusterVersion(bulkShardRequest)
            ).addListener(new ActionListener<>() {
                @Override
                public void onResponse(Void unused) {
                    try {
                        handler.messageReceived(request, channel, task);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    fail("failed to wait for node2 cluster state to advance: " + e.getMessage());
                }
            });

        });
        updateIndexSettings(Settings.builder().put("index.routing.allocation.require._name", node2), "index1");
        logger.info("==> Updated allocation tag, waiting for hand off...");
        safeAwait(handOffReceivedLatch);
        logger.info("==> sending some indexing requests");
        final var useBulk = randomBoolean();
        final var indexRequestUsesId = randomBoolean();
        final var indexRequestsWithId = useBulk ? randomIntBetween(1, 10) : indexRequestUsesId ? 1 : 0;
        final var indexRequestsWithoutId = useBulk ? randomIntBetween(1, 10) : indexRequestUsesId ? 0 : 1;
        final List<String> returnedIds;
        if (useBulk) {
            BulkRequestBuilder bulk = client().prepareBulk();
            for (int i = 0; i < indexRequestsWithId; i++) {
                bulk.add(prepareIndex("index1").setId(randomUUID()).setSource("text", randomAlphaOfLength(10)));
            }
            for (int i = 0; i < indexRequestsWithoutId; i++) {
                bulk.add(prepareIndex("index1").setSource("text", randomAlphaOfLength(10)));
            }
            final var listener = bulk.execute();
            safeAwait(node1ReceivedBulk);
            continueRelocationLatch.countDown();
            final var bulkResponse = listener.get();
            assertThat(node2ReceivedTheBulk.get(), equalTo(true));
            assertThat(bulkResponse.hasFailures(), equalTo(false));
            assertThat(bulkResponse.getItems().length, equalTo(indexRequestsWithId + indexRequestsWithoutId));
            returnedIds = Arrays.stream(bulkResponse.getItems()).map(item -> item.getId()).toList();
        } else {
            final var indexRequest = client(node1).prepareIndex("index1");
            if (indexRequestUsesId) {
                indexRequest.setId(randomUUID());
            }
            final var listener = indexRequest.setSource("text", randomAlphaOfLength(10)).execute();
            safeAwait(node1ReceivedBulk);
            continueRelocationLatch.countDown();
            final var indexResponse = listener.get();
            assertThat(node2ReceivedTheBulk.get(), equalTo(true));
            assertThat(indexResponse.status(), equalTo(RestStatus.CREATED));
            returnedIds = List.of(indexResponse.getId());
        }
        // Verify that each returned doc ID can be looked up; a incorrectly retried auto-ID request could cause a mismatch.
        for (var id : returnedIds) {
            var getResponse = client().prepareGet("index1", id).get();
            assertTrue("document [" + id + "] should exist", getResponse.isExists());
        }

        var indicesService = internalCluster().getInstance(org.elasticsearch.indices.IndicesService.class, node2);
        var indexService = indicesService.indexService(resolveIndex("index1"));
        var indexShard = indexService.getShardOrNull(0);
        assertNotNull(indexShard);
        long numVersionLookups = indexShard.tryWithEngineOrNull(engine -> {
            assertNotNull(engine);
            return EngineTestCase.getNumIndexVersionLookups(engine);
        });
        assertThat(numVersionLookups, equalTo((long) indexRequestsWithId));
    }

    /**
     * While the relocation source applies the cluster state that completes the relocation, its shard is already removed but it still
     * routes requests on the previous cluster state, to a local primary that is gone.
     */
    public void testRequestRoutedToRemovedRelocationSourceShardShouldNotMarkIndexRequestAsRetry() throws Exception {
        internalCluster().startMasterOnlyNode();
        String node1 = internalCluster().startDataOnlyNode();
        String node2 = internalCluster().startDataOnlyNode();
        createIndex("index1", indexSettings(1, 0).put("index.routing.allocation.require._name", node1).build());
        ensureGreen("index1");
        indexRandom(randomBoolean(), "index1", randomIntBetween(10, 100));
        flush("index1");
        final var shardId = new ShardId(resolveIndex("index1"), 0);

        final var node1IndicesService = internalCluster().getInstance(IndicesService.class, node1);
        final var node1RemovedShard = new CountDownLatch(1);
        final var continueApplyingClusterState = new CountDownLatch(1);
        // runs after the applier that removes the shard, while node1 still exposes the previous cluster state
        internalCluster().clusterService(node1).addStateApplier(event -> {
            if (node1RemovedShard.getCount() > 0 && node1IndicesService.getShardOrNull(shardId) == null) {
                node1RemovedShard.countDown();
                safeAwait(continueApplyingClusterState);
            }
        });

        final var node1HandledPrimaryRequest = new CountDownLatch(1);
        MockTransportService.getInstance(node1)
            .addRequestHandlingBehavior(TransportShardBulkAction.ACTION_NAME + "[p]", (handler, request, channel, task) -> {
                handler.messageReceived(request, channel, task);
                node1HandledPrimaryRequest.countDown();
            });
        final var node2ReceivedTheBulk = new AtomicBoolean(false);
        final var node2ReceivedRetry = new AtomicBoolean(false);
        MockTransportService.getInstance(node2)
            .addRequestHandlingBehavior(TransportShardBulkAction.ACTION_NAME, (handler, request, channel, task) -> {
                node2ReceivedTheBulk.set(true);
                if (Arrays.stream(((BulkShardRequest) request).items())
                    .anyMatch(r -> r.request() instanceof IndexRequest indexRequest && indexRequest.isRetry())) {
                    node2ReceivedRetry.set(true);
                }
                handler.messageReceived(request, channel, task);
            });

        updateIndexSettings(Settings.builder().put("index.routing.allocation.require._name", node2), "index1");
        safeAwait(node1RemovedShard);

        final int indexRequests = randomIntBetween(1, 10);
        BulkRequestBuilder bulk = client(node1).prepareBulk();
        for (int i = 0; i < indexRequests; i++) {
            bulk.add(prepareIndex("index1").setSource("text", randomAlphaOfLength(10)));
        }
        final var listener = bulk.execute();
        safeAwait(node1HandledPrimaryRequest);
        continueApplyingClusterState.countDown();

        final var bulkResponse = listener.get();
        assertThat(bulkResponse.hasFailures(), equalTo(false));
        assertThat(bulkResponse.getItems().length, equalTo(indexRequests));
        assertThat(node2ReceivedTheBulk.get(), equalTo(true));
        assertThat(node2ReceivedRetry.get(), equalTo(false));

        var indexShard = internalCluster().getInstance(IndicesService.class, node2).getShardOrNull(shardId);
        assertNotNull(indexShard);
        long numVersionLookups = indexShard.tryWithEngineOrNull(engine -> {
            assertNotNull(engine);
            return EngineTestCase.getNumIndexVersionLookups(engine);
        });
        assertThat(numVersionLookups, equalTo(0L));
    }
}
