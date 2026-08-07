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
import org.elasticsearch.indices.recovery.PeerRecoveryTargetService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.transport.MockTransportService;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.action.support.replication.TransportReplicationActionTests.getRoutedBasedOnClusterVersion;
import static org.hamcrest.CoreMatchers.equalTo;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class RelocationCausedIndexingRetryIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(MockTransportService.TestPlugin.class);
    }

    public void testPrimaryRelocationShouldNotMarkIndexRequestAsRetry() throws Exception {
        String master = internalCluster().startMasterOnlyNode();
        String node1 = internalCluster().startDataOnlyNode();
        String node2 = internalCluster().startDataOnlyNode();
        createIndex("index1", indexSettings(1, 0).put("index.routing.allocation.require._name", node1).build());
        ensureGreen("index1");
        indexRandom(randomBoolean(), "index1", randomIntBetween(10, 100));
        flush("index1");

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
}
