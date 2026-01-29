/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.get;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexReshardingMetadata;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.routing.IndexRouting;
import org.elasticsearch.cluster.routing.SplitShardCountSummary;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.get.GetResult;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.index.IndexVersionUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class MultiGetShardSplitHelperTests extends ESTestCase {

    private static final Logger logger = LogManager.getLogger(MultiGetShardSplitHelperTests.class);

    // ========================================
    // Tests for needsSplitCoordination
    // ========================================

    public void testNeedsSplitCoordinationWithUnsetSummary() {
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        var settings = indexSettings(IndexVersionUtils.randomCompatibleVersion(), 2, 0).build();
        IndexMetadata indexMetadata = IndexMetadata.builder(indexName).settings(settings).build();

        MultiGetShardRequest request = createMultiGetShardRequest(indexName, 0);
        // Request has UNSET summary by default
        assertFalse(MultiGetShardSplitHelper.needsSplitCoordination(request, indexMetadata, 0));
    }

    public void testNeedsSplitCoordinationWithMatchingSummary() {
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        var settings = indexSettings(IndexVersionUtils.randomCompatibleVersion(), 2, 0).build();
        IndexMetadata indexMetadata = IndexMetadata.builder(indexName).settings(settings).build();
        indexMetadata = IndexMetadata.builder(indexMetadata)
            .reshardingMetadata(IndexReshardingMetadata.newSplitByMultiple(2, 2))
            .build();

        SplitShardCountSummary currentSummary = SplitShardCountSummary.forSearch(indexMetadata, 0);
        assertThat(currentSummary, equalTo(SplitShardCountSummary.fromInt(2)));

        MultiGetShardRequest request = createMultiGetShardRequest(indexName, 0);
        request.setSplitShardCountSummary(currentSummary);

        // Should return false because the split summary matches
        assertFalse(MultiGetShardSplitHelper.needsSplitCoordination(request, indexMetadata, 0));
    }

    public void testNeedsSplitCoordinationWithMismatchedSummary() {
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        var settings = indexSettings(IndexVersionUtils.randomCompatibleVersion(), 2, 0).build();
        IndexMetadata indexMetadata = IndexMetadata.builder(indexName).settings(settings).build();

        // Request has a different summary (1) than the current state (2)
        MultiGetShardRequest request = createMultiGetShardRequest(indexName, 0);
        request.setSplitShardCountSummary(SplitShardCountSummary.fromInt(1));

        // Should return true because the split summary does not match
        assertTrue(MultiGetShardSplitHelper.needsSplitCoordination(request, indexMetadata, 0));
    }

    // ========================================
    // Tests for coordinateSplitRequest
    // ========================================

    public void testCoordinateSplitRequestAllItemsToSource() throws Exception {
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        var settings = indexSettings(IndexVersionUtils.randomCompatibleVersion(), 2, 0).build();
        IndexMetadata indexMetadata = IndexMetadata.builder(indexName).settings(settings).build();
        var projectMetadata = ProjectMetadata.builder(randomProjectIdOrDefault()).put(indexMetadata, randomBoolean()).build();

        ShardId sourceShardId = new ShardId(indexMetadata.getIndex(), 0);
        IndexRouting indexRouting = IndexRouting.fromIndexMetadata(indexMetadata);

        // Create request with stale summary (1 instead of actual 2)
        MultiGetShardRequest originalRequest = createMultiGetShardRequest(indexName, sourceShardId.getId());
        originalRequest.setSplitShardCountSummary(SplitShardCountSummary.fromInt(1));

        // Find IDs that route to shard 0
        List<String> shard0Ids = findIdsForShard(indexRouting, 0, 3);
        for (int i = 0; i < shard0Ids.size(); i++) {
            originalRequest.add(i, new MultiGetRequest.Item(indexName, shard0Ids.get(i)));
        }

        AtomicBoolean executeCalled = new AtomicBoolean(false);
        AtomicReference<MultiGetShardRequest> executedRequest = new AtomicReference<>();

        MultiGetShardSplitHelper splitHelper = new MultiGetShardSplitHelper(
            logger,
            projectMetadata,
            (request, listener) -> {
                executeCalled.set(true);
                executedRequest.set(request);
                MultiGetShardResponse response = new MultiGetShardResponse();
                for (int i = 0; i < request.items.size(); i++) {
                    response.add(
                        request.locations.get(i),
                        new GetResponse(new GetResult(indexName, request.items.get(i).id(), 0, 1, 1, true, null, null, null))
                    );
                }
                listener.onResponse(response);
            }
        );

        PlainActionFuture<MultiGetShardResponse> future = new PlainActionFuture<>();
        splitHelper.coordinateSplitRequest(originalRequest, sourceShardId, indexMetadata, future);

        MultiGetShardResponse response = future.get();
        assertNotNull(response);
        assertEquals(3, response.responses.size());

        assertTrue("Execute should have been called", executeCalled.get());
        assertNotNull("Request should have been executed", executedRequest.get());
    }

    public void testCoordinateSplitRequestSplitBetweenShards() throws Exception {
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        var settings = indexSettings(IndexVersionUtils.randomCompatibleVersion(), 2, 0).build();
        IndexMetadata indexMetadata = IndexMetadata.builder(indexName).settings(settings).build();
        var projectMetadata = ProjectMetadata.builder(randomProjectIdOrDefault()).put(indexMetadata, randomBoolean()).build();

        ShardId sourceShardId = new ShardId(indexMetadata.getIndex(), 0);
        IndexRouting indexRouting = IndexRouting.fromIndexMetadata(indexMetadata);

        // Find IDs for each shard
        List<String> shard0Ids = findIdsForShard(indexRouting, 0, 2);
        List<String> shard1Ids = findIdsForShard(indexRouting, 1, 2);

        MultiGetShardRequest originalRequest = createMultiGetShardRequest(indexName, sourceShardId.getId());
        originalRequest.setSplitShardCountSummary(SplitShardCountSummary.fromInt(1));

        // Mix of items routing to both shards
        originalRequest.add(0, new MultiGetRequest.Item(indexName, shard0Ids.get(0)));
        originalRequest.add(1, new MultiGetRequest.Item(indexName, shard1Ids.get(0)));
        originalRequest.add(2, new MultiGetRequest.Item(indexName, shard0Ids.get(1)));
        originalRequest.add(3, new MultiGetRequest.Item(indexName, shard1Ids.get(1)));

        Map<ShardId, List<MultiGetShardRequest>> executedRequests = new ConcurrentHashMap<>();
        AtomicInteger executeCount = new AtomicInteger(0);

        MultiGetShardSplitHelper splitHelper = new MultiGetShardSplitHelper(
            logger,
            projectMetadata,
            (request, listener) -> {
                executeCount.incrementAndGet();
                IndexMetadata reqIndexMetadata = projectMetadata.index(request.index());
                ShardId shardId = new ShardId(reqIndexMetadata.getIndex(), request.shardId());
                executedRequests.computeIfAbsent(shardId, k -> new ArrayList<>()).add(request);

                MultiGetShardResponse response = new MultiGetShardResponse();
                for (int i = 0; i < request.items.size(); i++) {
                    response.add(
                        request.locations.get(i),
                        new GetResponse(new GetResult(indexName, request.items.get(i).id(), 0, 1, 1, true, null, null, null))
                    );
                }
                listener.onResponse(response);
            }
        );

        PlainActionFuture<MultiGetShardResponse> future = new PlainActionFuture<>();
        splitHelper.coordinateSplitRequest(originalRequest, sourceShardId, indexMetadata, future);

        MultiGetShardResponse response = future.get();
        assertNotNull(response);
        assertEquals(4, response.responses.size());

        // Should have executed on both shards
        assertEquals(2, executeCount.get());
        assertEquals(2, executedRequests.size());
        assertTrue(executedRequests.containsKey(new ShardId(indexMetadata.getIndex(), 0)));
        assertTrue(executedRequests.containsKey(new ShardId(indexMetadata.getIndex(), 1)));
    }

    public void testCoordinateSplitRequestMultipleSplitsBehindFails() {
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        var settings = indexSettings(IndexVersionUtils.randomCompatibleVersion(), 8, 0).build();
        IndexMetadata indexMetadata = IndexMetadata.builder(indexName).settings(settings).build();
        var projectMetadata = ProjectMetadata.builder(randomProjectIdOrDefault()).put(indexMetadata, randomBoolean()).build();

        ShardId sourceShardId = new ShardId(indexMetadata.getIndex(), 0);

        // Request has count of 2, but current is 8 (two splits behind: 2 -> 4 -> 8)
        MultiGetShardRequest originalRequest = createMultiGetShardRequest(indexName, sourceShardId.getId());
        originalRequest.setSplitShardCountSummary(SplitShardCountSummary.fromInt(2));
        originalRequest.add(0, new MultiGetRequest.Item(indexName, "id1"));

        MultiGetShardSplitHelper splitHelper = new MultiGetShardSplitHelper(
            logger,
            projectMetadata,
            (request, listener) -> fail("Should not execute when multiple splits behind")
        );

        PlainActionFuture<MultiGetShardResponse> future = new PlainActionFuture<>();
        splitHelper.coordinateSplitRequest(originalRequest, sourceShardId, indexMetadata, future);

        Exception e = expectThrows(Exception.class, future::actionGet);
        assertThat(e.getMessage(), containsString("Multi-get request is too stale"));
        assertThat(e.getMessage(), containsString("shard count 2"));
        assertThat(e.getMessage(), containsString("current shard count is 8"));
    }

    public void testCoordinateSplitRequestHandlesFailures() throws Exception {
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        var settings = indexSettings(IndexVersionUtils.randomCompatibleVersion(), 2, 0).build();
        IndexMetadata indexMetadata = IndexMetadata.builder(indexName).settings(settings).build();
        var projectMetadata = ProjectMetadata.builder(randomProjectIdOrDefault()).put(indexMetadata, randomBoolean()).build();

        ShardId sourceShardId = new ShardId(indexMetadata.getIndex(), 0);
        ShardId targetShardId = new ShardId(indexMetadata.getIndex(), 1);
        IndexRouting indexRouting = IndexRouting.fromIndexMetadata(indexMetadata);

        // Find IDs for each shard
        List<String> shard0Ids = findIdsForShard(indexRouting, 0, 1);
        List<String> shard1Ids = findIdsForShard(indexRouting, 1, 1);

        MultiGetShardRequest originalRequest = createMultiGetShardRequest(indexName, sourceShardId.getId());
        originalRequest.setSplitShardCountSummary(SplitShardCountSummary.fromInt(1));
        originalRequest.add(0, new MultiGetRequest.Item(indexName, shard0Ids.get(0)));
        originalRequest.add(1, new MultiGetRequest.Item(indexName, shard1Ids.get(0)));

        Exception targetShardException = new RuntimeException("Target shard failed");

        MultiGetShardSplitHelper splitHelper = new MultiGetShardSplitHelper(
            logger,
            projectMetadata,
            (request, listener) -> {
                IndexMetadata reqIndexMetadata = projectMetadata.index(request.index());
                ShardId shardId = new ShardId(reqIndexMetadata.getIndex(), request.shardId());
                if (shardId.equals(sourceShardId)) {
                    // Source shard succeeds
                    MultiGetShardResponse response = new MultiGetShardResponse();
                    response.add(0, new GetResponse(new GetResult(indexName, shard0Ids.get(0), 0, 1, 1, true, null, null, null)));
                    listener.onResponse(response);
                } else {
                    // Target shard fails
                    listener.onFailure(targetShardException);
                }
            }
        );

        PlainActionFuture<MultiGetShardResponse> future = new PlainActionFuture<>();
        splitHelper.coordinateSplitRequest(originalRequest, sourceShardId, indexMetadata, future);

        MultiGetShardResponse response = future.get();
        assertNotNull(response);
        assertEquals(2, response.responses.size());

        // First item should succeed (from source shard)
        assertNotNull(response.responses.get(0));
        assertNull(response.failures.get(0));

        // Second item should fail (from target shard)
        assertNull(response.responses.get(1));
        assertNotNull(response.failures.get(1));
        assertEquals(targetShardException, response.failures.get(1).getFailure());
    }

    // ========================================
    // Tests for splitRequests
    // ========================================

    public void testSplitRequestsAllItemsToSource() {
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        var settings = indexSettings(IndexVersionUtils.randomCompatibleVersion(), 2, 0).build();
        IndexMetadata indexMetadata = IndexMetadata.builder(indexName).settings(settings).build();
        var projectMetadata = ProjectMetadata.builder(randomProjectIdOrDefault()).put(indexMetadata, randomBoolean()).build();

        ShardId sourceShardId = new ShardId(indexMetadata.getIndex(), 0);
        IndexRouting indexRouting = IndexRouting.fromIndexMetadata(indexMetadata);

        MultiGetShardRequest originalRequest = createMultiGetShardRequest(indexName, sourceShardId.getId(), 3);
        // Find IDs that route to shard 0
        List<String> shard0Ids = findIdsForShard(indexRouting, 0, 3);
        for (int i = 0; i < shard0Ids.size(); i++) {
            originalRequest.add(i, new MultiGetRequest.Item(indexName, shard0Ids.get(i)));
        }

        Map<ShardId, MultiGetShardRequest> splitRequests = MultiGetShardSplitHelper.splitRequests(originalRequest, projectMetadata);

        // Should return a single request for the source shard
        assertEquals(1, splitRequests.size());
        assertTrue(splitRequests.containsKey(sourceShardId));

        MultiGetShardRequest sourceRequest = splitRequests.get(sourceShardId);
        // Should be the original request since all items route to source
        assertSame(originalRequest, sourceRequest);
        assertEquals(3, sourceRequest.items.size());
    }

    public void testSplitRequestsAllItemsToTarget() {
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        var settings = indexSettings(IndexVersionUtils.randomCompatibleVersion(), 2, 0).build();
        IndexMetadata indexMetadata = IndexMetadata.builder(indexName).settings(settings).build();
        var projectMetadata = ProjectMetadata.builder(randomProjectIdOrDefault()).put(indexMetadata, randomBoolean()).build();

        ShardId sourceShardId = new ShardId(indexMetadata.getIndex(), 0);
        ShardId targetShardId = new ShardId(indexMetadata.getIndex(), 1);
        IndexRouting indexRouting = IndexRouting.fromIndexMetadata(indexMetadata);

        MultiGetShardRequest originalRequest = createMultiGetShardRequest(indexName, sourceShardId.getId(), 3);
        // Find IDs that route to shard 1
        List<String> shard1Ids = findIdsForShard(indexRouting, 1, 3);
        for (int i = 0; i < shard1Ids.size(); i++) {
            originalRequest.add(i, new MultiGetRequest.Item(indexName, shard1Ids.get(i)));
        }

        Map<ShardId, MultiGetShardRequest> splitRequests = MultiGetShardSplitHelper.splitRequests(originalRequest, projectMetadata);

        // Should return a single request for the target shard
        assertEquals(1, splitRequests.size());
        assertTrue(splitRequests.containsKey(targetShardId));

        MultiGetShardRequest targetRequest = splitRequests.get(targetShardId);
        assertEquals(3, targetRequest.items.size());
        assertEquals(3, targetRequest.locations.size());

        // Verify locations are preserved
        assertThat(targetRequest.locations, containsInAnyOrder(0, 1, 2));
    }

    public void testSplitRequestsSplitBetweenShards() {
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        var settings = indexSettings(IndexVersionUtils.randomCompatibleVersion(), 2, 0).build();
        IndexMetadata indexMetadata = IndexMetadata.builder(indexName).settings(settings).build();
        var projectMetadata = ProjectMetadata.builder(randomProjectIdOrDefault()).put(indexMetadata, randomBoolean()).build();

        ShardId sourceShardId = new ShardId(indexMetadata.getIndex(), 0);
        ShardId targetShardId = new ShardId(indexMetadata.getIndex(), 1);
        IndexRouting indexRouting = IndexRouting.fromIndexMetadata(indexMetadata);

        // Find IDs for each shard
        List<String> shard0Ids = findIdsForShard(indexRouting, 0, 2);
        List<String> shard1Ids = findIdsForShard(indexRouting, 1, 2);

        MultiGetShardRequest originalRequest = createMultiGetShardRequest(indexName, sourceShardId.getId(), 4);
        // Mix of items routing to both shards
        originalRequest.add(0, new MultiGetRequest.Item(indexName, shard0Ids.get(0)));
        originalRequest.add(1, new MultiGetRequest.Item(indexName, shard1Ids.get(0)));
        originalRequest.add(2, new MultiGetRequest.Item(indexName, shard0Ids.get(1)));
        originalRequest.add(3, new MultiGetRequest.Item(indexName, shard1Ids.get(1)));

        Map<ShardId, MultiGetShardRequest> splitRequests = MultiGetShardSplitHelper.splitRequests(originalRequest, projectMetadata);

        // Should return two requests
        assertEquals(2, splitRequests.size());
        assertTrue(splitRequests.containsKey(sourceShardId));
        assertTrue(splitRequests.containsKey(targetShardId));

        MultiGetShardRequest sourceRequest = splitRequests.get(sourceShardId);
        MultiGetShardRequest targetRequest = splitRequests.get(targetShardId);

        // Verify source shard request
        assertThat(sourceRequest.items, hasSize(2));
        assertThat(sourceRequest.locations, containsInAnyOrder(0, 2));

        // Verify target shard request
        assertThat(targetRequest.items, hasSize(2));
        assertThat(targetRequest.locations, containsInAnyOrder(1, 3));

        // Verify split shard count summary is set correctly on new requests
        SplitShardCountSummary expectedSummary = SplitShardCountSummary.forSearch(indexMetadata, sourceShardId.getId());
        assertEquals(expectedSummary, sourceRequest.getSplitShardCountSummary());
        assertEquals(expectedSummary, targetRequest.getSplitShardCountSummary());
    }

    public void testSplitRequestsEmptyItems() {
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        var settings = indexSettings(IndexVersionUtils.randomCompatibleVersion(), 2, 0).build();
        IndexMetadata indexMetadata = IndexMetadata.builder(indexName).settings(settings).build();
        var projectMetadata = ProjectMetadata.builder(randomProjectIdOrDefault()).put(indexMetadata, randomBoolean()).build();

        ShardId sourceShardId = new ShardId(indexMetadata.getIndex(), 0);

        MultiGetShardRequest originalRequest = createMultiGetShardRequest(indexName, sourceShardId.getId(), 0);

        Map<ShardId, MultiGetShardRequest> splitRequests = MultiGetShardSplitHelper.splitRequests(originalRequest, projectMetadata);

        // Should return the original request
        assertEquals(1, splitRequests.size());
        assertTrue(splitRequests.containsKey(sourceShardId));
        assertSame(originalRequest, splitRequests.get(sourceShardId));
    }

    // ========================================
    // Tests for combineResponses
    // ========================================

    public void testCombineResponsesAllSuccessful() {
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        var settings = indexSettings(IndexVersionUtils.randomCompatibleVersion(), 2, 0).build();
        IndexMetadata indexMetadata = IndexMetadata.builder(indexName).settings(settings).build();

        ShardId sourceShardId = new ShardId(indexMetadata.getIndex(), 0);
        ShardId targetShardId = new ShardId(indexMetadata.getIndex(), 1);

        // Create original request
        MultiGetShardRequest originalRequest = createMultiGetShardRequest(indexName, sourceShardId.getId(), 4);
        originalRequest.add(0, new MultiGetRequest.Item(indexName, "0"));
        originalRequest.add(1, new MultiGetRequest.Item(indexName, "1"));
        originalRequest.add(2, new MultiGetRequest.Item(indexName, "2"));
        originalRequest.add(3, new MultiGetRequest.Item(indexName, "3"));

        // Create split requests
        Map<ShardId, MultiGetShardRequest> splitRequests = new HashMap<>();
        MultiGetShardRequest sourceRequest = createMultiGetShardRequest(indexName, sourceShardId.getId(), 2);
        sourceRequest.add(0, new MultiGetRequest.Item(indexName, "0"));
        sourceRequest.add(2, new MultiGetRequest.Item(indexName, "2"));
        splitRequests.put(sourceShardId, sourceRequest);

        MultiGetShardRequest targetRequest = createMultiGetShardRequest(indexName, targetShardId.getId(), 2);
        targetRequest.add(1, new MultiGetRequest.Item(indexName, "1"));
        targetRequest.add(3, new MultiGetRequest.Item(indexName, "3"));
        splitRequests.put(targetShardId, targetRequest);

        // Create responses
        Map<ShardId, Tuple<MultiGetShardResponse, Exception>> responses = new HashMap<>();

        MultiGetShardResponse sourceResponse = new MultiGetShardResponse();
        sourceResponse.add(0, new GetResponse(new GetResult(indexName, "0", 0, 1, 1, true, null, null, null)));
        sourceResponse.add(2, new GetResponse(new GetResult(indexName, "2", 0, 1, 1, true, null, null, null)));
        responses.put(sourceShardId, new Tuple<>(sourceResponse, null));

        MultiGetShardResponse targetResponse = new MultiGetShardResponse();
        targetResponse.add(1, new GetResponse(new GetResult(indexName, "1", 0, 1, 1, true, null, null, null)));
        targetResponse.add(3, new GetResponse(new GetResult(indexName, "3", 0, 1, 1, true, null, null, null)));
        responses.put(targetShardId, new Tuple<>(targetResponse, null));

        // Combine responses
        Tuple<MultiGetShardResponse, Exception> result = MultiGetShardSplitHelper.combineResponses(
            originalRequest,
            splitRequests,
            responses
        );

        assertNotNull(result.v1());
        assertNull(result.v2());

        MultiGetShardResponse combinedResponse = result.v1();
        assertEquals(4, combinedResponse.locations.size());
        assertEquals(4, combinedResponse.responses.size());
        assertEquals(4, combinedResponse.failures.size());

        // Verify responses are in the correct order
        assertThat(combinedResponse.locations, equalTo(List.of(0, 1, 2, 3)));
        assertNotNull(combinedResponse.responses.get(0));
        assertEquals("0", combinedResponse.responses.get(0).getId());
        assertNotNull(combinedResponse.responses.get(1));
        assertEquals("1", combinedResponse.responses.get(1).getId());
        assertNotNull(combinedResponse.responses.get(2));
        assertEquals("2", combinedResponse.responses.get(2).getId());
        assertNotNull(combinedResponse.responses.get(3));
        assertEquals("3", combinedResponse.responses.get(3).getId());

        // Verify no failures
        for (int i = 0; i < 4; i++) {
            assertNull(combinedResponse.failures.get(i));
        }
    }

    public void testCombineResponsesWithFailures() {
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        var settings = indexSettings(IndexVersionUtils.randomCompatibleVersion(), 2, 0).build();
        IndexMetadata indexMetadata = IndexMetadata.builder(indexName).settings(settings).build();

        ShardId sourceShardId = new ShardId(indexMetadata.getIndex(), 0);
        ShardId targetShardId = new ShardId(indexMetadata.getIndex(), 1);

        // Create original request
        MultiGetShardRequest originalRequest = createMultiGetShardRequest(indexName, sourceShardId.getId(), 4);
        originalRequest.add(0, new MultiGetRequest.Item(indexName, "0"));
        originalRequest.add(1, new MultiGetRequest.Item(indexName, "1"));
        originalRequest.add(2, new MultiGetRequest.Item(indexName, "2"));
        originalRequest.add(3, new MultiGetRequest.Item(indexName, "3"));

        // Create split requests
        Map<ShardId, MultiGetShardRequest> splitRequests = new HashMap<>();
        MultiGetShardRequest sourceRequest = createMultiGetShardRequest(indexName, sourceShardId.getId(), 2);
        sourceRequest.add(0, new MultiGetRequest.Item(indexName, "0"));
        sourceRequest.add(2, new MultiGetRequest.Item(indexName, "2"));
        splitRequests.put(sourceShardId, sourceRequest);

        MultiGetShardRequest targetRequest = createMultiGetShardRequest(indexName, targetShardId.getId(), 2);
        targetRequest.add(1, new MultiGetRequest.Item(indexName, "1"));
        targetRequest.add(3, new MultiGetRequest.Item(indexName, "3"));
        splitRequests.put(targetShardId, targetRequest);

        // Create responses with target shard failure
        Map<ShardId, Tuple<MultiGetShardResponse, Exception>> responses = new HashMap<>();

        MultiGetShardResponse sourceResponse = new MultiGetShardResponse();
        sourceResponse.add(0, new GetResponse(new GetResult(indexName, "0", 0, 1, 1, true, null, null, null)));
        sourceResponse.add(2, new GetResponse(new GetResult(indexName, "2", 0, 1, 1, true, null, null, null)));
        responses.put(sourceShardId, new Tuple<>(sourceResponse, null));

        Exception targetFailure = new RuntimeException("Target shard failed");
        responses.put(targetShardId, new Tuple<>(null, targetFailure));

        // Combine responses
        Tuple<MultiGetShardResponse, Exception> result = MultiGetShardSplitHelper.combineResponses(
            originalRequest,
            splitRequests,
            responses
        );

        assertNotNull(result.v1());
        assertNull(result.v2());

        MultiGetShardResponse combinedResponse = result.v1();
        assertEquals(4, combinedResponse.locations.size());
        assertEquals(4, combinedResponse.responses.size());
        assertEquals(4, combinedResponse.failures.size());

        // Verify successful responses from source shard
        assertNotNull(combinedResponse.responses.get(0));
        assertEquals("0", combinedResponse.responses.get(0).getId());
        assertNull(combinedResponse.failures.get(0));

        assertNotNull(combinedResponse.responses.get(2));
        assertEquals("2", combinedResponse.responses.get(2).getId());
        assertNull(combinedResponse.failures.get(2));

        // Verify failures from target shard
        assertNull(combinedResponse.responses.get(1));
        assertNotNull(combinedResponse.failures.get(1));
        assertEquals("1", combinedResponse.failures.get(1).getId());
        assertEquals(targetFailure, combinedResponse.failures.get(1).getFailure());

        assertNull(combinedResponse.responses.get(3));
        assertNotNull(combinedResponse.failures.get(3));
        assertEquals("3", combinedResponse.failures.get(3).getId());
        assertEquals(targetFailure, combinedResponse.failures.get(3).getFailure());
    }

    public void testCombineResponsesWithMixedSuccessAndItemFailures() {
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        var settings = indexSettings(IndexVersionUtils.randomCompatibleVersion(), 2, 0).build();
        IndexMetadata indexMetadata = IndexMetadata.builder(indexName).settings(settings).build();

        ShardId sourceShardId = new ShardId(indexMetadata.getIndex(), 0);
        ShardId targetShardId = new ShardId(indexMetadata.getIndex(), 1);

        // Create original request
        MultiGetShardRequest originalRequest = createMultiGetShardRequest(indexName, sourceShardId.getId(), 4);
        originalRequest.add(0, new MultiGetRequest.Item(indexName, "0"));
        originalRequest.add(1, new MultiGetRequest.Item(indexName, "1"));
        originalRequest.add(2, new MultiGetRequest.Item(indexName, "2"));
        originalRequest.add(3, new MultiGetRequest.Item(indexName, "3"));

        // Create split requests
        Map<ShardId, MultiGetShardRequest> splitRequests = new HashMap<>();
        MultiGetShardRequest sourceRequest = createMultiGetShardRequest(indexName, sourceShardId.getId(), 2);
        sourceRequest.add(0, new MultiGetRequest.Item(indexName, "0"));
        sourceRequest.add(2, new MultiGetRequest.Item(indexName, "2"));
        splitRequests.put(sourceShardId, sourceRequest);

        MultiGetShardRequest targetRequest = createMultiGetShardRequest(indexName, targetShardId.getId(), 2);
        targetRequest.add(1, new MultiGetRequest.Item(indexName, "1"));
        targetRequest.add(3, new MultiGetRequest.Item(indexName, "3"));
        splitRequests.put(targetShardId, targetRequest);

        // Create responses with item-level failures
        Map<ShardId, Tuple<MultiGetShardResponse, Exception>> responses = new HashMap<>();

        MultiGetShardResponse sourceResponse = new MultiGetShardResponse();
        sourceResponse.add(0, new GetResponse(new GetResult(indexName, "0", 0, 1, 1, true, null, null, null)));
        sourceResponse.add(2, new MultiGetResponse.Failure(indexName, "2", new RuntimeException("Item 2 not found")));
        responses.put(sourceShardId, new Tuple<>(sourceResponse, null));

        MultiGetShardResponse targetResponse = new MultiGetShardResponse();
        targetResponse.add(1, new MultiGetResponse.Failure(indexName, "1", new RuntimeException("Item 1 not found")));
        targetResponse.add(3, new GetResponse(new GetResult(indexName, "3", 0, 1, 1, true, null, null, null)));
        responses.put(targetShardId, new Tuple<>(targetResponse, null));

        // Combine responses
        Tuple<MultiGetShardResponse, Exception> result = MultiGetShardSplitHelper.combineResponses(
            originalRequest,
            splitRequests,
            responses
        );

        assertNotNull(result.v1());
        assertNull(result.v2());

        MultiGetShardResponse combinedResponse = result.v1();
        assertEquals(4, combinedResponse.locations.size());

        // Verify location 0: success
        assertNotNull(combinedResponse.responses.get(0));
        assertEquals("0", combinedResponse.responses.get(0).getId());
        assertNull(combinedResponse.failures.get(0));

        // Verify location 1: item failure
        assertNull(combinedResponse.responses.get(1));
        assertNotNull(combinedResponse.failures.get(1));
        assertEquals("1", combinedResponse.failures.get(1).getId());

        // Verify location 2: item failure
        assertNull(combinedResponse.responses.get(2));
        assertNotNull(combinedResponse.failures.get(2));
        assertEquals("2", combinedResponse.failures.get(2).getId());

        // Verify location 3: success
        assertNotNull(combinedResponse.responses.get(3));
        assertEquals("3", combinedResponse.responses.get(3).getId());
        assertNull(combinedResponse.failures.get(3));
    }

    // ========================================
    // Helper methods
    // ========================================

    private MultiGetShardRequest createMultiGetShardRequest(String indexName, int shardId) {
        MultiGetRequest baseRequest = new MultiGetRequest();
        MultiGetShardRequest request = new MultiGetShardRequest(baseRequest, indexName, shardId);
        request.locations = new ArrayList<>();
        request.items = new ArrayList<>();
        return request;
    }

    private MultiGetShardRequest createMultiGetShardRequest(String indexName, int shardId, int expectedSize) {
        MultiGetRequest baseRequest = new MultiGetRequest();
        MultiGetShardRequest request = new MultiGetShardRequest(baseRequest, indexName, shardId);
        request.locations = new ArrayList<>(expectedSize);
        request.items = new ArrayList<>(expectedSize);
        return request;
    }

    /**
     * Helper method to find document IDs that route to a specific shard
     */
    private List<String> findIdsForShard(IndexRouting indexRouting, int targetShard, int count) {
        List<String> ids = new ArrayList<>();
        int attempt = 0;
        while (ids.size() < count && attempt < 10000) {
            String id = randomAlphaOfLength(10) + attempt;
            if (indexRouting.getShard(id, null) == targetShard) {
                ids.add(id);
            }
            attempt++;
        }
        if (ids.size() < count) {
            throw new IllegalStateException("Could not find enough IDs for shard " + targetShard);
        }
        return ids;
    }
}