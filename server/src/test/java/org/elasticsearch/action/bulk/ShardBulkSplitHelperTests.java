/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.bulk;

import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.routing.IndexRouting;
import org.elasticsearch.cluster.routing.SplitShardCountSummary;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

public class ShardBulkSplitHelperTests extends ESTestCase {

    public void testSplitAndCombine() {
        var numShards = randomIntBetween(1, 100);
        var settings = indexSettings(IndexVersion.current(), numShards, randomInt(5)).build();
        var indexMetadata = IndexMetadata.builder(randomAlphaOfLength(10)).settings(settings).build();
        var index = indexMetadata.getIndex();
        var projectMetadata = ProjectMetadata.builder(randomProjectIdOrDefault()).put(indexMetadata, randomBoolean()).build();
        var sourceShardId = new ShardId(index, randomInt(numShards - 1));

        // Item ids must be unique
        var bulkItemRequests = randomUnique(ESTestCase::randomInt, randomIntBetween(1, 1000)).stream()
            .map(i -> new BulkItemRequest(i, new TestWriteDocRequest(index, randomInt(numShards))))
            .toArray(BulkItemRequest[]::new);

        // Ensure that at least one item request goes to the source
        ((TestWriteDocRequest) randomFrom(bulkItemRequests).request()).shardId = sourceShardId;

        var originalRequest = new BulkShardRequest(
            sourceShardId,
            SplitShardCountSummary.fromInt(2),
            WriteRequest.RefreshPolicy.NONE,
            bulkItemRequests
        );

        // Split bulk shard request
        Map<ShardId, BulkShardRequest> splitBulkRequests = ShardBulkSplitHelper.splitRequests(originalRequest, projectMetadata);

        // Check that we got the same set of requests
        // The new bulk item requests are different instances but should have the same ids and doc requests
        Set<Tuple<Integer, TestWriteDocRequest>> itemRequestSet = splitBulkRequests.values()
            .stream()
            .flatMap(req -> Arrays.stream(req.items()))
            .map(item -> new Tuple<>(item.id(), (TestWriteDocRequest) item.request()))
            .collect(Collectors.toSet());
        Set<Tuple<Integer, TestWriteDocRequest>> originalRequestSet = Arrays.stream(bulkItemRequests)
            .map(item -> new Tuple<>(item.id(), (TestWriteDocRequest) item.request()))
            .collect(Collectors.toSet());
        assertEquals(originalRequestSet, itemRequestSet);

        final var shardException = new Exception("failed shard request");
        Map<ShardId, Tuple<BulkShardResponse, Exception>> bulkShardResponses = new HashMap<>();
        Map<Integer, BulkItemResponse> itemResponsesById = new HashMap<>();

        // Fabricate some results for the requests
        splitBulkRequests.forEach((shardId, bulkShardRequest) -> {
            // TODO: Right now ShardBulkSplitHelper#combineResponses expects the bulk shard request for the source shard to always succeed
            // There is a TODO there to handle that
            if (shardId.equals(sourceShardId) == false && randomBoolean()) {
                // Fail shard request
                bulkShardResponses.put(shardId, new Tuple<>(null, shardException));
            } else {
                BulkItemResponse[] responses = Arrays.stream(bulkShardRequest.items()).map(request -> {
                    BulkItemResponse response = BulkItemResponse.success(
                        request.id(),
                        randomFrom(DocWriteRequest.OpType.values()),
                        new IndexResponse(shardId, UUID.randomUUID().toString(), randomInt(), randomLong(), randomLong(), randomBoolean())
                    );
                    itemResponsesById.put(request.id(), response);
                    return response;
                }).toArray(BulkItemResponse[]::new);
                bulkShardResponses.put(shardId, new Tuple<>(new BulkShardResponse(shardId, responses), null));
            }
        });

        // Combine bulk shard responses
        Tuple<BulkShardResponse, Exception> combinedResult = ShardBulkSplitHelper.combineResponses(
            originalRequest,
            splitBulkRequests,
            bulkShardResponses
        );
        assertNull(combinedResult.v2());
        BulkShardResponse bulkShardResponse = combinedResult.v1();

        assertEquals(originalRequest.items().length, bulkShardResponse.getResponses().length);
        for (int i = 0; i < originalRequest.items().length; i++) {
            BulkItemRequest bulkItemRequest = originalRequest.items()[i];
            BulkItemResponse bulkItemResponse = bulkShardResponse.getResponses()[i];
            assertEquals(bulkItemRequest.id(), bulkItemResponse.getItemId());

            var docRequest = (TestWriteDocRequest) bulkItemRequest.request();
            Tuple<BulkShardResponse, Exception> expectedResponse = bulkShardResponses.get(docRequest.shardId);
            if (expectedResponse.v2() != null) {
                assertTrue(bulkItemResponse.isFailed());
                assertSame(bulkItemResponse.getFailure().getCause(), shardException);
            } else {
                assertEquals(itemResponsesById.get(bulkItemRequest.id()), bulkItemResponse);
            }
        }
    }

    private static class TestWriteDocRequest extends IndexRequest {
        ShardId shardId;

        TestWriteDocRequest(Index index, int shardId) {
            super(index.getName());
            this.shardId = new ShardId(index, shardId);
            autoGenerateId();
        }

        @Override
        public int rerouteAtSourceDuringResharding(IndexRouting indexRouting) {
            return shardId.id();
        }
    }
}
