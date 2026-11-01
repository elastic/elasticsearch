/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.bulk;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.TransportShardBulkActionTests.FakeDeleteResult;
import org.elasticsearch.action.bulk.TransportShardBulkActionTests.FakeIndexResult;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.SplitShardCountSummary;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class BulkPrimaryExecutionContextTests extends ESTestCase {

    public void testAbortedSkipped() {
        BulkShardRequest shardRequest = generateRandomRequest();

        ArrayList<DocWriteRequest<?>> nonAbortedRequests = new ArrayList<>();
        for (BulkItemRequest request : shardRequest.items()) {
            if (randomBoolean()) {
                request.abort("index", new ElasticsearchException("bla"));
            } else {
                nonAbortedRequests.add(request.request());
            }
        }

        final IndexShard primary = mock(IndexShard.class);
        when(primary.shardId()).thenReturn(shardRequest.shardId());
        ShardRouting shardRouting = newShardRouting(shardRequest.shardId(), ShardRouting.Role.DEFAULT);
        when(primary.routingEntry()).thenReturn(shardRouting);

        ArrayList<DocWriteRequest<?>> visitedRequests = new ArrayList<>();
        for (BulkPrimaryExecutionContext context = new BulkPrimaryExecutionContext(shardRequest, primary); context
            .hasMoreOperationsToExecute();) {
            visitedRequests.add(context.getCurrent());
            context.setRequestToExecute(context.getCurrent());
            // using failures prevents caring about types
            context.markOperationAsExecuted(
                new Engine.IndexResult(new ElasticsearchException("bla"), 1, context.getRequestToExecute().id())
            );
            context.markAsCompleted(context.getExecutionResult());
        }

        assertThat(visitedRequests, equalTo(nonAbortedRequests));
    }

    /**
     * The look-ahead is only allowed once an operation of the request waited for a mapping update, on an index that enables it.
     * It goes through the index requests that follow the current operation, skips the aborted ones and ends at the first
     * operation that is rejected or that is not an index request. The operations it went through don't start a new look-ahead.
     */
    public void testLookAheadForMappingUpdates() {
        BulkItemRequest[] items = new BulkItemRequest[] {
            new BulkItemRequest(0, new IndexRequest("index").id("0")),
            new BulkItemRequest(1, new IndexRequest("index").id("1")),
            new BulkItemRequest(2, new IndexRequest("index").id("2")),
            new BulkItemRequest(3, new IndexRequest("index").id("3")),
            new BulkItemRequest(4, new DeleteRequest("index", "4")),
            new BulkItemRequest(5, new IndexRequest("index").id("5")),
            new BulkItemRequest(6, new IndexRequest("index").id("6")) };
        items[2].abort("index", new ElasticsearchException("aborted"));
        BulkShardRequest shardRequest = new BulkShardRequest(
            new ShardId("index", "_na_", 0),
            SplitShardCountSummary.IRRELEVANT,
            WriteRequest.RefreshPolicy.NONE,
            items
        );
        boolean enabled = randomBoolean();
        final IndexShard primary = mock(IndexShard.class);
        when(primary.shardId()).thenReturn(shardRequest.shardId());
        IndexMetadata indexMetadata = IndexMetadata.builder("index")
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current())
                    .put(MapperService.INDEX_MAPPING_COMBINE_DYNAMIC_UPDATES_SETTING.getKey(), enabled)
            )
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();
        when(primary.indexSettings()).thenReturn(new IndexSettings(indexMetadata, Settings.EMPTY));
        ShardRouting shardRouting = newShardRouting(shardRequest.shardId(), ShardRouting.Role.DEFAULT);
        when(primary.routingEntry()).thenReturn(shardRouting);

        BulkPrimaryExecutionContext context = new BulkPrimaryExecutionContext(shardRequest, primary);
        assertFalse(context.shouldLookAheadForMappingUpdates());
        // operation 0 waits for a mapping update and is executed
        context.setRequestToExecute(context.getCurrent());
        context.markAsRequiringMappingUpdate();
        context.resetForMappingUpdateRetry();
        completeCurrentOperation(context);

        // operation 1
        assertThat(context.shouldLookAheadForMappingUpdates(), equalTo(enabled));
        if (enabled == false) {
            return;
        }
        List<String> offered = new ArrayList<>();
        context.lookAheadForMappingUpdates(request -> offered.add(request.id()));
        assertThat(offered, equalTo(List.of("3")));
        assertFalse(context.shouldLookAheadForMappingUpdates());
        completeCurrentOperation(context);

        // operation 3, the look-ahead went through it
        assertThat(context.getCurrent().id(), equalTo("3"));
        assertFalse(context.shouldLookAheadForMappingUpdates());
        completeCurrentOperation(context);

        // operation 4, the look-ahead ended on it
        assertTrue(context.shouldLookAheadForMappingUpdates());
        offered.clear();
        context.lookAheadForMappingUpdates(request -> {
            offered.add(request.id());
            return false;
        });
        assertThat(offered, equalTo(List.of("5")));
        completeCurrentOperation(context);

        // operation 5, the look-ahead rejected it
        assertTrue(context.shouldLookAheadForMappingUpdates());
        offered.clear();
        context.lookAheadForMappingUpdates(request -> offered.add(request.id()));
        assertThat(offered, equalTo(List.of("6")));
        completeCurrentOperation(context);

        // operation 6
        assertFalse(context.shouldLookAheadForMappingUpdates());
    }

    private static void completeCurrentOperation(BulkPrimaryExecutionContext context) {
        context.setRequestToExecute(context.getCurrent());
        context.markOperationAsExecuted(
            new Engine.IndexResult(new ElasticsearchException("failed"), 1, context.getRequestToExecute().id())
        );
        context.markAsCompleted(context.getExecutionResult());
    }

    private BulkShardRequest generateRandomRequest() {
        BulkItemRequest[] items = new BulkItemRequest[randomInt(20)];
        for (int i = 0; i < items.length; i++) {
            final DocWriteRequest<?> request = switch (randomFrom(DocWriteRequest.OpType.values())) {
                case INDEX -> new IndexRequest("index").id("id_" + i);
                case CREATE -> new IndexRequest("index").id("id_" + i).create(true);
                case UPDATE -> new UpdateRequest("index", "id_" + i);
                case DELETE -> new DeleteRequest("index", "id_" + i);
            };
            items[i] = new BulkItemRequest(i, request);
        }
        return new BulkShardRequest(
            new ShardId("index", "_na_", 0),
            SplitShardCountSummary.fromInt(randomInt(1024)),
            randomFrom(WriteRequest.RefreshPolicy.values()),
            items
        );
    }

    public void testTranslogLocation() {

        BulkShardRequest shardRequest = generateRandomRequest();

        Translog.Location expectedLocation = null;
        final IndexShard primary = mock(IndexShard.class);
        when(primary.shardId()).thenReturn(shardRequest.shardId());
        IndexMetadata indexMetadata = IndexMetadata.builder("index")
            .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current()))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();
        when(primary.indexSettings()).thenReturn(new IndexSettings(indexMetadata, Settings.EMPTY));
        ShardRouting shardRouting = newShardRouting(shardRequest.shardId(), ShardRouting.Role.DEFAULT);
        when(primary.routingEntry()).thenReturn(shardRouting);

        long translogGen = 0;
        long translogOffset = 0;

        BulkPrimaryExecutionContext context = new BulkPrimaryExecutionContext(shardRequest, primary);
        while (context.hasMoreOperationsToExecute()) {
            final Engine.Result result;
            final DocWriteRequest<?> current = context.getCurrent();
            final boolean failure = rarely();
            if (frequently()) {
                translogGen += randomIntBetween(1, 4);
                translogOffset = 0;
            } else {
                translogOffset += randomIntBetween(200, 400);
            }

            Translog.Location location = new Translog.Location(translogGen, translogOffset, randomInt(200));

            final boolean noOpResult = failure && randomBoolean();
            switch (current.opType()) {
                case INDEX, CREATE -> {
                    context.setRequestToExecute(current);
                    if (failure) {
                        result = noOpResult
                            ? new MockNoOpResult(new ElasticsearchException("bla"), 1, 1, location)
                            : new Engine.IndexResult(new ElasticsearchException("bla"), 1, current.id());
                    } else {
                        result = new FakeIndexResult(1, 1, randomLongBetween(0, 200), randomBoolean(), location, "id");
                    }
                }
                case UPDATE -> {
                    context.setRequestToExecute(new IndexRequest(current.index()).id(current.id()));
                    if (failure) {
                        result = noOpResult
                            ? new MockNoOpResult(new ElasticsearchException("bla"), 1, 1, location)
                            : new Engine.IndexResult(new ElasticsearchException("bla"), 1, 1, 1, current.id());
                    } else {
                        result = new FakeIndexResult(1, 1, randomLongBetween(0, 200), randomBoolean(), location, "id");
                    }
                }
                case DELETE -> {
                    context.setRequestToExecute(current);
                    if (failure) {
                        result = new Engine.DeleteResult(new ElasticsearchException("bla"), 1, 1, current.id());
                    } else {
                        result = new FakeDeleteResult(1, 1, randomLongBetween(0, 200), randomBoolean(), location, current.id());
                    }
                }
                default -> throw new AssertionError("unknown type:" + current.opType());
            }
            if (failure == false || (noOpResult && current.opType() != DocWriteRequest.OpType.DELETE)) {
                expectedLocation = location;
            }
            context.markOperationAsExecuted(result);
            context.markAsCompleted(context.getExecutionResult());
        }

        assertThat(context.getLocationToSync(), equalTo(expectedLocation));
    }

    private static class MockNoOpResult extends Engine.NoOpResult {

        private final Translog.Location location;

        MockNoOpResult(Exception failure, long term, long seqNo, Translog.Location location) {
            super(term, seqNo, failure);
            this.location = location;
        }

        @Override
        public Translog.Location getTranslogLocation() {
            return location;
        }
    }

    private ShardRouting newShardRouting(ShardId shardId, ShardRouting.Role role) {
        final UnassignedInfo unassignedInfo = new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "_message");
        return ShardRouting.newUnassigned(
            shardId,
            true,
            RecoverySource.ExistingStoreRecoverySource.INSTANCE,
            unassignedInfo,
            role,
            ShardRouting.RecoveryPriority.UNASSIGNED_NEW_PRIMARY
        );
    }
}
