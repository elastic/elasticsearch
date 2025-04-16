/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ccr.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionTestUtils;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardNotStartedException;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardNotFoundException;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.xcontent.XContentType;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasToString;
import static org.hamcrest.Matchers.instanceOf;

public class ShardChangesActionTests extends ESSingleNodeTestCase {

    @Override
    protected boolean resetNodeAfterTest() {
        return true;
    }

    public void testGetOperations() throws Exception {
        final IndexService indexService = createIndex("index", indexSettings(1, 0).build());

        final int numWrites = randomIntBetween(10, 4096);
        for (int i = 0; i < numWrites; i++) {
            prepareIndex("index").setId(Integer.toString(i)).setSource("{}", XContentType.JSON).get();
        }

        // A number of times, get operations within a range that exists:
        int iters = randomIntBetween(8, 32);
        IndexShard indexShard = indexService.getShard(0);
        for (int iter = 0; iter < iters; iter++) {
            int min = randomIntBetween(0, numWrites - 1);
            int max = randomIntBetween(min, numWrites - 1);
            int size = max - min + 1;
            final Translog.Operation[] operations = ShardChangesAction.getOperations(
                indexShard,
                indexShard.getLastKnownGlobalCheckpoint(),
                min,
                size,
                indexShard.getHistoryUUID(),
                ByteSizeValue.of(Long.MAX_VALUE, ByteSizeUnit.BYTES)
            );
            final List<Long> seenSeqNos = Arrays.stream(operations).map(Translog.Operation::seqNo).collect(Collectors.toList());
            final List<Long> expectedSeqNos = LongStream.rangeClosed(min, max).boxed().collect(Collectors.toList());
            assertThat(seenSeqNos, equalTo(expectedSeqNos));
        }

        {
            // get operations for a range for which no operations exist
            final IllegalStateException e = expectThrows(
                IllegalStateException.class,
                () -> ShardChangesAction.getOperations(
                    indexShard,
                    indexShard.getLastKnownGlobalCheckpoint(),
                    numWrites,
                    numWrites + 1,
                    indexShard.getHistoryUUID(),
                    ByteSizeValue.of(Long.MAX_VALUE, ByteSizeUnit.BYTES)
                )
            );
            final String message = String.format(
                Locale.ROOT,
                "not exposing operations from [%d] greater than the global checkpoint [%d]",
                numWrites,
                indexShard.getLastKnownGlobalCheckpoint()
            );
            assertThat(e, hasToString(containsString(message)));
        }

        // get operations for a range some operations do not exist:
        Translog.Operation[] operations = ShardChangesAction.getOperations(
            indexShard,
            indexShard.getLastKnownGlobalCheckpoint(),
            numWrites - 10,
            numWrites + 10,
            indexShard.getHistoryUUID(),
            ByteSizeValue.of(Long.MAX_VALUE, ByteSizeUnit.BYTES)
        );
        assertThat(operations.length, equalTo(10));

        // Unexpected history UUID:
        Exception e = expectThrows(
            IllegalStateException.class,
            () -> ShardChangesAction.getOperations(
                indexShard,
                indexShard.getLastKnownGlobalCheckpoint(),
                0,
                10,
                "different-history-uuid",
                ByteSizeValue.of(Long.MAX_VALUE, ByteSizeUnit.BYTES)
            )
        );
        assertThat(
            e.getMessage(),
            equalTo("unexpected history uuid, expected [different-history-uuid], actual [" + indexShard.getHistoryUUID() + "]")
        );

        // invalid range
        {
            final long fromSeqNo = randomLongBetween(Long.MIN_VALUE, -1);
            final int batchSize = randomIntBetween(0, Integer.MAX_VALUE);
            final IllegalArgumentException invalidRangeError = expectThrows(
                IllegalArgumentException.class,
                () -> ShardChangesAction.getOperations(
                    indexShard,
                    indexShard.getLastKnownGlobalCheckpoint(),
                    fromSeqNo,
                    batchSize,
                    indexShard.getHistoryUUID(),
                    ByteSizeValue.of(Long.MAX_VALUE, ByteSizeUnit.BYTES)
                )
            );
            assertThat(
                invalidRangeError.getMessage(),
                equalTo("Invalid range; from_seqno [" + fromSeqNo + "], to_seqno [" + (fromSeqNo + batchSize - 1) + "]")
            );
        }
    }

    public void testGetOperationsWhenShardNotStarted() throws Exception {
        IndexShard indexShard = Mockito.mock(IndexShard.class);

        ShardRouting shardRouting = TestShardRouting.newShardRouting("index", 0, "_node_id", true, ShardRoutingState.INITIALIZING);
        Mockito.when(indexShard.routingEntry()).thenReturn(shardRouting);
        expectThrows(
            IndexShardNotStartedException.class,
            () -> ShardChangesAction.getOperations(
                indexShard,
                indexShard.getLastKnownGlobalCheckpoint(),
                0,
                1,
                indexShard.getHistoryUUID(),
                ByteSizeValue.of(Long.MAX_VALUE, ByteSizeUnit.BYTES)
            )
        );
    }

    public void testGetOperationsExceedByteLimit() throws Exception {
        final IndexService indexService = createIndex("index", indexSettings(1, 0).build());

        final long numWrites = 32;
        for (int i = 0; i < numWrites; i++) {
            prepareIndex("index").setId(Integer.toString(i)).setSource("{}", XContentType.JSON).get();
        }

        final IndexShard indexShard = indexService.getShard(0);
        final Translog.Operation[] operations = ShardChangesAction.getOperations(
            indexShard,
            indexShard.getLastKnownGlobalCheckpoint(),
            0,
            randomIntBetween(100, 500),
            indexShard.getHistoryUUID(),
            ByteSizeValue.of(256, ByteSizeUnit.BYTES)
        );
        assertThat(operations.length, equalTo(8));
        assertThat(operations[0].seqNo(), equalTo(0L));
        assertThat(operations[1].seqNo(), equalTo(1L));
        assertThat(operations[2].seqNo(), equalTo(2L));
        assertThat(operations[3].seqNo(), equalTo(3L));
        assertThat(operations[4].seqNo(), equalTo(4L));
        assertThat(operations[5].seqNo(), equalTo(5L));
        assertThat(operations[6].seqNo(), equalTo(6L));
        assertThat(operations[7].seqNo(), equalTo(7L));
    }

    public void testGetOperationsAlwaysReturnAtLeastOneOp() throws Exception {
        final IndexService indexService = createIndex("index", indexSettings(1, 0).build());

        prepareIndex("index").setId("0").setSource("{}", XContentType.JSON).get();

        final IndexShard indexShard = indexService.getShard(0);
        final Translog.Operation[] operations = ShardChangesAction.getOperations(
            indexShard,
            indexShard.getLastKnownGlobalCheckpoint(),
            0,
            1,
            indexShard.getHistoryUUID(),
            ByteSizeValue.ZERO
        );
        assertThat(operations.length, equalTo(1));
        assertThat(operations[0].seqNo(), equalTo(0L));
    }

    public void testIndexNotFound() throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicReference<Exception> reference = new AtomicReference<>();
        final ShardChangesAction.TransportAction transportAction = node().injector().getInstance(ShardChangesAction.TransportAction.class);
        ActionTestUtils.execute(
            transportAction,
            null,
            new ShardChangesAction.Request(new ShardId(new Index("non-existent", "uuid"), 0), "uuid"),
            new ActionListener<ShardChangesAction.Response>() {
                @Override
                public void onResponse(final ShardChangesAction.Response response) {
                    fail();
                }

                @Override
                public void onFailure(final Exception e) {
                    reference.set(e);
                    latch.countDown();
                }
            }
        );
        latch.await();
        assertNotNull(reference.get());
        assertThat(reference.get(), instanceOf(IndexNotFoundException.class));
    }

    public void testShardNotFound() throws InterruptedException {
        final int numberOfShards = randomIntBetween(1, 5);
        final IndexService indexService = createIndex("index", Settings.builder().put("index.number_of_shards", numberOfShards).build());
        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicReference<Exception> reference = new AtomicReference<>();
        final ShardChangesAction.TransportAction transportAction = node().injector().getInstance(ShardChangesAction.TransportAction.class);
        ActionTestUtils.execute(
            transportAction,
            null,
            new ShardChangesAction.Request(new ShardId(indexService.getMetadata().getIndex(), numberOfShards), "uuid"),
            new ActionListener<ShardChangesAction.Response>() {
                @Override
                public void onResponse(final ShardChangesAction.Response response) {
                    fail();
                }

                @Override
                public void onFailure(final Exception e) {
                    reference.set(e);
                    latch.countDown();
                }
            }
        );
        latch.await();
        assertNotNull(reference.get());
        assertThat(reference.get(), instanceOf(ShardNotFoundException.class));
    }

    public void testShardChangesActionRequestHasDescription() {
        var index = new Index("index", "uuid");
        var shardId = new ShardId(index, 0);

        var description = new ShardChangesAction.Request(shardId, "uuid").getDescription();

        assertThat(description, equalTo("shardId[[index][0]]"));
    }
}
