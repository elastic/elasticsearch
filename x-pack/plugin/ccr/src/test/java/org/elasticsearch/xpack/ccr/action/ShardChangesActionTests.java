/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ccr.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardNotStartedException;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardNotFoundException;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class ShardChangesActionTests extends ESSingleNodeTestCase {

    @Override
    protected boolean resetNodeAfterTest() {
        return true;
    }

    public void testGetOperations() throws Exception {
        final Settings settings = Settings.builder()
                .put("index.number_of_shards", 1)
                .put("index.number_of_replicas", 0)
                .build();
        final IndexService indexService = createIndex("index", settings);

        final int numWrites = randomIntBetween(2, 8192);
        for (int i = 0; i < numWrites; i++) {
            client().prepareIndex("index", "doc", Integer.toString(i)).setSource("{}", XContentType.JSON).get();
        }

        // A number of times, get operations within a range that exists:
        int iters = randomIntBetween(8, 32);
        IndexShard indexShard = indexService.getShard(0);
        for (int iter = 0; iter < iters; iter++) {
            int min = randomIntBetween(0, numWrites - 1);
            int max = randomIntBetween(min, numWrites - 1);
            int size = max - min + 1;
            final Translog.Operation[] operations = ShardChangesAction.getOperations(indexShard,
                indexShard.getGlobalCheckpoint(), min, size, Long.MAX_VALUE);
            final List<Long> seenSeqNos = Arrays.stream(operations).map(Translog.Operation::seqNo).collect(Collectors.toList());
            final List<Long> expectedSeqNos = LongStream.rangeClosed(min, max).boxed().collect(Collectors.toList());
            assertThat(seenSeqNos, equalTo(expectedSeqNos));
        }


        // get operations for a range no operations exists:
        Translog.Operation[] operations =  ShardChangesAction.getOperations(indexShard, indexShard.getGlobalCheckpoint(),
            numWrites, numWrites + 1, Long.MAX_VALUE);
        assertThat(operations.length, equalTo(0));

        // get operations for a range some operations do not exist:
        operations = ShardChangesAction.getOperations(indexShard, indexShard.getGlobalCheckpoint(),
            numWrites  - 10, numWrites + 10, Long.MAX_VALUE);
        assertThat(operations.length, equalTo(10));
    }

    public void testGetOperationsWhenShardNotStarted() throws Exception {
        IndexShard indexShard = Mockito.mock(IndexShard.class);

        ShardRouting shardRouting = TestShardRouting.newShardRouting("index", 0, "_node_id", true, ShardRoutingState.INITIALIZING);
        Mockito.when(indexShard.routingEntry()).thenReturn(shardRouting);
        expectThrows(IndexShardNotStartedException.class, () -> ShardChangesAction.getOperations(indexShard,
            indexShard.getGlobalCheckpoint(), 0, 1, Long.MAX_VALUE));
    }

    public void testGetOperationsExceedByteLimit() throws Exception {
        final Settings settings = Settings.builder()
                .put("index.number_of_shards", 1)
                .put("index.number_of_replicas", 0)
                .build();
        final IndexService indexService = createIndex("index", settings);

        final long numWrites = 32;
        for (int i = 0; i < numWrites; i++) {
            client().prepareIndex("index", "doc", Integer.toString(i)).setSource("{}", XContentType.JSON).get();
        }

        final IndexShard indexShard = indexService.getShard(0);
        final Translog.Operation[] operations = ShardChangesAction.getOperations(indexShard, indexShard.getGlobalCheckpoint(),
            0, 12, 256);
        assertThat(operations.length, equalTo(12));
        assertThat(operations[0].seqNo(), equalTo(0L));
        assertThat(operations[1].seqNo(), equalTo(1L));
        assertThat(operations[2].seqNo(), equalTo(2L));
        assertThat(operations[3].seqNo(), equalTo(3L));
        assertThat(operations[4].seqNo(), equalTo(4L));
        assertThat(operations[5].seqNo(), equalTo(5L));
        assertThat(operations[6].seqNo(), equalTo(6L));
        assertThat(operations[7].seqNo(), equalTo(7L));
        assertThat(operations[8].seqNo(), equalTo(8L));
        assertThat(operations[9].seqNo(), equalTo(9L));
        assertThat(operations[10].seqNo(), equalTo(10L));
        assertThat(operations[11].seqNo(), equalTo(11L));
    }

    public void testGetOperationsAlwaysReturnAtLeastOneOp() throws Exception {
        final Settings settings = Settings.builder()
            .put("index.number_of_shards", 1)
            .put("index.number_of_replicas", 0)
            .build();
        final IndexService indexService = createIndex("index", settings);

        client().prepareIndex("index", "doc", "0").setSource("{}", XContentType.JSON).get();

        final IndexShard indexShard = indexService.getShard(0);
        final Translog.Operation[] operations =
            ShardChangesAction.getOperations(indexShard, indexShard.getGlobalCheckpoint(), 0, 1, 0);
        assertThat(operations.length, equalTo(1));
        assertThat(operations[0].seqNo(), equalTo(0L));
    }

    public void testIndexNotFound() throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicReference<Exception> reference = new AtomicReference<>();
        final ShardChangesAction.TransportAction transportAction = node().injector().getInstance(ShardChangesAction.TransportAction.class);
        transportAction.execute(
                new ShardChangesAction.Request(new ShardId(new Index("non-existent", "uuid"), 0)),
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
        });
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
        transportAction.execute(
                new ShardChangesAction.Request(new ShardId(indexService.getMetaData().getIndex(), numberOfShards)),
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
                });
        latch.await();
        assertNotNull(reference.get());
        assertThat(reference.get(), instanceOf(ShardNotFoundException.class));
    }

}
