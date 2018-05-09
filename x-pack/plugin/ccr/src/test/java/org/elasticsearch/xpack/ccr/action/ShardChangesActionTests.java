/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ccr.action;

import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardNotStartedException;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class ShardChangesActionTests extends ESSingleNodeTestCase {

    public void testGetOperationsBetween() throws Exception {
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
            final ShardChangesAction.Response r = ShardChangesAction.getOperationsBetween(indexShard, min, max, Long.MAX_VALUE);
            final List<Long> seenSeqNos = Arrays.stream(r.getOperations()).map(Translog.Operation::seqNo).collect(Collectors.toList());
            final List<Long> expectedSeqNos = LongStream.range(min, max + 1).boxed().collect(Collectors.toList());
            assertThat(seenSeqNos, equalTo(expectedSeqNos));
        }

        // get operations for a range no operations exists:
        Exception e = expectThrows(IllegalStateException.class,
                () -> ShardChangesAction.getOperationsBetween(indexShard, numWrites, numWrites + 1, Long.MAX_VALUE));
        assertThat(e.getMessage(), containsString("Not all operations between min_seqno [" + numWrites + "] and max_seqno [" +
                (numWrites + 1) +"] found"));

        // get operations for a range some operations do not exist:
        e = expectThrows(IllegalStateException.class,
                () -> ShardChangesAction.getOperationsBetween(indexShard, numWrites  - 10, numWrites + 10, Long.MAX_VALUE));
        assertThat(e.getMessage(), containsString("Not all operations between min_seqno [" + (numWrites - 10) + "] and max_seqno [" +
                (numWrites + 10) +"] found"));
    }

    public void testGetOperationsBetweenWhenShardNotStarted() throws Exception {
        IndexShard indexShard = Mockito.mock(IndexShard.class);

        ShardRouting shardRouting = TestShardRouting.newShardRouting("index", 0, "_node_id", true, ShardRoutingState.INITIALIZING);
        Mockito.when(indexShard.routingEntry()).thenReturn(shardRouting);
        expectThrows(IndexShardNotStartedException.class, () -> ShardChangesAction.getOperationsBetween(indexShard, 0, 1, Long.MAX_VALUE));
    }

    public void testGetOperationsBetweenExceedByteLimit() throws Exception {
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
        final ShardChangesAction.Response r = ShardChangesAction.getOperationsBetween(indexShard, 0, numWrites - 1, 256);
        assertThat(r.getOperations().length, equalTo(12));
        assertThat(r.getOperations()[0].seqNo(), equalTo(0L));
        assertThat(r.getOperations()[1].seqNo(), equalTo(1L));
        assertThat(r.getOperations()[2].seqNo(), equalTo(2L));
        assertThat(r.getOperations()[3].seqNo(), equalTo(3L));
        assertThat(r.getOperations()[4].seqNo(), equalTo(4L));
        assertThat(r.getOperations()[5].seqNo(), equalTo(5L));
        assertThat(r.getOperations()[6].seqNo(), equalTo(6L));
        assertThat(r.getOperations()[7].seqNo(), equalTo(7L));
        assertThat(r.getOperations()[8].seqNo(), equalTo(8L));
        assertThat(r.getOperations()[9].seqNo(), equalTo(9L));
        assertThat(r.getOperations()[10].seqNo(), equalTo(10L));
        assertThat(r.getOperations()[11].seqNo(), equalTo(11L));
    }

}
