/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.action.admin.indices.close;

import org.elasticsearch.action.admin.indices.flush.FlushRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.seqno.SeqNoStats;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.junit.Before;

import static org.elasticsearch.cluster.metadata.MetaDataIndexStateService.INDEX_CLOSED_BLOCK;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TransportShardCloseActionTests extends ESTestCase {

    private IndexShard indexShard;
    private TransportShardCloseAction action;
    private ClusterService clusterService;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();

        indexShard = mock(IndexShard.class);
        when(indexShard.getActiveOperationsCount()).thenReturn(0);
        when(indexShard.getGlobalCheckpoint()).thenReturn(0L);
        when(indexShard.seqNoStats()).thenReturn(new SeqNoStats(0L, 0L, 0L));

        final ShardId shardId = new ShardId("index", "_na_", randomIntBetween(0, 3));
        when(indexShard.shardId()).thenReturn(shardId);


        clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenReturn(new ClusterState.Builder(new ClusterName("test"))
            .blocks(ClusterBlocks.builder().addIndexBlock("index", INDEX_CLOSED_BLOCK).build()).build());

        action = new TransportShardCloseAction(Settings.EMPTY, mock(TransportService.class), clusterService,
            mock(IndicesService.class), mock(ThreadPool.class), mock(ShardStateAction.class), mock(ActionFilters.class),
            mock(IndexNameExpressionResolver.class));
    }

    private void executeOnPrimaryOrReplica() throws Exception {
        if (randomBoolean()) {
            assertNotNull(action.shardOperationOnPrimary(new ShardCloseRequest(indexShard.shardId()), indexShard));
        } else {
            assertNotNull(action.shardOperationOnPrimary(new ShardCloseRequest(indexShard.shardId()), indexShard));
        }
    }

    public void testOperationSuccessful() throws Exception {
        executeOnPrimaryOrReplica();
        verify(indexShard, times(1)).flush(any(FlushRequest.class));
    }

    public void testOperationFailsWithOnGoingOps() {
        when(indexShard.getActiveOperationsCount()).thenReturn(randomIntBetween(1, 10));

        IllegalStateException exception = expectThrows(IllegalStateException.class, this::executeOnPrimaryOrReplica);
        assertThat(exception.getMessage(),
            equalTo("On-going operations in progress while checking index shard " + indexShard.shardId() + " before closing"));
        verify(indexShard, times(0)).flush(any(FlushRequest.class));
    }

    public void testOperationFailsWithNoBlock() {
        when(clusterService.state()).thenReturn(new ClusterState.Builder(new ClusterName("test")).build());

        IllegalStateException exception = expectThrows(IllegalStateException.class, this::executeOnPrimaryOrReplica);
        assertThat(exception.getMessage(),
            equalTo("Index shard " + indexShard.shardId() + " must be blocked by " + INDEX_CLOSED_BLOCK + " before closing"));
        verify(indexShard, times(0)).flush(any(FlushRequest.class));
    }

    public void testOperationFailsWithGlobalCheckpointNotCaughtUp() {
        final long maxSeqNo = randomLongBetween(SequenceNumbers.UNASSIGNED_SEQ_NO, Long.MAX_VALUE);
        final long localCheckpoint = randomLongBetween(SequenceNumbers.UNASSIGNED_SEQ_NO, maxSeqNo);
        final long globalCheckpoint = randomValueOtherThan(maxSeqNo,
            () -> randomLongBetween(SequenceNumbers.UNASSIGNED_SEQ_NO, localCheckpoint));
        when(indexShard.seqNoStats()).thenReturn(new SeqNoStats(maxSeqNo, localCheckpoint, globalCheckpoint));
        when(indexShard.getGlobalCheckpoint()).thenReturn(globalCheckpoint);

        IllegalStateException exception = expectThrows(IllegalStateException.class, this::executeOnPrimaryOrReplica);
        assertThat(exception.getMessage(), equalTo("Global checkpoint [" + globalCheckpoint + "] mismatches maximum sequence number ["
            + maxSeqNo + "] on index shard " + indexShard.shardId()));
        verify(indexShard, times(0)).flush(any(FlushRequest.class));
    }
}
