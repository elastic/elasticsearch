/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package co.elastic.elasticsearch.stateless.engine.translog;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Collections;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItems;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class NodeTranslogBufferTests extends ESTestCase {

    private final BytesArray header = new BytesArray(new byte[] { 'h', 'e', 'a', 'd', 'e', 'r' });

    public void testNoIntervalFlushIfSyncNotRequested() {
        NodeTranslogBuffer translogBuffer = new NodeTranslogBuffer(BigArrays.NON_RECYCLING_INSTANCE, 10);
        assertFalse(translogBuffer.markMinimumIntervalExhausted());
    }

    public void testIntervalFlushIfSyncIsRequested() {
        NodeTranslogBuffer translogBuffer = new NodeTranslogBuffer(BigArrays.NON_RECYCLING_INSTANCE, 10);
        assertFalse(translogBuffer.markSyncRequested());
        assertTrue(translogBuffer.markMinimumIntervalExhausted());
    }

    public void testFlushRequestedIfSyncRequestTriggeredAfterIntervalExhausted() {
        NodeTranslogBuffer translogBuffer = new NodeTranslogBuffer(BigArrays.NON_RECYCLING_INSTANCE, 10);
        assertFalse(translogBuffer.markMinimumIntervalExhausted());
        assertTrue(translogBuffer.markSyncRequested());
        // Will only indicate flush should happen once
        assertFalse(translogBuffer.markSyncRequested());
    }

    public void testFlushRequestedIfSizeThresholdExhausted() throws IOException {
        NodeTranslogBuffer translogBuffer = new NodeTranslogBuffer(BigArrays.NON_RECYCLING_INSTANCE, 50);
        if (randomBoolean()) {
            assertFalse(translogBuffer.markMinimumIntervalExhausted());
        }
        assertTrue(translogBuffer.writeToBuffer(mock(ShardSyncState.class), serialized(new byte[30]), 1, new Translog.Location(0, 0, 40)));
        assertFalse(translogBuffer.shouldFlushBufferDueToSize());
        assertTrue(translogBuffer.writeToBuffer(mock(ShardSyncState.class), serialized(new byte[10]), 2, new Translog.Location(0, 40, 20)));

        assertTrue(translogBuffer.shouldFlushBufferDueToSize());
        // Will only indicate flush should happen once
        assertFalse(translogBuffer.shouldFlushBufferDueToSize());
    }

    public void testCannotAddIfTranslogHasBeenWritten() throws IOException {
        NodeTranslogBuffer translogBuffer = new NodeTranslogBuffer(BigArrays.NON_RECYCLING_INSTANCE, 50);
        ShardSyncState shardSyncState = mock(ShardSyncState.class);
        when(shardSyncState.getShardId()).thenReturn(new ShardId("test1", "_na_", 0));
        when(shardSyncState.createDirectory(1, 1)).thenReturn(new TranslogMetadata.Directory(0, new int[0]));

        assertTrue(translogBuffer.writeToBuffer(shardSyncState, serialized(new byte[40]), 1, new Translog.Location(0, 0, 50)));

        translogBuffer.complete(1, Set.of(shardSyncState));

        assertFalse(translogBuffer.writeToBuffer(shardSyncState, serialized(new byte[10]), 2, new Translog.Location(0, 50, 20)));
    }

    public void testInactiveShardsAreNotIncludedInTranslog() throws IOException {
        ShardSyncState inactiveShard = mock(ShardSyncState.class);
        when(inactiveShard.getShardId()).thenReturn(new ShardId("inactive", "_na_", 0));

        ShardSyncState activeShard = mock(ShardSyncState.class);
        ShardId activeShardId = new ShardId("active", "_na_", 0);
        when(activeShard.getShardId()).thenReturn(activeShardId);
        when(activeShard.createDirectory(0, 1)).thenReturn(new TranslogMetadata.Directory(1, new int[0]));
        Translog.Serialized operation = serialized(new byte[100]);

        NodeTranslogBuffer translogBuffer = new NodeTranslogBuffer(BigArrays.NON_RECYCLING_INSTANCE, 1000);
        assertTrue(translogBuffer.writeToBuffer(inactiveShard, operation, 1, new Translog.Location(0, 0, operation.length())));
        assertNull(translogBuffer.complete(0, Collections.emptySet()));

        translogBuffer = new NodeTranslogBuffer(BigArrays.NON_RECYCLING_INSTANCE, 1000);
        assertTrue(translogBuffer.writeToBuffer(inactiveShard, operation, 1, new Translog.Location(0, 0, operation.length())));

        assertTrue(translogBuffer.writeToBuffer(activeShard, operation, 1, new Translog.Location(0, 0, operation.length())));
        TranslogReplicator.CompoundTranslog translog = translogBuffer.complete(0, Set.of(activeShard));
        assertThat(translog.metadata().operations().keySet(), hasItems(activeShardId));
        assertThat(translog.metadata().operations().size(), equalTo(1));
        assertThat(translog.metadata().syncedLocations().keySet(), hasItems(activeShardId));
        assertThat(translog.metadata().syncedLocations().size(), equalTo(1));
    }

    public void testActiveShardsWithoutBufferedDataAreCheckpointedInTranslog() throws IOException {
        ShardSyncState activeButNoBufferedDataShard = mock(ShardSyncState.class);
        ShardId activeButNoBufferedShardId = new ShardId("active_but_no_buffered", "_na_", 0);
        when(activeButNoBufferedDataShard.getShardId()).thenReturn(activeButNoBufferedShardId);
        when(activeButNoBufferedDataShard.createDirectory(0, 0)).thenReturn(new TranslogMetadata.Directory(0, new int[0]));

        ShardSyncState activeShard = mock(ShardSyncState.class);
        ShardId activeShardId = new ShardId("active", "_na_", 0);
        when(activeShard.getShardId()).thenReturn(activeShardId);
        when(activeShard.createDirectory(0, 1)).thenReturn(new TranslogMetadata.Directory(1, new int[0]));

        NodeTranslogBuffer translogBuffer = new NodeTranslogBuffer(BigArrays.NON_RECYCLING_INSTANCE, 1000);
        assertTrue(translogBuffer.writeToBuffer(activeShard, serialized(new byte[60]), 1, new Translog.Location(0, 0, 100)));

        TranslogReplicator.CompoundTranslog translog = translogBuffer.complete(0, Set.of(activeShard, activeButNoBufferedDataShard));
        assertThat(translog.metadata().operations().keySet(), hasItems(activeShardId, activeButNoBufferedShardId));
        assertThat(translog.metadata().operations().size(), equalTo(2));
        assertThat(translog.metadata().syncedLocations().keySet(), hasItems(activeShardId));
        assertThat(translog.metadata().syncedLocations().size(), equalTo(1));
    }

    public void testClosedShardIsNotCheckpointedInTranslog() throws IOException {
        ShardSyncState closedShard = mock(ShardSyncState.class);
        ShardId closedShardId = new ShardId("closed", "_na_", 0);
        when(closedShard.getShardId()).thenReturn(closedShardId);
        when(closedShard.createDirectory(0, 0)).thenReturn(new TranslogMetadata.Directory(0, new int[0]));
        when(closedShard.isClosed()).thenReturn(true);

        ShardSyncState activeShard = mock(ShardSyncState.class);
        ShardId activeShardId = new ShardId("active", "_na_", 0);
        when(activeShard.getShardId()).thenReturn(activeShardId);
        when(activeShard.createDirectory(0, 1)).thenReturn(new TranslogMetadata.Directory(1, new int[0]));

        NodeTranslogBuffer translogBuffer = new NodeTranslogBuffer(BigArrays.NON_RECYCLING_INSTANCE, 1000);
        assertTrue(translogBuffer.writeToBuffer(activeShard, serialized(new byte[90]), 1, new Translog.Location(0, 0, 100)));

        TranslogReplicator.CompoundTranslog translog = translogBuffer.complete(0, Set.of(activeShard, closedShard));
        assertThat(translog.metadata().operations().keySet(), hasItems(activeShardId));
        assertThat(translog.metadata().operations().size(), equalTo(1));
        assertThat(translog.metadata().syncedLocations().keySet(), hasItems(activeShardId));
        assertThat(translog.metadata().syncedLocations().size(), equalTo(1));
    }

    private Translog.Serialized serialized(byte[] source) {
        return new Translog.Serialized(header, new BytesArray(source), 0);
    }
}
