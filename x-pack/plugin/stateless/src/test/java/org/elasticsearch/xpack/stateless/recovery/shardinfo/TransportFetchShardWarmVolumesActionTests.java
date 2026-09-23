/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.recovery.shardinfo;

import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.stateless.commits.BlobFile;
import org.elasticsearch.xpack.stateless.commits.BlobFileRanges;
import org.elasticsearch.xpack.stateless.commits.BlobLocation;
import org.elasticsearch.xpack.stateless.engine.PrimaryTermAndGeneration;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;

import static org.elasticsearch.cluster.routing.ShardRoutingState.STARTED;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TransportFetchShardWarmVolumesActionTests extends ESTestCase {

    public void testEstimateWarmVolumeTwoFilesOneBlobUsesMax() {
        BlobFileRanges first = range("blob-a", 0, 10);
        BlobFileRanges second = range("blob-a", 5, 20);
        assertThat(TransportFetchShardWarmVolumesAction.estimateWarmVolume(List.of(first, second)), equalTo(25L));
    }

    public void testEstimateWarmVolumeTwoBlobsSumsMaxima() {
        BlobFileRanges blobA = range("blob-a", 0, 10);
        BlobFileRanges blobB = range("blob-b", 3, 7);
        assertThat(TransportFetchShardWarmVolumesAction.estimateWarmVolume(List.of(blobA, blobB)), equalTo(20L));
    }

    public void testEstimateWarmVolumeEmptyIsZero() {
        assertThat(TransportFetchShardWarmVolumesAction.estimateWarmVolume(List.of()), equalTo(0L));
    }

    public void testSnapshotSearchableShardsSkipsNonSearchable() {
        Index index = new Index("idx", randomUUID());
        IndexShard searchable = mockShard(index, 0, ShardRouting.Role.SEARCH_ONLY);
        IndexShard indexing = mockShard(index, 1, ShardRouting.Role.INDEX_ONLY);
        IndexService indexService = mock(IndexService.class);
        when(indexService.iterator()).thenReturn(List.of(searchable, indexing).iterator());
        IndicesService indicesService = mock(IndicesService.class);
        when(indicesService.iterator()).thenReturn(List.of(indexService).iterator());

        List<IndexShard> snapshot = TransportFetchShardWarmVolumesAction.snapshotSearchableShards(indicesService);
        assertThat(snapshot, equalTo(List.of(searchable)));
    }

    public void testFailedEstimateIsSkippedNotZero() {
        Index index = new Index("idx", randomUUID());
        IndexShard shard = mockShard(index, 0, ShardRouting.Role.SEARCH_ONLY);
        Store store = mock(Store.class);
        when(shard.store()).thenReturn(store);

        assertThat(TransportFetchShardWarmVolumesAction.tryEstimateShardWarmVolume(shard), equalTo(OptionalLong.empty()));
        verify(shard, never()).storeStats();
    }

    public void testOneFailureYieldsPartialMap() {
        Index index = new Index("idx", randomUUID());
        IndexShard ok = mockShard(index, 0, ShardRouting.Role.SEARCH_ONLY);
        IndexShard failing = mockShard(index, 1, ShardRouting.Role.SEARCH_ONLY);
        Store failingStore = mock(Store.class);
        when(failing.store()).thenReturn(failingStore);

        Map<Index, Map<Integer, Long>> volumes = TransportFetchShardWarmVolumesAction.collectWarmVolumes(List.of(ok, failing), shard -> {
            if (shard == ok) {
                return OptionalLong.of(40L);
            }
            return TransportFetchShardWarmVolumesAction.tryEstimateShardWarmVolume(shard);
        });
        assertThat(volumes.get(index), equalTo(Map.of(0, 40L)));
        verify(failing, never()).storeStats();
        verify(ok, never()).storeStats();
    }

    public void testResponseRoundTrip() throws IOException {
        Index index = new Index("idx", randomUUID());
        var original = new TransportFetchShardWarmVolumesAction.Response(randomNonNegativeLong(), Map.of(index, Map.of(0, 12L, 1, 34L)));
        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);
        var copy = new TransportFetchShardWarmVolumesAction.Response(out.bytes().streamInput());
        assertThat(copy, equalTo(original));
        assertThat(copy.toEntry().volumes().size(), equalTo(2));
    }

    private static IndexShard mockShard(Index index, int shard, ShardRouting.Role role) {
        ShardId shardId = new ShardId(index, shard);
        ShardRouting routing = TestShardRouting.shardRoutingBuilder(shardId, "node", false, STARTED).withRole(role).build();
        IndexShard indexShard = mock(IndexShard.class);
        when(indexShard.routingEntry()).thenReturn(routing);
        when(indexShard.shardId()).thenReturn(shardId);
        return indexShard;
    }

    private static BlobFileRanges range(String blobName, long offset, long length) {
        return new BlobFileRanges(new BlobLocation(new BlobFile(blobName, new PrimaryTermAndGeneration(1, -1)), offset, length));
    }
}
