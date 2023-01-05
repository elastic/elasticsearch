/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.searchablesnapshots.cache.common;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.blobcache.common.ByteRange;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.get.GetResult;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshots;
import org.elasticsearch.xpack.searchablesnapshots.cache.blob.BlobStoreCacheService;
import org.elasticsearch.xpack.searchablesnapshots.store.IndexInputStats;

import java.util.concurrent.ConcurrentHashMap;

import static org.elasticsearch.index.seqno.SequenceNumbers.UNASSIGNED_PRIMARY_TERM;
import static org.elasticsearch.index.seqno.SequenceNumbers.UNASSIGNED_SEQ_NO;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;

public final class TestUtils {
    private TestUtils() {}

    public static void assertCounter(IndexInputStats.Counter counter, long total, long count, long min, long max) {
        assertThat(counter.total(), equalTo(total));
        assertThat(counter.count(), equalTo(count));
        assertThat(counter.min(), equalTo(min));
        assertThat(counter.max(), equalTo(max));
    }

    public static class NoopBlobStoreCacheService extends BlobStoreCacheService {

        public NoopBlobStoreCacheService() {
            super(null, mock(Client.class), SearchableSnapshots.SNAPSHOT_BLOB_CACHE_INDEX);
        }

        @Override
        protected boolean useLegacyCachedBlobSizes() {
            return false;
        }

        @Override
        protected void innerGet(GetRequest request, ActionListener<GetResponse> listener) {
            listener.onFailure(new IndexNotFoundException(request.index()));
        }

        @Override
        protected void innerPut(IndexRequest request, ActionListener<IndexResponse> listener) {
            listener.onFailure(new IndexNotFoundException(request.index()));
        }

        @Override
        public ByteRange computeBlobCacheByteRange(ShardId shardId, String fileName, long fileLength, ByteSizeValue maxMetadataLength) {
            return ByteRange.EMPTY;
        }
    }

    public static class SimpleBlobStoreCacheService extends BlobStoreCacheService {

        private final ConcurrentHashMap<String, BytesArray> blobs = new ConcurrentHashMap<>();

        public SimpleBlobStoreCacheService() {
            super(null, mock(Client.class), SearchableSnapshots.SNAPSHOT_BLOB_CACHE_INDEX);
        }

        @Override
        protected boolean useLegacyCachedBlobSizes() {
            return false;
        }

        @Override
        protected void innerGet(GetRequest request, ActionListener<GetResponse> listener) {
            final BytesArray bytes = blobs.get(request.id());
            listener.onResponse(
                new GetResponse(
                    new GetResult(
                        request.index(),
                        request.id(),
                        UNASSIGNED_SEQ_NO,
                        UNASSIGNED_PRIMARY_TERM,
                        0L,
                        bytes != null,
                        bytes,
                        null,
                        null
                    )
                )
            );
        }

        @Override
        protected void innerPut(IndexRequest request, ActionListener<IndexResponse> listener) {
            final BytesArray bytesArray = blobs.put(request.id(), new BytesArray(request.source().toBytesRef(), true));
            listener.onResponse(
                new IndexResponse(
                    new ShardId(request.index(), "_na", 0),
                    request.id(),
                    UNASSIGNED_SEQ_NO,
                    UNASSIGNED_PRIMARY_TERM,
                    0L,
                    bytesArray == null
                )
            );
        }
    }
}
