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
import org.elasticsearch.common.TriConsumer;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.DeleteResult;
import org.elasticsearch.common.blobstore.OptionalBytesReference;
import org.elasticsearch.common.blobstore.support.BlobMetadata;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.get.GetResult;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.xpack.searchablesnapshots.cache.blob.BlobStoreCacheService;
import org.elasticsearch.xpack.searchablesnapshots.store.IndexInputStats;
import org.hamcrest.MatcherAssert;

import java.io.ByteArrayInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;

import static java.util.Collections.synchronizedNavigableSet;
import static org.elasticsearch.blobcache.BlobCacheTestUtils.mergeContiguousRanges;
import static org.elasticsearch.blobcache.BlobCacheUtils.toIntBytes;
import static org.elasticsearch.index.seqno.SequenceNumbers.UNASSIGNED_PRIMARY_TERM;
import static org.elasticsearch.index.seqno.SequenceNumbers.UNASSIGNED_SEQ_NO;
import static org.elasticsearch.test.ESTestCase.between;
import static org.elasticsearch.test.ESTestCase.randomLongBetween;
import static org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshots.SNAPSHOT_BLOB_CACHE_INDEX;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public final class TestUtils {
    private TestUtils() {}

    public static SortedSet<ByteRange> randomPopulateAndReads(final CacheFile cacheFile) {
        return randomPopulateAndReads(cacheFile, (fileChannel, aLong, aLong2) -> {});
    }

    public static SortedSet<ByteRange> randomPopulateAndReads(CacheFile cacheFile, TriConsumer<FileChannel, Long, Long> consumer) {
        final SortedSet<ByteRange> ranges = synchronizedNavigableSet(new TreeSet<>());
        final List<Future<Integer>> futures = new ArrayList<>();
        final DeterministicTaskQueue deterministicTaskQueue = new DeterministicTaskQueue();
        for (int i = 0; i < between(0, 10); i++) {
            final long start = randomLongBetween(0L, Math.max(0L, cacheFile.getLength() - 1L));
            final long end = randomLongBetween(Math.min(start + 1L, cacheFile.getLength()), cacheFile.getLength());
            final ByteRange range = ByteRange.of(start, end);
            futures.add(
                cacheFile.populateAndRead(range, range, channel -> Math.toIntExact(end - start), (channel, from, to, progressUpdater) -> {
                    consumer.apply(channel, from, to);
                    ranges.add(ByteRange.of(from, to));
                    progressUpdater.accept(to);
                }, deterministicTaskQueue.getThreadPool().generic())
            );
        }
        deterministicTaskQueue.runAllRunnableTasks();
        assertTrue(futures.stream().allMatch(Future::isDone));
        return mergeContiguousRanges(ranges);
    }

    public static void assertCacheFileEquals(CacheFile expected, CacheFile actual) {
        MatcherAssert.assertThat(actual.getLength(), equalTo(expected.getLength()));
        MatcherAssert.assertThat(actual.getFile(), equalTo(expected.getFile()));
        MatcherAssert.assertThat(actual.getCacheKey(), equalTo(expected.getCacheKey()));
        MatcherAssert.assertThat(actual.getCompletedRanges(), equalTo(expected.getCompletedRanges()));
    }

    public static long sumOfCompletedRangesLengths(CacheFile cacheFile) {
        return cacheFile.getCompletedRanges().stream().mapToLong(ByteRange::length).sum();
    }

    public static void assertCounter(IndexInputStats.Counter counter, long total, long count, long min, long max) {
        assertThat(counter.total(), equalTo(total));
        assertThat(counter.count(), equalTo(count));
        assertThat(counter.min(), equalTo(min));
        assertThat(counter.max(), equalTo(max));
    }

    /**
     * A {@link BlobContainer} that can read a single in-memory blob.
     * Any attempt to read a different blob will throw a {@link FileNotFoundException}
     */
    public static BlobContainer singleBlobContainer(final String blobName, final byte[] blobContent) {
        return new MostlyUnimplementedFakeBlobContainer() {
            @Override
            public InputStream readBlob(String name, long position, long length) throws IOException {
                if (blobName.equals(name) == false) {
                    throw new FileNotFoundException("Blob not found: " + name);
                }
                return Streams.limitStream(
                    new ByteArrayInputStream(blobContent, toIntBytes(position), blobContent.length - toIntBytes(position)),
                    length
                );
            }
        };
    }

    public static BlobContainer singleSplitBlobContainer(final String blobName, final byte[] blobContent, final int partSize) {
        if (partSize >= blobContent.length) {
            return singleBlobContainer(blobName, blobContent);
        } else {
            final String prefix = blobName + ".part";
            return new MostlyUnimplementedFakeBlobContainer() {
                @Override
                public InputStream readBlob(String name, long position, long length) throws IOException {
                    if (name.startsWith(prefix) == false) {
                        throw new FileNotFoundException("Blob not found: " + name);
                    }
                    assert position + length <= partSize
                        : "cannot read [" + position + "-" + (position + length) + "] from array part of length [" + partSize + "]";
                    final int partNumber = Integer.parseInt(name.substring(prefix.length()));
                    final int positionInBlob = toIntBytes(position) + partSize * partNumber;
                    assert positionInBlob + length <= blobContent.length
                        : "cannot read ["
                            + positionInBlob
                            + "-"
                            + (positionInBlob + length)
                            + "] from array of length ["
                            + blobContent.length
                            + "]";
                    return Streams.limitStream(
                        new ByteArrayInputStream(blobContent, positionInBlob, blobContent.length - positionInBlob),
                        length
                    );
                }
            };
        }
    }

    private static class MostlyUnimplementedFakeBlobContainer implements BlobContainer {

        @Override
        public long readBlobPreferredLength() {
            return Long.MAX_VALUE;
        }

        @Override
        public Map<String, BlobMetadata> listBlobs() {
            throw unsupportedException();
        }

        @Override
        public BlobPath path() {
            throw unsupportedException();
        }

        @Override
        public boolean blobExists(String blobName) {
            throw unsupportedException();
        }

        @Override
        public InputStream readBlob(String blobName) {
            throw unsupportedException();
        }

        @Override
        public InputStream readBlob(String blobName, long position, long length) throws IOException {
            throw unsupportedException();
        }

        @Override
        public void writeBlob(String blobName, InputStream inputStream, long blobSize, boolean failIfAlreadyExists) {
            throw unsupportedException();
        }

        @Override
        public void writeMetadataBlob(
            String blobName,
            boolean failIfAlreadyExists,
            boolean atomic,
            CheckedConsumer<OutputStream, IOException> writer
        ) {
            throw unsupportedException();
        }

        @Override
        public void writeBlobAtomic(String blobName, BytesReference bytes, boolean failIfAlreadyExists) {
            throw unsupportedException();
        }

        @Override
        public DeleteResult delete() {
            throw unsupportedException();
        }

        @Override
        public void deleteBlobsIgnoringIfNotExists(Iterator<String> blobNames) {
            throw unsupportedException();
        }

        @Override
        public Map<String, BlobContainer> children() {
            throw unsupportedException();
        }

        @Override
        public Map<String, BlobMetadata> listBlobsByPrefix(String blobNamePrefix) {
            throw unsupportedException();
        }

        @Override
        public void compareAndExchangeRegister(
            String key,
            BytesReference expected,
            BytesReference updated,
            ActionListener<OptionalBytesReference> listener
        ) {
            listener.onFailure(unsupportedException());
        }

        private UnsupportedOperationException unsupportedException() {
            assert false : "this operation is not supported and should have not be called";
            return new UnsupportedOperationException("This operation is not supported");
        }
    }

    public static class NoopBlobStoreCacheService extends BlobStoreCacheService {

        public NoopBlobStoreCacheService() {
            super(null, mock(Client.class), SNAPSHOT_BLOB_CACHE_INDEX);
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
            super(null, mock(Client.class), SNAPSHOT_BLOB_CACHE_INDEX);
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
