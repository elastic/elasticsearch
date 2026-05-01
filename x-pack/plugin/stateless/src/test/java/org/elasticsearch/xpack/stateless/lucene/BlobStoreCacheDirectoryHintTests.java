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

package org.elasticsearch.xpack.stateless.lucene;

import org.apache.lucene.store.DataAccessHint;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.elasticsearch.blobcache.BlobCacheMetrics;
import org.elasticsearch.blobcache.shared.SharedBytes;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.stateless.cache.StatelessSharedBlobCacheService;
import org.elasticsearch.xpack.stateless.cache.reader.CacheBlobReader;
import org.elasticsearch.xpack.stateless.cache.reader.CacheFileReader;
import org.elasticsearch.xpack.stateless.cache.reader.CacheFileReaderTestUtils;
import org.elasticsearch.xpack.stateless.commits.BlobFile;
import org.elasticsearch.xpack.stateless.commits.BlobFileRanges;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.xpack.stateless.commits.BlobLocationTestUtils.createBlobFileRanges;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class BlobStoreCacheDirectoryHintTests extends ESTestCase {

    // Verify that opening a .vec file automatically injects DataAccessHint.RANDOM into the IOContext.
    public void testVecFileGetsRandomAccessHint() throws IOException {
        var capturedContext = new AtomicReference<IOContext>();
        var dir = createCapturingDirectory(capturedContext);
        dir.updateMetadata(Map.of("_0.vec", createBlobFileRanges(1L, 0L, 0, 1024)), 1024L);

        dir.openInput("_0.vec", IOContext.DEFAULT);

        assertTrue(capturedContext.get().hints().contains(DataAccessHint.RANDOM));
    }

    // Verify that opening a non-.vec file does not inject DataAccessHint.RANDOM.
    public void testNonVecFileDoesNotGetHint() throws IOException {
        var capturedContext = new AtomicReference<IOContext>();
        var dir = createCapturingDirectory(capturedContext);
        dir.updateMetadata(Map.of("_0.doc", createBlobFileRanges(1L, 0L, 0, 1024)), 1024L);

        dir.openInput("_0.doc", IOContext.DEFAULT);

        assertFalse(capturedContext.get().hints().contains(DataAccessHint.RANDOM));
    }

    // Verify that an already-present DataAccessHint.RANDOM is preserved and not duplicated.
    public void testExplicitHintIsPreserved() throws IOException {
        var capturedContext = new AtomicReference<IOContext>();
        var dir = createCapturingDirectory(capturedContext);
        dir.updateMetadata(Map.of("_0.vec", createBlobFileRanges(1L, 0L, 0, 1024)), 1024L);

        IOContext ctxWithHint = IOContext.DEFAULT.withHints(DataAccessHint.RANDOM);
        dir.openInput("_0.vec", ctxWithHint);

        assertTrue(capturedContext.get().hints().contains(DataAccessHint.RANDOM));
    }

    // Verify that CacheFileReader constructed with RANDOM hint preserves advice through copy().
    @SuppressWarnings("unchecked")
    public void testCacheFileReaderCopyPreservesAdvice() {
        var cacheFile = mock(StatelessSharedBlobCacheService.CacheFile.class);
        when(cacheFile.copy()).thenReturn(cacheFile);
        var cacheBlobReader = mock(CacheBlobReader.class);
        var blobFileRanges = createBlobFileRanges(1L, 0L, 0, 1024);

        var original = new CacheFileReader(
            cacheFile,
            cacheBlobReader,
            blobFileRanges,
            BlobCacheMetrics.NOOP,
            System::currentTimeMillis,
            IOContext.DEFAULT.withHints(DataAccessHint.RANDOM)
        );
        var copy = original.copy();

        assertNotSame(original, copy);
        assertEquals(SharedBytes.MADV_RANDOM, CacheFileReaderTestUtils.getRegionAdvice(copy));
    }

    // Verify that copyWithContext derives MADV_RANDOM or MADV_NORMAL from IOContext hints.
    @SuppressWarnings("unchecked")
    public void testCacheFileReaderCopyWithContext() {
        var cacheFile = mock(StatelessSharedBlobCacheService.CacheFile.class);
        when(cacheFile.copy()).thenReturn(cacheFile);
        var cacheBlobReader = mock(CacheBlobReader.class);
        var blobFileRanges = createBlobFileRanges(1L, 0L, 0, 1024);

        var original = new CacheFileReader(cacheFile, cacheBlobReader, blobFileRanges, BlobCacheMetrics.NOOP, System::currentTimeMillis);

        var withRandom = original.copyWithContext(IOContext.DEFAULT.withHints(DataAccessHint.RANDOM));
        assertNotSame(original, withRandom);
        assertEquals(SharedBytes.MADV_RANDOM, CacheFileReaderTestUtils.getRegionAdvice(withRandom));

        var withNormal = withRandom.copyWithContext(IOContext.DEFAULT);
        assertNotSame(withRandom, withNormal);
        assertEquals(SharedBytes.MADV_NORMAL, CacheFileReaderTestUtils.getRegionAdvice(withNormal));
    }

    // Verify that slice(String, long, long, IOContext) with DataAccessHint.RANDOM sets MADV_RANDOM
    // on the resulting BlobCacheIndexInput's CacheFileReader.
    @SuppressWarnings("unchecked")
    public void testSliceWithRandomHintSetsMadvRandom() {
        var cacheFile = mock(StatelessSharedBlobCacheService.CacheFile.class);
        when(cacheFile.copy()).thenReturn(cacheFile);
        var cacheBlobReader = mock(CacheBlobReader.class);
        var blobFileRanges = createBlobFileRanges(1L, 0L, 0, 1024);

        var reader = new CacheFileReader(cacheFile, cacheBlobReader, blobFileRanges, BlobCacheMetrics.NOOP, System::currentTimeMillis);
        var indexInput = new BlobCacheIndexInput("test.cfs", IOContext.DEFAULT, reader, null, 1024, 0);

        IOContext randomCtx = IOContext.DEFAULT.withHints(DataAccessHint.RANDOM);
        var slice = (BlobCacheIndexInput) indexInput.slice("_0.vec", 0, 512, randomCtx);

        assertEquals(SharedBytes.MADV_RANDOM, BlobStoreCacheDirectoryTestUtils.getRegionAdvice(slice));
    }

    // Verify that slice(String, long, long, IOContext) without DataAccessHint.RANDOM sets MADV_NORMAL.
    @SuppressWarnings("unchecked")
    public void testSliceWithoutRandomHintSetsMadvNormal() {
        var cacheFile = mock(StatelessSharedBlobCacheService.CacheFile.class);
        when(cacheFile.copy()).thenReturn(cacheFile);
        var cacheBlobReader = mock(CacheBlobReader.class);
        var blobFileRanges = createBlobFileRanges(1L, 0L, 0, 1024);

        var reader = new CacheFileReader(
            cacheFile,
            cacheBlobReader,
            blobFileRanges,
            BlobCacheMetrics.NOOP,
            System::currentTimeMillis,
            IOContext.DEFAULT.withHints(DataAccessHint.RANDOM)
        );
        var indexInput = new BlobCacheIndexInput("test.cfs", IOContext.DEFAULT, reader, null, 1024, 0);

        var slice = (BlobCacheIndexInput) indexInput.slice("_0.doc", 0, 512, IOContext.DEFAULT);

        assertEquals(SharedBytes.MADV_NORMAL, BlobStoreCacheDirectoryTestUtils.getRegionAdvice(slice));
    }

    // Verify that a .vec file opened directly (not through compound) gets MADV_RANDOM from openInput,
    // and then a 4-arg slice from a compound file also correctly propagates MADV_RANDOM.
    @SuppressWarnings("unchecked")
    public void testSliceWithRandomHintPreservesOffset() {
        var cacheFile = mock(StatelessSharedBlobCacheService.CacheFile.class);
        when(cacheFile.copy()).thenReturn(cacheFile);
        var cacheBlobReader = mock(CacheBlobReader.class);
        var blobFileRanges = createBlobFileRanges(1L, 0L, 0, 2048);

        var reader = new CacheFileReader(cacheFile, cacheBlobReader, blobFileRanges, BlobCacheMetrics.NOOP, System::currentTimeMillis);
        var indexInput = new BlobCacheIndexInput("test.cfs", IOContext.DEFAULT, reader, null, 2048, 100);

        IOContext randomCtx = IOContext.DEFAULT.withHints(DataAccessHint.RANDOM);
        var slice = (BlobCacheIndexInput) indexInput.slice("_0.vec", 200, 512, randomCtx);

        assertEquals(SharedBytes.MADV_RANDOM, BlobStoreCacheDirectoryTestUtils.getRegionAdvice(slice));
        assertEquals(512, slice.length());
    }

    private static BlobStoreCacheDirectory createCapturingDirectory(AtomicReference<IOContext> capturedContext) {
        var cacheService = mock(StatelessSharedBlobCacheService.class);
        var shardId = mock(ShardId.class);

        return new BlobStoreCacheDirectory(cacheService, shardId) {
            @Override
            protected IndexInput doOpenInput(String name, IOContext context, BlobFileRanges blobFileRanges) {
                capturedContext.set(context);
                return mock(IndexInput.class);
            }

            @Override
            protected CacheBlobReader getCacheBlobReader(String fileName, BlobFile blobFile) {
                return mock(CacheBlobReader.class);
            }

            @Override
            public CacheBlobReader getCacheBlobReaderForWarming(BlobFile blobFile) {
                return mock(CacheBlobReader.class);
            }

            @Override
            public BlobStoreCacheDirectory createNewBlobStoreCacheDirectoryForWarming() {
                return this;
            }
        };
    }
}
