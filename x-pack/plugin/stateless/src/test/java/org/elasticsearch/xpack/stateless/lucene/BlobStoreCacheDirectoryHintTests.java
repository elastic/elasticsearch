/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.lucene;

import org.apache.lucene.store.DataAccessHint;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.elasticsearch.blobcache.BlobCacheMetrics;
import org.elasticsearch.blobcache.common.ByteRange;
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
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.xpack.stateless.commits.BlobLocationTestUtils.createBlobFileRanges;
import static org.hamcrest.Matchers.containsString;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class BlobStoreCacheDirectoryHintTests extends ESTestCase {

    private static final int REGION_SIZE = 16 * 1024 * 1024; // 16MB

    // --- BlobStoreCacheDirectory hint pass-through tests ---

    public void testExplicitHintIsPreserved() throws IOException {
        var capturedContext = new AtomicReference<IOContext>();
        var dir = createCapturingDirectory(capturedContext);
        dir.updateMetadata(Map.of("_0.vec", createBlobFileRanges(1L, 0L, 0, 1024)), 1024L);

        dir.openInput("_0.vec", IOContext.DEFAULT.withHints(DataAccessHint.RANDOM));

        assertTrue(capturedContext.get().hints().contains(DataAccessHint.RANDOM));
    }

    // --- contextToAdvice tests ---

    public void testContextToAdviceWithRandomHint() {
        IOContext randomCtx = IOContext.DEFAULT.withHints(DataAccessHint.RANDOM);
        int advice = CacheFileReaderTestUtils.contextToAdvice(randomCtx, true);

        if (CacheFileReaderTestUtils.isMadviseRandomEnabled()) {
            assertEquals(SharedBytes.MADV_RANDOM, advice);
        } else {
            assertEquals(SharedBytes.MADV_NORMAL, advice);
        }
    }

    public void testContextToAdviceWithoutRandomHint() {
        assertEquals(SharedBytes.MADV_NORMAL, CacheFileReaderTestUtils.contextToAdvice(IOContext.DEFAULT, true));
    }

    public void testContextToAdviceWithRandomHintButNoSearchRole() {
        IOContext randomCtx = IOContext.DEFAULT.withHints(DataAccessHint.RANDOM);
        assertEquals(SharedBytes.MADV_NORMAL, CacheFileReaderTestUtils.contextToAdvice(randomCtx, false));
    }

    // --- Top-level file (exclusive blob) tests ---

    // Top-level CacheFileReader has exclusiveRange covering the entire blob.
    @SuppressWarnings("unchecked")
    public void testTopLevelFileHasFullExclusiveRange() {
        var cacheFile = mock(StatelessSharedBlobCacheService.CacheFile.class);
        var reader = new CacheFileReader(
            cacheFile,
            mock(CacheBlobReader.class),
            createBlobFileRanges(1L, 0L, 0, 1024),
            BlobCacheMetrics.NOOP,
            System::currentTimeMillis,
            REGION_SIZE,
            IOContext.DEFAULT.withHints(DataAccessHint.RANDOM),
            true
        );

        assertEquals(0, CacheFileReaderTestUtils.getExclusiveStart(reader));
        assertEquals(Long.MAX_VALUE, CacheFileReaderTestUtils.getExclusiveEnd(reader));
    }

    // copy() preserves the desired advice and exclusive range.
    @SuppressWarnings("unchecked")
    public void testCopyPreservesAdviceAndExclusiveRange() {
        var cacheFile = mock(StatelessSharedBlobCacheService.CacheFile.class);
        when(cacheFile.copy()).thenReturn(cacheFile);

        var original = new CacheFileReader(
            cacheFile,
            mock(CacheBlobReader.class),
            createBlobFileRanges(1L, 0L, 0, 1024),
            BlobCacheMetrics.NOOP,
            System::currentTimeMillis,
            REGION_SIZE,
            IOContext.DEFAULT.withHints(DataAccessHint.RANDOM),
            true
        );
        var copy = original.copy();

        assertNotSame(original, copy);
        assertEquals(CacheFileReaderTestUtils.getDesiredAdvice(original), CacheFileReaderTestUtils.getDesiredAdvice(copy));
        assertEquals(CacheFileReaderTestUtils.getExclusiveStart(original), CacheFileReaderTestUtils.getExclusiveStart(copy));
        assertEquals(CacheFileReaderTestUtils.getExclusiveEnd(original), CacheFileReaderTestUtils.getExclusiveEnd(copy));
    }

    // --- Compound file exclusive region tests ---

    // A large .vec sub-file spanning many regions has interior exclusive regions.
    @SuppressWarnings("unchecked")
    public void testCompoundSliceLargeVecHasExclusiveInterior() {
        var cacheFile = mock(StatelessSharedBlobCacheService.CacheFile.class);
        when(cacheFile.copy()).thenReturn(cacheFile);

        var original = new CacheFileReader(
            cacheFile,
            mock(CacheBlobReader.class),
            createBlobFileRanges(1L, 0L, 0, 500 * 1024 * 1024),
            BlobCacheMetrics.NOOP,
            System::currentTimeMillis,
            REGION_SIZE,
            IOContext.DEFAULT,
            true
        );

        // .vec at offset 10MB, length 500MB - 20MB = 480MB within the compound blob
        long subFileOffset = 10 * 1024 * 1024L;
        long subFileLength = 480 * 1024 * 1024L;
        var slice = original.copyWithContext(IOContext.DEFAULT.withHints(DataAccessHint.RANDOM), subFileOffset, subFileLength);

        if (CacheFileReaderTestUtils.isMadviseRandomEnabled()) {
            assertEquals(SharedBytes.MADV_RANDOM, CacheFileReaderTestUtils.getDesiredAdvice(slice));
            // exclusiveStart = ceil(10MB / 16MB) * 16MB = 16MB
            assertEquals(REGION_SIZE, CacheFileReaderTestUtils.getExclusiveStart(slice));
            // exclusiveEnd = floor((10MB + 480MB) / 16MB) * 16MB = floor(490MB / 16MB) * 16MB
            long expectedEnd = ((subFileOffset + subFileLength) / REGION_SIZE) * REGION_SIZE;
            assertEquals(expectedEnd, CacheFileReaderTestUtils.getExclusiveEnd(slice));
            assertTrue(CacheFileReaderTestUtils.getExclusiveStart(slice) < CacheFileReaderTestUtils.getExclusiveEnd(slice));
        } else {
            assertEquals(SharedBytes.MADV_NORMAL, CacheFileReaderTestUtils.getDesiredAdvice(slice));
        }
    }

    // A small sub-file that fits within a single region has no exclusive regions.
    @SuppressWarnings("unchecked")
    public void testCompoundSliceSmallFileNoExclusiveRegions() {
        var cacheFile = mock(StatelessSharedBlobCacheService.CacheFile.class);
        when(cacheFile.copy()).thenReturn(cacheFile);

        var original = new CacheFileReader(
            cacheFile,
            mock(CacheBlobReader.class),
            createBlobFileRanges(1L, 0L, 0, 100 * 1024 * 1024),
            BlobCacheMetrics.NOOP,
            System::currentTimeMillis,
            REGION_SIZE,
            IOContext.DEFAULT,
            true
        );

        // .vec at offset 5MB, length 8MB — fits within one region boundary, no exclusive regions
        long subFileOffset = 5 * 1024 * 1024L;
        long subFileLength = 8 * 1024 * 1024L;
        var slice = original.copyWithContext(IOContext.DEFAULT.withHints(DataAccessHint.RANDOM), subFileOffset, subFileLength);

        if (CacheFileReaderTestUtils.isMadviseRandomEnabled()) {
            assertEquals(SharedBytes.MADV_RANDOM, CacheFileReaderTestUtils.getDesiredAdvice(slice));
            // exclusiveStart = ceil(5MB / 16MB) * 16MB = 16MB
            // exclusiveEnd = floor(13MB / 16MB) * 16MB = 0
            // exclusiveStart > exclusiveEnd → no exclusive regions
            assertTrue(CacheFileReaderTestUtils.getExclusiveStart(slice) >= CacheFileReaderTestUtils.getExclusiveEnd(slice));
        }
    }

    // A sub-file perfectly aligned to region boundaries is fully exclusive.
    @SuppressWarnings("unchecked")
    public void testCompoundSliceAlignedFileFullyExclusive() {
        var cacheFile = mock(StatelessSharedBlobCacheService.CacheFile.class);
        when(cacheFile.copy()).thenReturn(cacheFile);

        var original = new CacheFileReader(
            cacheFile,
            mock(CacheBlobReader.class),
            createBlobFileRanges(1L, 0L, 0, 100 * 1024 * 1024),
            BlobCacheMetrics.NOOP,
            System::currentTimeMillis,
            REGION_SIZE,
            IOContext.DEFAULT,
            true
        );

        // .vec at region boundary, length = 3 regions
        long subFileOffset = 2L * REGION_SIZE;
        long subFileLength = 3L * REGION_SIZE;
        var slice = original.copyWithContext(IOContext.DEFAULT.withHints(DataAccessHint.RANDOM), subFileOffset, subFileLength);

        if (CacheFileReaderTestUtils.isMadviseRandomEnabled()) {
            assertEquals(subFileOffset, CacheFileReaderTestUtils.getExclusiveStart(slice));
            assertEquals(subFileOffset + subFileLength, CacheFileReaderTestUtils.getExclusiveEnd(slice));
        }
    }

    // --- adviceForRange tests ---

    @SuppressWarnings("unchecked")
    public void testAdviceForRangeInsideExclusiveRegion() {
        var cacheFile = mock(StatelessSharedBlobCacheService.CacheFile.class);
        when(cacheFile.copy()).thenReturn(cacheFile);

        var original = new CacheFileReader(
            cacheFile,
            mock(CacheBlobReader.class),
            createBlobFileRanges(1L, 0L, 0, 500 * 1024 * 1024),
            BlobCacheMetrics.NOOP,
            System::currentTimeMillis,
            REGION_SIZE,
            IOContext.DEFAULT,
            true
        );

        // .vec starts at 10MB, 480MB long → exclusive range [16MB, 480MB)
        long subFileOffset = 10 * 1024 * 1024L;
        long subFileLength = 480 * 1024 * 1024L;
        var slice = original.copyWithContext(IOContext.DEFAULT.withHints(DataAccessHint.RANDOM), subFileOffset, subFileLength);

        if (CacheFileReaderTestUtils.isMadviseRandomEnabled()) {
            // Range fully inside exclusive region → MADV_RANDOM
            ByteRange insideRange = ByteRange.of(REGION_SIZE, 2L * REGION_SIZE);
            assertEquals(SharedBytes.MADV_RANDOM, CacheFileReaderTestUtils.adviceForRange(slice, insideRange));

            // Range at the boundary start (before exclusive region) → MADV_NORMAL
            ByteRange boundaryRange = ByteRange.of(0, REGION_SIZE);
            assertEquals(SharedBytes.MADV_NORMAL, CacheFileReaderTestUtils.adviceForRange(slice, boundaryRange));

            // Range spanning the exclusive start boundary → MADV_NORMAL
            ByteRange straddleRange = ByteRange.of(REGION_SIZE / 2, REGION_SIZE + REGION_SIZE / 2);
            assertEquals(SharedBytes.MADV_NORMAL, CacheFileReaderTestUtils.adviceForRange(slice, straddleRange));
        }
    }

    // For a top-level file, adviceForRange returns MADV_RANDOM for any range.
    @SuppressWarnings("unchecked")
    public void testAdviceForRangeTopLevelFile() {
        var cacheFile = mock(StatelessSharedBlobCacheService.CacheFile.class);

        var reader = new CacheFileReader(
            cacheFile,
            mock(CacheBlobReader.class),
            createBlobFileRanges(1L, 0L, 0, 500 * 1024 * 1024),
            BlobCacheMetrics.NOOP,
            System::currentTimeMillis,
            REGION_SIZE,
            IOContext.DEFAULT.withHints(DataAccessHint.RANDOM),
            true
        );

        if (CacheFileReaderTestUtils.isMadviseRandomEnabled()) {
            assertEquals(SharedBytes.MADV_RANDOM, CacheFileReaderTestUtils.adviceForRange(reader, ByteRange.of(0, REGION_SIZE)));
            assertEquals(
                SharedBytes.MADV_RANDOM,
                CacheFileReaderTestUtils.adviceForRange(reader, ByteRange.of(100L * REGION_SIZE, 101L * REGION_SIZE))
            );
        }
    }

    // adviceForRange returns MADV_NORMAL for a range straddling the exclusive end boundary.
    @SuppressWarnings("unchecked")
    public void testAdviceForRangeAtExclusiveEndBoundary() {
        var cacheFile = mock(StatelessSharedBlobCacheService.CacheFile.class);
        when(cacheFile.copy()).thenReturn(cacheFile);

        var original = new CacheFileReader(
            cacheFile,
            mock(CacheBlobReader.class),
            createBlobFileRanges(1L, 0L, 0, 500 * 1024 * 1024),
            BlobCacheMetrics.NOOP,
            System::currentTimeMillis,
            REGION_SIZE,
            IOContext.DEFAULT,
            true
        );

        long subFileOffset = 10 * 1024 * 1024L;
        long subFileLength = 480 * 1024 * 1024L;
        var slice = original.copyWithContext(IOContext.DEFAULT.withHints(DataAccessHint.RANDOM), subFileOffset, subFileLength);

        if (CacheFileReaderTestUtils.isMadviseRandomEnabled()) {
            long exclEnd = CacheFileReaderTestUtils.getExclusiveEnd(slice);
            // Range that straddles the exclusive end boundary → MADV_NORMAL
            ByteRange tailStraddle = ByteRange.of(exclEnd - REGION_SIZE / 2, exclEnd + REGION_SIZE / 2);
            assertEquals(SharedBytes.MADV_NORMAL, CacheFileReaderTestUtils.adviceForRange(slice, tailStraddle));

            // Range just inside the exclusive end → MADV_RANDOM
            ByteRange justInside = ByteRange.of(exclEnd - REGION_SIZE, exclEnd);
            assertEquals(SharedBytes.MADV_RANDOM, CacheFileReaderTestUtils.adviceForRange(slice, justInside));

            // Range just outside the exclusive end → MADV_NORMAL
            ByteRange justOutside = ByteRange.of(exclEnd, exclEnd + REGION_SIZE);
            assertEquals(SharedBytes.MADV_NORMAL, CacheFileReaderTestUtils.adviceForRange(slice, justOutside));
        }
    }

    // adviceForRange always returns MADV_NORMAL when the desired advice is MADV_NORMAL.
    @SuppressWarnings("unchecked")
    public void testAdviceForRangeWithNormalDesiredAdvice() {
        var cacheFile = mock(StatelessSharedBlobCacheService.CacheFile.class);
        when(cacheFile.copy()).thenReturn(cacheFile);

        var original = new CacheFileReader(
            cacheFile,
            mock(CacheBlobReader.class),
            createBlobFileRanges(1L, 0L, 0, 500 * 1024 * 1024),
            BlobCacheMetrics.NOOP,
            System::currentTimeMillis,
            REGION_SIZE,
            IOContext.DEFAULT,
            false
        );
        var slice = original.copyWithContext(IOContext.DEFAULT, 0, 500 * 1024 * 1024L);
        assertEquals(SharedBytes.MADV_NORMAL, CacheFileReaderTestUtils.adviceForRange(slice, ByteRange.of(0, REGION_SIZE)));
    }

    // --- BlobCacheIndexInput slice tests ---

    // 4-arg slice computes exclusive range for the sub-file.
    @SuppressWarnings("unchecked")
    public void testFourArgSliceComputesExclusiveRange() {
        var cacheFile = mock(StatelessSharedBlobCacheService.CacheFile.class);
        when(cacheFile.copy()).thenReturn(cacheFile);

        var reader = new CacheFileReader(
            cacheFile,
            mock(CacheBlobReader.class),
            createBlobFileRanges(1L, 0L, 0, 500 * 1024 * 1024),
            BlobCacheMetrics.NOOP,
            System::currentTimeMillis,
            REGION_SIZE,
            IOContext.DEFAULT,
            true
        );
        var indexInput = new BlobCacheIndexInput("test.cfs", IOContext.DEFAULT, reader, null, 500 * 1024 * 1024, 0);
        IOContext randomCtx = IOContext.DEFAULT.withHints(DataAccessHint.RANDOM);
        // .vec at offset 10MB, length 100MB
        long sliceOffset = 10 * 1024 * 1024L;
        long sliceLength = 100 * 1024 * 1024L;
        var slice = (BlobCacheIndexInput) indexInput.slice("_0.vec", sliceOffset, sliceLength, randomCtx);

        if (CacheFileReaderTestUtils.isMadviseRandomEnabled()) {
            assertEquals(SharedBytes.MADV_RANDOM, BlobStoreCacheDirectoryTestUtils.getDesiredAdvice(slice));
            assertEquals(REGION_SIZE, BlobStoreCacheDirectoryTestUtils.getExclusiveStart(slice));
            long expectedEnd = ((sliceOffset + sliceLength) / REGION_SIZE) * REGION_SIZE;
            assertEquals(expectedEnd, BlobStoreCacheDirectoryTestUtils.getExclusiveEnd(slice));
        } else {
            assertEquals(SharedBytes.MADV_NORMAL, BlobStoreCacheDirectoryTestUtils.getDesiredAdvice(slice));
        }
    }

    // 4-arg slice correctly accumulates offset.
    @SuppressWarnings("unchecked")
    public void testSlicePreservesOffset() {
        var cacheFile = mock(StatelessSharedBlobCacheService.CacheFile.class);
        when(cacheFile.copy()).thenReturn(cacheFile);

        var reader = new CacheFileReader(
            cacheFile,
            mock(CacheBlobReader.class),
            createBlobFileRanges(1L, 0L, 0, 2048),
            BlobCacheMetrics.NOOP,
            System::currentTimeMillis,
            REGION_SIZE,
            IOContext.DEFAULT,
            false
        );
        long parentOffset = 100;
        var indexInput = new BlobCacheIndexInput("test.cfs", IOContext.DEFAULT, reader, null, 2048, parentOffset);
        long sliceOffset = 200;
        long sliceLength = 512;
        var slice = (BlobCacheIndexInput) indexInput.slice("_0.vec", sliceOffset, sliceLength, IOContext.DEFAULT);
        assertEquals(sliceLength, slice.length());
        assertEquals(0, slice.getFilePointer());
        assertEquals("_0.vec", slice.getSliceDescription());
        assertThat(slice.toString(), containsString("offset=" + (parentOffset + sliceOffset)));
    }

    // 3-arg slice preserves parent's advice and exclusive range.
    @SuppressWarnings("unchecked")
    public void testThreeArgSlicePreservesParentState() {
        var cacheFile = mock(StatelessSharedBlobCacheService.CacheFile.class);
        when(cacheFile.copy()).thenReturn(cacheFile);

        var reader = new CacheFileReader(
            cacheFile,
            mock(CacheBlobReader.class),
            createBlobFileRanges(1L, 0L, 0, 1024),
            BlobCacheMetrics.NOOP,
            System::currentTimeMillis,
            REGION_SIZE,
            IOContext.DEFAULT.withHints(DataAccessHint.RANDOM),
            true
        );
        var indexInput = new BlobCacheIndexInput("test.vec", IOContext.DEFAULT, reader, null, 1024, 0);
        var slice = (BlobCacheIndexInput) indexInput.doSlice("sub", 0, 512);
        assertEquals(CacheFileReaderTestUtils.getDesiredAdvice(reader), BlobStoreCacheDirectoryTestUtils.getDesiredAdvice(slice));
        assertEquals(CacheFileReaderTestUtils.getExclusiveStart(reader), BlobStoreCacheDirectoryTestUtils.getExclusiveStart(slice));
        assertEquals(CacheFileReaderTestUtils.getExclusiveEnd(reader), BlobStoreCacheDirectoryTestUtils.getExclusiveEnd(slice));
    }

    // 3-arg slice on a MADV_NORMAL parent stays MADV_NORMAL.
    @SuppressWarnings("unchecked")
    public void testThreeArgSlicePreservesNormalAdvice() {
        var cacheFile = mock(StatelessSharedBlobCacheService.CacheFile.class);
        when(cacheFile.copy()).thenReturn(cacheFile);

        var reader = new CacheFileReader(
            cacheFile,
            mock(CacheBlobReader.class),
            createBlobFileRanges(1L, 0L, 0, 1024),
            BlobCacheMetrics.NOOP,
            System::currentTimeMillis
        );
        var indexInput = new BlobCacheIndexInput("test.cfs", IOContext.DEFAULT, reader, null, 1024, 0);
        var slice = (BlobCacheIndexInput) indexInput.doSlice("_0.doc", 0, 512);
        assertEquals(SharedBytes.MADV_NORMAL, BlobStoreCacheDirectoryTestUtils.getDesiredAdvice(slice));
    }

    // --- Rounding utility tests ---

    public void testRoundUpToRegion() {
        assertEquals(0, CacheFileReaderTestUtils.roundUpToRegion(0, REGION_SIZE));
        assertEquals(REGION_SIZE, CacheFileReaderTestUtils.roundUpToRegion(1, REGION_SIZE));
        assertEquals(REGION_SIZE, CacheFileReaderTestUtils.roundUpToRegion(REGION_SIZE, REGION_SIZE));
        assertEquals(2L * REGION_SIZE, CacheFileReaderTestUtils.roundUpToRegion(REGION_SIZE + 1, REGION_SIZE));
    }

    public void testRoundDownToRegion() {
        assertEquals(0, CacheFileReaderTestUtils.roundDownToRegion(0, REGION_SIZE));
        assertEquals(0, CacheFileReaderTestUtils.roundDownToRegion(REGION_SIZE - 1, REGION_SIZE));
        assertEquals(REGION_SIZE, CacheFileReaderTestUtils.roundDownToRegion(REGION_SIZE, REGION_SIZE));
        assertEquals(REGION_SIZE, CacheFileReaderTestUtils.roundDownToRegion(2L * REGION_SIZE - 1, REGION_SIZE));
    }

    // The 6-arg constructor sets regionSize=0 and desiredMAdvice=MADV_NORMAL.
    // tryRead must not divide by regionSize in this case.
    @SuppressWarnings("unchecked")
    public void testTryReadWith6ArgConstructorDoesNotThrow() throws IOException {
        var cacheFile = mock(StatelessSharedBlobCacheService.CacheFile.class);
        when(cacheFile.tryRead(any(ByteBuffer.class), anyLong())).thenReturn(true);

        var reader = new CacheFileReader(
            cacheFile,
            mock(CacheBlobReader.class),
            createBlobFileRanges(1L, 0L, 0, 1024),
            BlobCacheMetrics.NOOP,
            System::currentTimeMillis
        );

        ByteBuffer buf = ByteBuffer.allocate(10);
        assertTrue(reader.tryRead(buf, 0));
        verify(cacheFile).tryRead(buf, 0);
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
