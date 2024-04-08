/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.searchablesnapshots.cache.common;

import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.Constants;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.blobcache.BlobCacheTestUtils;
import org.elasticsearch.blobcache.common.ByteRange;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.filesystem.FileSystemNatives;
import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.core.PathUtilsForTesting;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.searchablesnapshots.cache.common.CacheFile.EvictionListener;
import org.hamcrest.Matcher;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.Set;
import java.util.SortedSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static org.elasticsearch.xpack.searchablesnapshots.cache.common.TestUtils.randomPopulateAndReads;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

@LuceneTestCase.SuppressFileSystems("DisableFsyncFS") // required by {@link testCacheFileCreatedAsSparseFile()}
public class CacheFileTests extends ESTestCase {

    private static final CacheFile.ModificationListener NOOP = new CacheFile.ModificationListener() {
        @Override
        public void onCacheFileNeedsFsync(CacheFile cacheFile) {}

        @Override
        public void onCacheFileDelete(CacheFile cacheFile) {}
    };

    private static final CacheKey CACHE_KEY = new CacheKey("_snap_uuid", "_snap_index", new ShardId("_name", "_uuid", 0), "_filename");

    public void testGetCacheKey() {
        final Path file = createTempDir().resolve("file.new");
        final CacheKey cacheKey = new CacheKey(
            UUIDs.randomBase64UUID(random()),
            randomAlphaOfLength(5).toLowerCase(Locale.ROOT),
            new ShardId(randomAlphaOfLength(5).toLowerCase(Locale.ROOT), UUIDs.randomBase64UUID(random()), randomInt(5)),
            randomAlphaOfLength(105).toLowerCase(Locale.ROOT)
        );

        final CacheFile cacheFile = new CacheFile(cacheKey, randomLongBetween(1, 100), file, NOOP);
        assertThat(cacheFile.getCacheKey(), sameInstance(cacheKey));
    }

    public void testAcquireAndRelease() throws Exception {
        final Path file = createTempDir().resolve("file.cache");
        final TestCacheFileModificationListener updatesListener = new TestCacheFileModificationListener();
        final CacheFile cacheFile = new CacheFile(CACHE_KEY, randomLongBetween(1, 100), file, updatesListener);
        assertFalse(updatesListener.containsDelete(cacheFile));

        assertThat("Cache file is not acquired: no channel exists", cacheFile.getChannel(), nullValue());
        assertThat("Cache file is not acquired: file does not exist", Files.exists(file), is(false));

        final TestEvictionListener listener = new TestEvictionListener();
        cacheFile.acquire(listener);
        assertThat("Cache file has been acquired: file should exists", Files.exists(file), is(true));
        assertThat("Cache file has been acquired: channel should exists", cacheFile.getChannel(), notNullValue());
        assertThat("Cache file has been acquired: channel is open", cacheFile.getChannel().isOpen(), is(true));
        assertThat("Cache file has been acquired: eviction listener is not executed", listener.isCalled(), is(false));

        cacheFile.release(listener);
        assertThat("Cache file has been released: eviction listener is not executed", listener.isCalled(), is(false));
        assertThat("Cache file has been released: channel does not exist", cacheFile.getChannel(), nullValue());
        assertThat("Cache file is not evicted: file still exists after release", Files.exists(file), is(true));

        cacheFile.acquire(listener);

        FileChannel fileChannel = cacheFile.getChannel();
        assertThat("Channel should exists", fileChannel, notNullValue());
        assertThat("Channel is open", fileChannel.isOpen(), is(true));

        assertThat("Cache file is not evicted: eviction listener is not executed notified", listener.isCalled(), is(false));
        cacheFile.startEviction();

        assertThat("Cache file has been evicted: eviction listener was executed", listener.isCalled(), is(true));
        assertThat("Cache file is evicted but not fully released: file still exists", Files.exists(file), is(true));
        assertThat("Cache file is evicted but not fully released: channel still exists", cacheFile.getChannel(), notNullValue());
        assertThat("Cache file is evicted but not fully released: channel is open", cacheFile.getChannel().isOpen(), is(true));
        assertThat("Channel didn't change after eviction", cacheFile.getChannel(), sameInstance(fileChannel));
        assertFalse(updatesListener.containsDelete(cacheFile));

        cacheFile.release(listener);
        assertThat("Cache file evicted and fully released: channel does not exist", cacheFile.getChannel(), nullValue());
        assertThat("Cache file has been deleted", Files.exists(file), is(false));
        assertTrue(updatesListener.containsDelete(cacheFile));
    }

    public void testCacheFileNotAcquired() throws IOException {
        final Path file = createTempDir().resolve("file.cache");
        final TestCacheFileModificationListener updatesListener = new TestCacheFileModificationListener();
        final CacheFile cacheFile = new CacheFile(CACHE_KEY, randomLongBetween(1, 100), file, updatesListener);

        assertThat(Files.exists(file), is(false));
        assertThat(cacheFile.getChannel(), nullValue());

        if (randomBoolean()) {
            final TestEvictionListener listener = new TestEvictionListener();
            cacheFile.acquire(listener);

            assertThat(cacheFile.getChannel(), notNullValue());
            assertThat(Files.exists(file), is(true));

            cacheFile.release(listener);
        }

        assertFalse(updatesListener.containsUpdate(cacheFile));
        assertFalse(updatesListener.containsDelete(cacheFile));

        cacheFile.startEviction();

        assertThat(cacheFile.getChannel(), nullValue());
        assertFalse(Files.exists(file));
        assertTrue(updatesListener.containsDelete(cacheFile));
    }

    public void testDeleteOnCloseAfterLastRelease() throws Exception {
        final Path file = createTempDir().resolve("file.cache");
        final TestCacheFileModificationListener updatesListener = new TestCacheFileModificationListener();
        final CacheFile cacheFile = new CacheFile(CACHE_KEY, randomLongBetween(1, 100), file, updatesListener);

        final List<TestEvictionListener> acquiredListeners = new ArrayList<>();
        for (int i = 0; i < randomIntBetween(1, 20); i++) {
            TestEvictionListener listener = new TestEvictionListener();
            cacheFile.acquire(listener);
            assertThat(cacheFile.getChannel(), notNullValue());
            acquiredListeners.add(listener);
        }

        final List<TestEvictionListener> releasedListeners = new ArrayList<>();
        for (Iterator<TestEvictionListener> it = acquiredListeners.iterator(); it.hasNext();) {
            if (randomBoolean()) {
                TestEvictionListener listener = it.next();
                releasedListeners.add(listener);
                cacheFile.release(listener);
                it.remove();
            }
        }

        assertTrue(Files.exists(file));
        cacheFile.startEviction();

        releasedListeners.forEach(l -> assertFalse("Released listeners before cache file eviction are not called", l.isCalled()));
        acquiredListeners.forEach(l -> assertTrue("Released listeners after cache file eviction are called", l.isCalled()));
        acquiredListeners.forEach(cacheFile::release);

        assertTrue(updatesListener.containsDelete(cacheFile));
        assertFalse(Files.exists(file));
    }

    public void testConcurrentAccess() throws Exception {
        final Path file = createTempDir().resolve("file.cache");
        final TestCacheFileModificationListener updatesListener = new TestCacheFileModificationListener();
        final CacheFile cacheFile = new CacheFile(CACHE_KEY, randomLongBetween(1, 100), file, updatesListener);

        final TestEvictionListener evictionListener = new TestEvictionListener();
        cacheFile.acquire(evictionListener);
        final long length = cacheFile.getLength();
        final DeterministicTaskQueue deterministicTaskQueue = new DeterministicTaskQueue();
        final ThreadPool threadPool = deterministicTaskQueue.getThreadPool();
        final Future<Integer> populateAndReadFuture;
        final Future<Integer> readIfAvailableFuture;
        if (randomBoolean()) {
            populateAndReadFuture = cacheFile.populateAndRead(
                ByteRange.of(0L, length),
                ByteRange.of(0L, length),
                channel -> Math.toIntExact(length),
                (channel, from, to, progressUpdater) -> progressUpdater.accept(length),
                threadPool.generic()
            );
        } else {
            populateAndReadFuture = null;
        }
        if (randomBoolean()) {
            readIfAvailableFuture = cacheFile.readIfAvailableOrPending(ByteRange.of(0L, length), channel -> Math.toIntExact(length));
        } else {
            readIfAvailableFuture = null;
        }
        final boolean evicted = randomBoolean();
        if (evicted) {
            deterministicTaskQueue.scheduleNow(cacheFile::startEviction);
        }
        deterministicTaskQueue.scheduleNow(() -> cacheFile.release(evictionListener));
        deterministicTaskQueue.runAllRunnableTasks();
        if (populateAndReadFuture != null) {
            try {
                assertTrue(populateAndReadFuture.isDone());
                populateAndReadFuture.get();
                assertTrue(updatesListener.containsUpdate(cacheFile));
            } catch (ExecutionException e) {
                assertThat(e.getCause(), instanceOf(AlreadyClosedException.class));
                assertFalse(updatesListener.containsUpdate(cacheFile));
                assertTrue(updatesListener.containsDelete(cacheFile));
            }
        }
        if (readIfAvailableFuture != null) {
            assertTrue(readIfAvailableFuture.isDone());
        }
        if (evicted) {
            assertFalse(Files.exists(file));
            assertTrue(updatesListener.containsDelete(cacheFile));
        }
    }

    public void testFSync() throws Exception {
        final BlobCacheTestUtils.FSyncTrackingFileSystemProvider fileSystem = setupFSyncCountingFileSystem();
        try {
            final TestCacheFileModificationListener updatesListener = new TestCacheFileModificationListener();
            final CacheFile cacheFile = new CacheFile(CACHE_KEY, randomLongBetween(0L, 1000L), fileSystem.resolve("test"), updatesListener);
            assertFalse(cacheFile.needsFsync());
            assertFalse(updatesListener.containsUpdate(cacheFile));

            final TestEvictionListener listener = new TestEvictionListener();
            cacheFile.acquire(listener);

            try {
                if (randomBoolean()) {
                    final SortedSet<ByteRange> completedRanges = cacheFile.fsync();
                    assertNumberOfFSyncs(cacheFile.getFile(), equalTo(0));
                    assertThat(completedRanges, hasSize(0));
                    assertFalse(cacheFile.needsFsync());
                    assertFalse(updatesListener.containsUpdate(cacheFile));
                }

                final SortedSet<ByteRange> expectedCompletedRanges = randomPopulateAndReads(cacheFile);
                if (expectedCompletedRanges.isEmpty() == false) {
                    assertTrue(cacheFile.needsFsync());
                    assertTrue(updatesListener.containsUpdate(cacheFile));
                    updatesListener.reset();
                } else {
                    assertFalse(cacheFile.needsFsync());
                    assertFalse(updatesListener.containsUpdate(cacheFile));
                }

                final SortedSet<ByteRange> completedRanges = cacheFile.fsync();
                assertNumberOfFSyncs(cacheFile.getFile(), equalTo(expectedCompletedRanges.isEmpty() ? 0 : 1));
                assertArrayEquals(completedRanges.toArray(ByteRange[]::new), expectedCompletedRanges.toArray(ByteRange[]::new));
                assertFalse(cacheFile.needsFsync());
                assertFalse(updatesListener.containsUpdate(cacheFile));
            } finally {
                cacheFile.release(listener);
            }
        } finally {
            fileSystem.tearDown();
        }
    }

    public void testFSyncOnEvictedFile() throws Exception {
        final BlobCacheTestUtils.FSyncTrackingFileSystemProvider fileSystem = setupFSyncCountingFileSystem();
        try {
            final TestCacheFileModificationListener updatesListener = new TestCacheFileModificationListener();
            final CacheFile cacheFile = new CacheFile(CACHE_KEY, randomLongBetween(0L, 1000L), fileSystem.resolve("test"), updatesListener);
            assertFalse(cacheFile.needsFsync());
            assertFalse(updatesListener.containsUpdate(cacheFile));
            assertFalse(updatesListener.containsDelete(cacheFile));

            final TestEvictionListener listener = new TestEvictionListener();
            cacheFile.acquire(listener);

            boolean released = false;
            try {
                final SortedSet<ByteRange> expectedCompletedRanges = randomPopulateAndReads(cacheFile);
                if (expectedCompletedRanges.isEmpty() == false) {
                    assertTrue(cacheFile.needsFsync());
                    assertTrue(updatesListener.containsUpdate(cacheFile));
                    updatesListener.reset();

                    final SortedSet<ByteRange> completedRanges = cacheFile.fsync();
                    assertArrayEquals(completedRanges.toArray(ByteRange[]::new), expectedCompletedRanges.toArray(ByteRange[]::new));
                    assertNumberOfFSyncs(cacheFile.getFile(), equalTo(1));
                }
                assertFalse(cacheFile.needsFsync());
                assertFalse(updatesListener.containsUpdate(cacheFile));
                updatesListener.reset();

                cacheFile.startEviction();
                assertFalse(updatesListener.containsDelete(cacheFile));

                if (rarely()) {
                    assertThat("New ranges should not be written after cache file eviction", randomPopulateAndReads(cacheFile), hasSize(0));
                }
                if (randomBoolean()) {
                    cacheFile.release(listener);
                    assertTrue(updatesListener.containsDelete(cacheFile));
                    released = true;
                }
                updatesListener.reset();

                final SortedSet<ByteRange> completedRangesAfterEviction = cacheFile.fsync();
                assertNumberOfFSyncs(cacheFile.getFile(), equalTo(expectedCompletedRanges.isEmpty() ? 0 : 1));
                assertThat(completedRangesAfterEviction, hasSize(0));
                assertFalse(cacheFile.needsFsync());
                assertFalse(updatesListener.containsUpdate(cacheFile));
                updatesListener.reset();
            } finally {
                if (released == false) {
                    cacheFile.release(listener);
                    assertTrue(updatesListener.containsDelete(cacheFile));
                }
            }
        } finally {
            fileSystem.tearDown();
        }
    }

    public void testFSyncFailure() throws Exception {
        final BlobCacheTestUtils.FSyncTrackingFileSystemProvider fileSystem = setupFSyncCountingFileSystem();
        try {
            fileSystem.failFSyncs(true);

            final TestCacheFileModificationListener updatesListener = new TestCacheFileModificationListener();
            final CacheFile cacheFile = new CacheFile(CACHE_KEY, randomLongBetween(0L, 1000L), fileSystem.resolve("test"), updatesListener);
            assertFalse(cacheFile.needsFsync());
            assertFalse(updatesListener.containsUpdate(cacheFile));
            assertFalse(updatesListener.containsDelete(cacheFile));

            final TestEvictionListener listener = new TestEvictionListener();
            cacheFile.acquire(listener);

            try {
                final SortedSet<ByteRange> expectedCompletedRanges = randomPopulateAndReads(cacheFile);
                if (expectedCompletedRanges.isEmpty() == false) {
                    assertTrue(cacheFile.needsFsync());
                    assertTrue(updatesListener.containsUpdate(cacheFile));
                    updatesListener.reset();

                    IOException exception = expectThrows(IOException.class, cacheFile::fsync);
                    assertThat(exception.getMessage(), containsString("simulated"));
                    assertTrue(cacheFile.needsFsync());
                    assertTrue(updatesListener.containsUpdate(cacheFile));
                    updatesListener.reset();
                } else {
                    assertFalse(cacheFile.needsFsync());
                    final SortedSet<ByteRange> completedRanges = cacheFile.fsync();
                    assertTrue(completedRanges.isEmpty());
                }
                assertNumberOfFSyncs(cacheFile.getFile(), equalTo(0));

                fileSystem.failFSyncs(false);

                final SortedSet<ByteRange> completedRanges = cacheFile.fsync();
                assertArrayEquals(completedRanges.toArray(ByteRange[]::new), expectedCompletedRanges.toArray(ByteRange[]::new));
                assertNumberOfFSyncs(cacheFile.getFile(), equalTo(expectedCompletedRanges.isEmpty() ? 0 : 1));
                assertFalse(cacheFile.needsFsync());
                assertFalse(updatesListener.containsUpdate(cacheFile));
                assertFalse(updatesListener.containsDelete(cacheFile));
                updatesListener.reset();
            } finally {
                cacheFile.release(listener);
            }
        } finally {
            fileSystem.tearDown();
        }
    }

    private static void assumeLinux64bitsOrWindows() {
        assumeTrue(
            "This test uses native methods implemented only for Windows & Linux 64bits",
            Constants.WINDOWS || Constants.LINUX && Constants.JRE_IS_64BIT
        );
    }

    public void testCacheFileCreatedAsSparseFile() throws Exception {
        assumeLinux64bitsOrWindows();
        final long fourKb = 4096L;
        final long oneMb = 1 << 20;

        final Path file = createTempDir().resolve(UUIDs.randomBase64UUID(random()));
        final CacheFile cacheFile = new CacheFile(
            new CacheKey("_snap_uuid", "_snap_name", new ShardId("_name", "_uid", 0), "_filename"),
            randomLongBetween(fourKb, oneMb),
            file,
            NOOP
        );
        assertFalse(Files.exists(file));

        final TestEvictionListener listener = new TestEvictionListener();
        cacheFile.acquire(listener);
        try {
            final FileChannel fileChannel = cacheFile.getChannel();
            assertTrue(Files.exists(file));

            OptionalLong sizeOnDisk = FileSystemNatives.allocatedSizeInBytes(file);
            assertTrue(sizeOnDisk.isPresent());
            assertThat(sizeOnDisk.getAsLong(), equalTo(0L));

            // write 1 byte at the last position in the cache file.
            // For non sparse files, Windows would allocate the full file on disk in order to write a single byte at the end,
            // making the next assertion fails.
            fill(fileChannel, Math.toIntExact(cacheFile.getLength() - 1L), Math.toIntExact(cacheFile.getLength()));
            fileChannel.force(false);

            sizeOnDisk = FileSystemNatives.allocatedSizeInBytes(file);
            assertTrue(sizeOnDisk.isPresent());
            assertThat(
                "Cache file should be sparse and not fully allocated on disk",
                sizeOnDisk.getAsLong(),
                allOf(greaterThan(0L), lessThan(oneMb))
            );

            // on Linux we can infer the filesystem's block size if only 1 byte was written,
            // but this is not always right with encryption at rest using dmcrypt
            final long blockSize = Constants.LINUX && noEncryptionAtRest(file) ? sizeOnDisk.getAsLong() : 0L;

            fill(fileChannel, 0, Math.toIntExact(cacheFile.getLength()));
            fileChannel.force(false);

            sizeOnDisk = FileSystemNatives.allocatedSizeInBytes(file);
            assertTrue(sizeOnDisk.isPresent());
            assertThat(
                "Cache file should be fully allocated on disk (maybe more given cluster/block size)",
                sizeOnDisk.getAsLong(),
                greaterThanOrEqualTo(cacheFile.getLength())
            );

            if (blockSize > 0L) {
                final long nbBlocks = (cacheFile.getLength() + blockSize - 1) / blockSize; // ceil(cacheFile.getLength() / blockSize)
                final long expectedSize = nbBlocks * blockSize;
                assertThat(
                    "Cache file size mismatches (block size: "
                        + blockSize
                        + ", number of blocks: "
                        + nbBlocks
                        + ", file length: "
                        + cacheFile.getLength()
                        + ')',
                    sizeOnDisk.getAsLong(),
                    equalTo(expectedSize)
                );
            }
        } finally {
            cacheFile.release(listener);
        }
    }

    /**
     * @return true when no encryption at rest is in use (best effort method)
     */
    private static boolean noEncryptionAtRest(Path path) {
        return path.toAbsolutePath().toString().contains("/mnt/secret") == false;
    }

    static class TestEvictionListener implements EvictionListener {

        private final SetOnce<CacheFile> evicted = new SetOnce<>();

        CacheFile getEvictedCacheFile() {
            return evicted.get();
        }

        boolean isCalled() {
            return getEvictedCacheFile() != null;
        }

        @Override
        public void onEviction(CacheFile evictedCacheFile) {
            evicted.set(Objects.requireNonNull(evictedCacheFile));
        }
    }

    private static class TestCacheFileModificationListener implements CacheFile.ModificationListener {

        private final Set<CacheFile> updates = new HashSet<>();
        private final Set<CacheFile> deletes = new HashSet<>();

        @Override
        public synchronized void onCacheFileNeedsFsync(CacheFile cacheFile) {
            assertTrue(updates.add(cacheFile));
        }

        synchronized boolean containsUpdate(CacheFile cacheFile) {
            return updates.contains(cacheFile);
        }

        @Override
        public synchronized void onCacheFileDelete(CacheFile cacheFile) {
            assertTrue(deletes.add(cacheFile));
        }

        synchronized boolean containsDelete(CacheFile cacheFile) {
            return deletes.contains(cacheFile);
        }

        synchronized void reset() {
            updates.clear();
            deletes.clear();
        }
    }

    public static void assertNumberOfFSyncs(final Path path, final Matcher<Integer> matcher) {
        final BlobCacheTestUtils.FSyncTrackingFileSystemProvider provider = (BlobCacheTestUtils.FSyncTrackingFileSystemProvider) path
            .getFileSystem()
            .provider();
        final Integer fsyncCount = provider.getNumberOfFSyncs(path);
        assertThat("File [" + path + "] was never fsynced", fsyncCount, notNullValue());
        assertThat("Mismatching number of fsync for [" + path + "]", fsyncCount, matcher);
    }

    private static BlobCacheTestUtils.FSyncTrackingFileSystemProvider setupFSyncCountingFileSystem() {
        final FileSystem defaultFileSystem = PathUtils.getDefaultFileSystem();
        final BlobCacheTestUtils.FSyncTrackingFileSystemProvider provider = new BlobCacheTestUtils.FSyncTrackingFileSystemProvider(
            defaultFileSystem,
            createTempDir()
        );
        PathUtilsForTesting.installMock(provider.getFileSystem(null));
        return provider;
    }

    private static void fill(FileChannel fileChannel, int from, int to) {
        final byte[] buffer = new byte[Math.min(Math.max(0, to - from), 1024)];
        Arrays.fill(buffer, (byte) 0xff);
        assert fileChannel.isOpen();

        try {
            int written = 0;
            int remaining = to - from;
            while (remaining > 0) {
                final int len = Math.min(remaining, buffer.length);
                fileChannel.write(ByteBuffer.wrap(buffer, 0, len), from + written);
                remaining -= len;
                written += len;
            }
            assert written == to - from;
        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }
}
