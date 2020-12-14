/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.index.store.cache;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.cluster.coordination.DeterministicTaskQueue;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.io.PathUtils;
import org.elasticsearch.common.io.PathUtilsForTesting;
import org.elasticsearch.common.util.concurrent.RunOnce;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.cache.CacheFile.EvictionListener;
import org.elasticsearch.index.store.cache.TestUtils.FSyncTrackingFileSystemProvider;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.hamcrest.Matcher;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.SortedSet;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.common.settings.Settings.builder;
import static org.elasticsearch.index.store.cache.TestUtils.randomPopulateAndReads;
import static org.elasticsearch.node.Node.NODE_NAME_SETTING;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

public class CacheFileTests extends ESTestCase {

    private static final Runnable NOOP = () -> {};
    private static final CacheKey CACHE_KEY = new CacheKey(
        new SnapshotId("_name", "_uuid"),
        new IndexId("_name", "_uuid"),
        new ShardId("_name", "_uuid", 0),
        "_filename"
    );

    public void testGetCacheKey() throws Exception {
        final CacheKey cacheKey = new CacheKey(
            new SnapshotId(randomAlphaOfLength(5).toLowerCase(Locale.ROOT), UUIDs.randomBase64UUID(random())),
            new IndexId(randomAlphaOfLength(5).toLowerCase(Locale.ROOT), UUIDs.randomBase64UUID(random())),
            new ShardId(randomAlphaOfLength(5).toLowerCase(Locale.ROOT), UUIDs.randomBase64UUID(random()), randomInt(5)),
            randomAlphaOfLength(105).toLowerCase(Locale.ROOT)
        );

        final CacheFile cacheFile = new CacheFile(cacheKey, randomLongBetween(1, 100), createTempFile(), NOOP);
        assertThat(cacheFile.getCacheKey(), sameInstance(cacheKey));
    }

    public void testAcquireAndRelease() throws Exception {
        final Path file = createTempDir().resolve("file.cache");
        final CacheFile cacheFile = new CacheFile(CACHE_KEY, randomLongBetween(1, 100), file, NOOP);

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

        cacheFile.release(listener);
        assertThat("Cache file evicted and fully released: channel does not exist", cacheFile.getChannel(), nullValue());
        assertThat("Cache file has been deleted", Files.exists(file), is(false));
    }

    public void testCacheFileNotAcquired() throws IOException {
        final Path file = createTempDir().resolve("file.cache");
        final CacheFile cacheFile = new CacheFile(CACHE_KEY, randomLongBetween(1, 100), file, NOOP);

        assertThat(Files.exists(file), is(false));
        assertThat(cacheFile.getChannel(), nullValue());

        if (randomBoolean()) {
            final TestEvictionListener listener = new TestEvictionListener();
            cacheFile.acquire(listener);

            assertThat(cacheFile.getChannel(), notNullValue());
            assertThat(Files.exists(file), is(true));

            cacheFile.release(listener);
        }

        cacheFile.startEviction();
        assertThat(cacheFile.getChannel(), nullValue());
        assertFalse(Files.exists(file));
    }

    public void testDeleteOnCloseAfterLastRelease() throws Exception {
        final Path file = createTempDir().resolve("file.cache");
        final CacheFile cacheFile = new CacheFile(CACHE_KEY, randomLongBetween(1, 100), file, NOOP);

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

        assertFalse(Files.exists(file));
    }

    public void testConcurrentAccess() throws Exception {
        final Path file = createTempDir().resolve("file.cache");
        final CacheFile cacheFile = new CacheFile(CACHE_KEY, randomLongBetween(1, 100), file, NOOP);

        final TestEvictionListener evictionListener = new TestEvictionListener();
        cacheFile.acquire(evictionListener);
        final long length = cacheFile.getLength();
        final DeterministicTaskQueue deterministicTaskQueue = new DeterministicTaskQueue(
            builder().put(NODE_NAME_SETTING.getKey(), getTestName()).build(),
            random()
        );
        final ThreadPool threadPool = deterministicTaskQueue.getThreadPool();
        final Future<Integer> populateAndReadFuture;
        final Future<Integer> readIfAvailableFuture;
        if (randomBoolean()) {
            populateAndReadFuture = cacheFile.populateAndRead(
                Tuple.tuple(0L, length),
                Tuple.tuple(0L, length),
                channel -> Math.toIntExact(length),
                (channel, from, to, progressUpdater) -> progressUpdater.accept(length),
                threadPool.generic()
            );
        } else {
            populateAndReadFuture = null;
        }
        if (randomBoolean()) {
            readIfAvailableFuture = cacheFile.readIfAvailableOrPending(Tuple.tuple(0L, length), channel -> Math.toIntExact(length));
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
            assertTrue(populateAndReadFuture.isDone());
        }
        if (readIfAvailableFuture != null) {
            assertTrue(readIfAvailableFuture.isDone());
        }
        if (evicted) {
            assertFalse(Files.exists(file));
        }
    }

    public void testFSync() throws Exception {
        final FSyncTrackingFileSystemProvider fileSystem = setupFSyncCountingFileSystem();
        try {
            final AtomicBoolean needsFSyncCalled = new AtomicBoolean();
            final CacheFile cacheFile = new CacheFile(
                CACHE_KEY,
                randomLongBetween(0L, 1000L),
                fileSystem.resolve("test"),
                () -> assertFalse(needsFSyncCalled.getAndSet(true))
            );
            assertFalse(cacheFile.needsFsync());
            assertFalse(needsFSyncCalled.get());

            final TestEvictionListener listener = new TestEvictionListener();
            cacheFile.acquire(listener);

            try {
                if (randomBoolean()) {
                    final SortedSet<Tuple<Long, Long>> completedRanges = cacheFile.fsync();
                    assertNumberOfFSyncs(cacheFile.getFile(), equalTo(0));
                    assertThat(completedRanges, hasSize(0));
                    assertFalse(cacheFile.needsFsync());
                    assertFalse(needsFSyncCalled.get());
                }

                final SortedSet<Tuple<Long, Long>> expectedCompletedRanges = randomPopulateAndReads(cacheFile);
                if (expectedCompletedRanges.isEmpty() == false) {
                    assertTrue(cacheFile.needsFsync());
                    assertTrue(needsFSyncCalled.getAndSet(false));
                } else {
                    assertFalse(cacheFile.needsFsync());
                    assertFalse(needsFSyncCalled.get());
                }

                final SortedSet<Tuple<Long, Long>> completedRanges = cacheFile.fsync();
                assertNumberOfFSyncs(cacheFile.getFile(), equalTo(expectedCompletedRanges.isEmpty() ? 0 : 1));
                assertArrayEquals(completedRanges.toArray(Tuple[]::new), expectedCompletedRanges.toArray(Tuple[]::new));
                assertFalse(cacheFile.needsFsync());
                assertFalse(needsFSyncCalled.get());
            } finally {
                cacheFile.release(listener);
            }
        } finally {
            fileSystem.tearDown();
        }
    }

    public void testFSyncOnEvictedFile() throws Exception {
        final FSyncTrackingFileSystemProvider fileSystem = setupFSyncCountingFileSystem();
        try {
            final AtomicBoolean needsFSyncCalled = new AtomicBoolean();
            final CacheFile cacheFile = new CacheFile(
                CACHE_KEY,
                randomLongBetween(0L, 1000L),
                fileSystem.resolve("test"),
                () -> assertFalse(needsFSyncCalled.getAndSet(true))
            );
            assertFalse(cacheFile.needsFsync());
            assertFalse(needsFSyncCalled.get());

            final TestEvictionListener listener = new TestEvictionListener();
            cacheFile.acquire(listener);

            final RunOnce releaseOnce = new RunOnce(() -> cacheFile.release(listener));
            try {
                final SortedSet<Tuple<Long, Long>> expectedCompletedRanges = randomPopulateAndReads(cacheFile);
                if (expectedCompletedRanges.isEmpty() == false) {
                    assertTrue(cacheFile.needsFsync());
                    assertTrue(needsFSyncCalled.getAndSet(false));

                    final SortedSet<Tuple<Long, Long>> completedRanges = cacheFile.fsync();
                    assertArrayEquals(completedRanges.toArray(Tuple[]::new), expectedCompletedRanges.toArray(Tuple[]::new));
                    assertNumberOfFSyncs(cacheFile.getFile(), equalTo(1));
                }
                assertFalse(cacheFile.needsFsync());
                assertFalse(needsFSyncCalled.get());

                cacheFile.startEviction();

                if (rarely()) {
                    assertThat("New ranges should not be written after cache file eviction", randomPopulateAndReads(cacheFile), hasSize(0));
                }
                if (randomBoolean()) {
                    releaseOnce.run();
                }

                final SortedSet<Tuple<Long, Long>> completedRangesAfterEviction = cacheFile.fsync();
                assertNumberOfFSyncs(cacheFile.getFile(), equalTo(expectedCompletedRanges.isEmpty() ? 0 : 1));
                assertThat(completedRangesAfterEviction, hasSize(0));
                assertFalse(cacheFile.needsFsync());
                assertFalse(needsFSyncCalled.get());
            } finally {
                releaseOnce.run();
            }
        } finally {
            fileSystem.tearDown();
        }
    }

    public void testFSyncFailure() throws Exception {
        final FSyncTrackingFileSystemProvider fileSystem = setupFSyncCountingFileSystem();
        try {
            fileSystem.failFSyncs(true);

            final AtomicBoolean needsFSyncCalled = new AtomicBoolean();
            final CacheFile cacheFile = new CacheFile(
                CACHE_KEY,
                randomLongBetween(0L, 1000L),
                fileSystem.resolve("test"),
                () -> assertFalse(needsFSyncCalled.getAndSet(true))
            );
            assertFalse(cacheFile.needsFsync());
            assertFalse(needsFSyncCalled.get());

            final TestEvictionListener listener = new TestEvictionListener();
            cacheFile.acquire(listener);

            try {
                final SortedSet<Tuple<Long, Long>> expectedCompletedRanges = randomPopulateAndReads(cacheFile);
                if (expectedCompletedRanges.isEmpty() == false) {
                    assertTrue(cacheFile.needsFsync());
                    assertTrue(needsFSyncCalled.getAndSet(false));
                    IOException exception = expectThrows(IOException.class, cacheFile::fsync);
                    assertThat(exception.getMessage(), containsString("simulated"));
                    assertTrue(cacheFile.needsFsync());
                    assertTrue(needsFSyncCalled.getAndSet(false));
                } else {
                    assertFalse(cacheFile.needsFsync());
                    final SortedSet<Tuple<Long, Long>> completedRanges = cacheFile.fsync();
                    assertTrue(completedRanges.isEmpty());
                }
                assertNumberOfFSyncs(cacheFile.getFile(), equalTo(0));

                fileSystem.failFSyncs(false);

                final SortedSet<Tuple<Long, Long>> completedRanges = cacheFile.fsync();
                assertArrayEquals(completedRanges.toArray(Tuple[]::new), expectedCompletedRanges.toArray(Tuple[]::new));
                assertNumberOfFSyncs(cacheFile.getFile(), equalTo(expectedCompletedRanges.isEmpty() ? 0 : 1));
                assertFalse(cacheFile.needsFsync());
                assertFalse(needsFSyncCalled.get());
            } finally {
                cacheFile.release(listener);
            }
        } finally {
            fileSystem.tearDown();
        }
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

    public static void assertNumberOfFSyncs(final Path path, final Matcher<Integer> matcher) {
        final FSyncTrackingFileSystemProvider provider = (FSyncTrackingFileSystemProvider) path.getFileSystem().provider();
        final Integer fsyncCount = provider.getNumberOfFSyncs(path);
        assertThat("File [" + path + "] was never fsynced", fsyncCount, notNullValue());
        assertThat("Mismatching number of fsync for [" + path + "]", fsyncCount, matcher);
    }

    private static FSyncTrackingFileSystemProvider setupFSyncCountingFileSystem() {
        final FileSystem defaultFileSystem = PathUtils.getDefaultFileSystem();
        final FSyncTrackingFileSystemProvider provider = new FSyncTrackingFileSystemProvider(defaultFileSystem, createTempDir());
        PathUtilsForTesting.installMock(provider.getFileSystem(null));
        return provider;
    }
}
