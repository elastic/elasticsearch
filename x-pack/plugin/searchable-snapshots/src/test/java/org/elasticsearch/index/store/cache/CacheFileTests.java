/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.index.store.cache;

import org.apache.lucene.mockfile.FilterFileChannel;
import org.apache.lucene.mockfile.FilterFileSystemProvider;
import org.apache.lucene.mockfile.FilterPath;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.cluster.coordination.DeterministicTaskQueue;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.io.PathUtils;
import org.elasticsearch.common.io.PathUtilsForTesting;
import org.elasticsearch.index.store.cache.CacheFile.EvictionListener;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.hamcrest.Matcher;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.spi.FileSystemProvider;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static java.util.Collections.synchronizedNavigableSet;
import static org.elasticsearch.common.settings.Settings.builder;
import static org.elasticsearch.index.store.cache.TestUtils.mergeContiguousRanges;
import static org.elasticsearch.node.Node.NODE_NAME_SETTING;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

public class CacheFileTests extends ESTestCase {

    public void testAcquireAndRelease() throws Exception {
        final Path file = createTempDir().resolve("file.cache");
        final CacheFile cacheFile = new CacheFile("test", randomLongBetween(1, 100), file);

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
        final CacheFile cacheFile = new CacheFile("test", randomLongBetween(1, 100), file);

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
        final CacheFile cacheFile = new CacheFile("test", randomLongBetween(1, 100), file);

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
        final CacheFile cacheFile = new CacheFile("test", randomLongBetween(1, 100), file);

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
        try (FSyncTrackingFileSystemProvider fileSystem = setupFSyncCountingFileSystem()) {
            final CacheFile cacheFile = new CacheFile("test", randomLongBetween(100, 1000), fileSystem.resolve("test"));
            assertFalse(cacheFile.needsFsync());

            final TestEvictionListener listener = new TestEvictionListener();
            cacheFile.acquire(listener);

            try {
                if (randomBoolean()) {
                    final SortedSet<Tuple<Long, Long>> completedRanges = cacheFile.fsync();
                    assertNumberOfFSyncs(cacheFile.getFile(), equalTo(0L));
                    assertThat(completedRanges, hasSize(0));
                    assertFalse(cacheFile.needsFsync());
                }

                final SortedSet<Tuple<Long, Long>> expectedCompletedRanges = randomPopulateAndReads(cacheFile);
                if (expectedCompletedRanges.isEmpty() == false) {
                    assertTrue(cacheFile.needsFsync());
                } else {
                    assertFalse(cacheFile.needsFsync());
                }

                final SortedSet<Tuple<Long, Long>> completedRanges = cacheFile.fsync();
                assertNumberOfFSyncs(cacheFile.getFile(), equalTo(expectedCompletedRanges.isEmpty() ? 0L : 1L));
                assertArrayEquals(completedRanges.toArray(Tuple[]::new), expectedCompletedRanges.toArray(Tuple[]::new));
                assertFalse(cacheFile.needsFsync());
            } finally {
                cacheFile.release(listener);
            }
        }
    }

    public void testFSyncOnEvictedFile() throws Exception {
        try (FSyncTrackingFileSystemProvider fileSystem = setupFSyncCountingFileSystem()) {
            final CacheFile cacheFile = new CacheFile("test", randomLongBetween(1L, 1000L), fileSystem.resolve("test"));
            assertFalse(cacheFile.needsFsync());

            final TestEvictionListener listener = new TestEvictionListener();
            cacheFile.acquire(listener);

            try {
                final SortedSet<Tuple<Long, Long>> expectedCompletedRanges = randomPopulateAndReads(cacheFile);
                if (expectedCompletedRanges.isEmpty() == false) {
                    assertTrue(cacheFile.needsFsync());
                    final SortedSet<Tuple<Long, Long>> completedRanges = cacheFile.fsync();
                    assertArrayEquals(completedRanges.toArray(Tuple[]::new), expectedCompletedRanges.toArray(Tuple[]::new));
                    assertNumberOfFSyncs(cacheFile.getFile(), equalTo(1L));
                    assertFalse(cacheFile.needsFsync());
                }
                assertFalse(cacheFile.needsFsync());

                cacheFile.startEviction();

                final SortedSet<Tuple<Long, Long>> completedRangesAfterEviction = cacheFile.fsync();
                assertNumberOfFSyncs(cacheFile.getFile(), equalTo(expectedCompletedRanges.isEmpty() ? 0L : 1L));
                assertThat(completedRangesAfterEviction, hasSize(0));
                assertFalse(cacheFile.needsFsync());
            } finally {
                cacheFile.release(listener);
            }
        }
    }

    public void testFSyncFailure() throws Exception {
        try (FSyncTrackingFileSystemProvider fileSystem = setupFSyncCountingFileSystem()) {
            fileSystem.failFSyncs.set(true);

            final CacheFile cacheFile = new CacheFile("test", randomLongBetween(1L, 1000L), fileSystem.resolve("test"));
            assertFalse(cacheFile.needsFsync());

            final TestEvictionListener listener = new TestEvictionListener();
            cacheFile.acquire(listener);

            try {
                final SortedSet<Tuple<Long, Long>> expectedCompletedRanges = randomPopulateAndReads(cacheFile);
                if (expectedCompletedRanges.isEmpty() == false) {
                    assertTrue(cacheFile.needsFsync());
                    expectThrows(IOException.class, cacheFile::fsync);
                } else {
                    assertFalse(cacheFile.needsFsync());
                    final SortedSet<Tuple<Long, Long>> completedRanges = cacheFile.fsync();
                    assertTrue(completedRanges.isEmpty());
                }
                assertNumberOfFSyncs(cacheFile.getFile(), equalTo(0L));

                fileSystem.failFSyncs.set(false);

                final SortedSet<Tuple<Long, Long>> completedRanges = cacheFile.fsync();
                assertArrayEquals(completedRanges.toArray(Tuple[]::new), expectedCompletedRanges.toArray(Tuple[]::new));
                assertNumberOfFSyncs(cacheFile.getFile(), equalTo(expectedCompletedRanges.isEmpty() ? 0L : 1L));
                assertFalse(cacheFile.needsFsync());
            } finally {
                cacheFile.release(listener);
            }
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

    private SortedSet<Tuple<Long, Long>> randomPopulateAndReads(final CacheFile cacheFile) {
        final SortedSet<Tuple<Long, Long>> ranges = synchronizedNavigableSet(new TreeSet<>(Comparator.comparingLong(Tuple::v1)));
        final List<Future<Integer>> futures = new ArrayList<>();
        final DeterministicTaskQueue deterministicTaskQueue = new DeterministicTaskQueue(
            builder().put(NODE_NAME_SETTING.getKey(), getTestName()).build(),
            random()
        );
        for (int i = 0; i < between(0, 10); i++) {
            final long start = randomLongBetween(0L, cacheFile.getLength() - 1L);
            final long end = randomLongBetween(start + 1L, cacheFile.getLength());
            final Tuple<Long, Long> range = Tuple.tuple(start, end);
            futures.add(
                cacheFile.populateAndRead(range, range, channel -> Math.toIntExact(end - start), (channel, from, to, progressUpdater) -> {
                    ranges.add(Tuple.tuple(from, to));
                    progressUpdater.accept(to);
                }, deterministicTaskQueue.getThreadPool().generic())
            );
        }
        deterministicTaskQueue.runAllRunnableTasks();
        assertTrue(futures.stream().allMatch(Future::isDone));
        return mergeContiguousRanges(ranges);
    }

    public static void assertNumberOfFSyncs(final Path path, final Matcher<Long> matcher) {
        final FSyncTrackingFileSystemProvider provider = (FSyncTrackingFileSystemProvider) path.getFileSystem().provider();
        final AtomicLong fsyncCounter = provider.files.get(path);
        assertThat("File [" + path + "] was never fsynced", notNullValue());
        assertThat("Mismatching number of fsync for [" + path + "]", fsyncCounter.get(), matcher);
    }

    private static FSyncTrackingFileSystemProvider setupFSyncCountingFileSystem() {
        final FileSystem defaultFileSystem = PathUtils.getDefaultFileSystem();
        final FSyncTrackingFileSystemProvider provider = new FSyncTrackingFileSystemProvider(defaultFileSystem, createTempDir());
        PathUtilsForTesting.installMock(provider.getFileSystem(null));
        return provider;
    }

    /**
     * A {@link FileSystemProvider} that counts the number of times the method {@link FileChannel#force(boolean)} is executed on every
     * files. It reinstates the default file system when this file system provider is closed.
     */
    private static class FSyncTrackingFileSystemProvider extends FilterFileSystemProvider implements AutoCloseable {

        private final Map<Path, AtomicLong> files = new ConcurrentHashMap<>();
        private final AtomicBoolean failFSyncs = new AtomicBoolean();
        private final FileSystem delegateInstance;
        private final Path rootDir;

        FSyncTrackingFileSystemProvider(FileSystem delegate, Path rootDir) {
            super("fsynccounting://", delegate);
            this.rootDir = new FilterPath(rootDir, this.fileSystem);
            this.delegateInstance = delegate;
        }

        public Path resolve(String other) {
            return rootDir.resolve(other);
        }

        @Override
        public FileChannel newFileChannel(Path path, Set<? extends OpenOption> options, FileAttribute<?>... attrs) throws IOException {
            return new FilterFileChannel(delegate.newFileChannel(toDelegate(path), options, attrs)) {

                final AtomicLong counter = files.computeIfAbsent(path, p -> new AtomicLong(0L));

                @Override
                public void force(boolean metaData) throws IOException {
                    if (failFSyncs.get()) {
                        throw new IOException("simulated");
                    }
                    super.force(metaData);
                    counter.incrementAndGet();
                }
            };
        }

        @Override
        public void close() {
            PathUtilsForTesting.installMock(delegateInstance);
        }
    }
}
