/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.index.store.cache;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.cluster.coordination.DeterministicTaskQueue;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.index.store.cache.CacheFile.EvictionListener;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Future;

import static org.elasticsearch.common.settings.Settings.builder;
import static org.elasticsearch.node.Node.NODE_NAME_SETTING;
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
}
