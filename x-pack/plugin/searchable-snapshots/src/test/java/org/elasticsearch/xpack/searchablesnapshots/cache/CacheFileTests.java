/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.searchablesnapshots.cache;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.searchablesnapshots.cache.CacheFile.EvictionListener;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

public class CacheFileTests extends ESTestCase {

    public void testAcquireAndRelease() throws Exception {
        final Path file = createTempDir().resolve("file.cache");
        final CacheFile cacheFile = new CacheFile("test", randomLongBetween(1, 100), file);

        assertThat("Cache file is not acquired: no channel exists", cacheFile.getChannelRefCounter(), nullValue());
        assertThat("Cache file is not acquired: file does not exist", Files.exists(file), is(false));

        final TestEvictionListener listener = new TestEvictionListener();
        boolean acquired = cacheFile.acquire(listener);
        assertThat("Cache file has been acquired", acquired, is(true));
        assertThat("Cache file has been acquired: file should exists", Files.exists(file), is(true));
        assertThat("Cache file has been acquired: eviction listener is not executed", listener.isCalled(), is(false));

        CacheFile.FileChannelRefCounted channelRef = cacheFile.getChannelRefCounter();
        assertThat("Cache file has been acquired: channel should exists", channelRef, notNullValue());
        assertThat("Cache file has been acquired: channel ref count should be 1", channelRef.refCount(), equalTo(1));

        boolean released = cacheFile.release(listener);
        assertThat("Cache file has been released", released, is(true));
        assertThat("Cache file has been released: eviction listener is not executed", listener.isCalled(), is(false));

        channelRef = cacheFile.getChannelRefCounter();
        assertThat("Cache file has been released and not acquired again: channel does not exist", channelRef, nullValue());
        assertThat("Cache file is not evicted: file still exists after release", Files.exists(file), is(true));

        acquired = cacheFile.acquire(listener);
        assertThat("Cache file is acquired again", acquired, is(true));

        for (int i = 0; i < randomIntBetween(1, 10); i++) {
            channelRef = cacheFile.getChannelRefCounter();
            assertThat(channelRef, notNullValue());

            channelRef.incRef();
            try {
                assertThat(channelRef.refCount(), equalTo(2));
            } finally {
                channelRef.decRef();
                assertThat(Files.exists(cacheFile.getFile()), is(true));
            }
        }

        final CacheFile.FileChannelRefCounted lastChannelRef = channelRef;

        assertThat("Cache file is not evicted: eviction listener is not executed notified", listener.isCalled(), is(false));
        cacheFile.close();

        assertThat("Cache file has been evicted: eviction listener was executed", listener.isCalled(), is(true));
        assertThat("Cache file is evicted but not fully released: file still exists", Files.exists(file), is(true));
        assertThat("Cache file is evicted but not fully released: channel still exists", channelRef, notNullValue());
        assertThat("Channel didn't change after eviction", channelRef, sameInstance(lastChannelRef));

        for (int i = 0; i < randomIntBetween(1, 10); i++) {
            channelRef = cacheFile.getChannelRefCounter();
            assertThat(channelRef, notNullValue());

            channelRef.incRef();
            try {
                assertThat(channelRef.refCount(), equalTo(2));
                assertThat(channelRef, sameInstance(lastChannelRef));
            } finally {
                channelRef.decRef();
                assertThat(Files.exists(cacheFile.getFile()), is(true));
            }
        }

        released = cacheFile.release(listener);
        assertTrue("Cache file is fully released", released);
        assertThat("Cache file evicted and fully released: channel is closed", channelRef.refCount(), equalTo(0));
        assertThat("Cache file has been deleted", Files.exists(file), is(false));
    }

    public void testCacheFileNotAcquired() throws IOException {
        final Path file = createTempDir().resolve("file.cache");
        final CacheFile cacheFile = new CacheFile("test", randomLongBetween(1, 100), file);

        assertFalse(Files.exists(file));
        assertThat(cacheFile.getChannelRefCounter(), nullValue());

        if (randomBoolean()) {
            final TestEvictionListener listener = new TestEvictionListener();
            boolean acquired = cacheFile.acquire(listener);
            assertTrue("Cache file is acquired", acquired);

            assertThat(cacheFile.getChannelRefCounter(), notNullValue());
            assertThat(Files.exists(file), is(true));

            boolean released = cacheFile.release(listener);
            assertTrue("Cache file is released", released);
        }

        cacheFile.close();
        assertFalse(Files.exists(file));
    }

    public void testDeleteOnCloseAfterLastRelease() throws Exception {
        final Path file = createTempDir().resolve("file.cache");
        final CacheFile cacheFile = new CacheFile("test", randomLongBetween(1, 100), file);

        final List<TestEvictionListener> acquiredListeners = new ArrayList<>();
        for (int i = 0; i < randomIntBetween(1, 20); i++) {
            TestEvictionListener listener = new TestEvictionListener();
            assertTrue(cacheFile.acquire(listener));
            acquiredListeners.add(listener);
        }

        final List<TestEvictionListener> releasedListeners = new ArrayList<>();
        for (Iterator<TestEvictionListener> it = acquiredListeners.iterator(); it.hasNext(); ) {
            if (randomBoolean()) {
                TestEvictionListener listener = it.next();
                releasedListeners.add(listener);
                cacheFile.release(listener);
                it.remove();
            }
        }

        assertTrue(Files.exists(file));
        cacheFile.close();

        releasedListeners.forEach(l -> assertFalse("Released listeners before cache file eviction are not called", l.isCalled()));
        acquiredListeners.forEach(l -> assertTrue("Released listeners after cache file eviction are called", l.isCalled()));
        acquiredListeners.forEach(cacheFile::release);

        assertFalse(Files.exists(file));
    }

    class TestEvictionListener implements EvictionListener {

        private SetOnce<CacheFile> evicted = new SetOnce<>();

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
