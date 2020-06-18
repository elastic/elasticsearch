/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.index.store.cache;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.index.store.cache.CacheFile.EvictionListener;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

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
        boolean acquired = cacheFile.acquire(listener);
        assertThat("Cache file has been acquired", acquired, is(true));
        assertThat("Cache file has been acquired: file should exists", Files.exists(file), is(true));
        assertThat("Cache file has been acquired: channel should exists", cacheFile.getChannel(), notNullValue());
        assertThat("Cache file has been acquired: channel is open", cacheFile.getChannel().isOpen(), is(true));
        assertThat("Cache file has been acquired: eviction listener is not executed", listener.isCalled(), is(false));

        boolean released = cacheFile.release(listener);
        assertThat("Cache file has been released", released, is(true));
        assertThat("Cache file has been released: eviction listener is not executed", listener.isCalled(), is(false));
        assertThat("Cache file has been released: channel does not exist", cacheFile.getChannel(), nullValue());
        assertThat("Cache file is not evicted: file still exists after release", Files.exists(file), is(true));

        acquired = cacheFile.acquire(listener);
        assertThat("Cache file is acquired again", acquired, is(true));

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

        released = cacheFile.release(listener);
        assertTrue("Cache file is fully released", released);
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
            boolean acquired = cacheFile.acquire(listener);
            assertTrue("Cache file is acquired", acquired);

            assertThat(cacheFile.getChannel(), notNullValue());
            assertThat(Files.exists(file), is(true));

            boolean released = cacheFile.release(listener);
            assertTrue("Cache file is released", released);
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
            assertTrue(cacheFile.acquire(listener));
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
