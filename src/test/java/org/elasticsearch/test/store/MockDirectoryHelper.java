/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.test.store;

import com.carrotsearch.randomizedtesting.generators.RandomPicks;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.MockDirectoryWrapper.Throttling;
import org.apache.lucene.store.MockDirectoryWrapper;
import org.apache.lucene.store.NRTCachingDirectory;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardPath;
import org.elasticsearch.index.store.FsDirectoryService;
import org.elasticsearch.index.store.IndexStore;
import org.elasticsearch.index.store.IndexStoreModule;
import com.carrotsearch.randomizedtesting.SeedUtils;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Collection;
import java.util.Random;
import java.util.Set;

public class MockDirectoryHelper {
    public static final String RANDOM_IO_EXCEPTION_RATE = "index.store.mock.random.io_exception_rate";
    public static final String RANDOM_IO_EXCEPTION_RATE_ON_OPEN = "index.store.mock.random.io_exception_rate_on_open";
    public static final String RANDOM_PREVENT_DOUBLE_WRITE = "index.store.mock.random.prevent_double_write";
    public static final String RANDOM_NO_DELETE_OPEN_FILE = "index.store.mock.random.no_delete_open_file";
    public static final String CRASH_INDEX = "index.store.mock.random.crash_index";

    public static final Set<ElasticsearchMockDirectoryWrapper> wrappers = ConcurrentCollections.newConcurrentSet();

    private final Random random;
    private final double randomIOExceptionRate;
    private final double randomIOExceptionRateOnOpen;
    private final Throttling throttle;
    private final Settings indexSettings;
    private final ShardId shardId;
    private final boolean preventDoubleWrite;
    private final boolean noDeleteOpenFile;
    private final ESLogger logger;
    private final boolean crashIndex;

    public MockDirectoryHelper(ShardId shardId, Settings indexSettings, ESLogger logger, Random random, long seed) {
        this.random = random;
        randomIOExceptionRate = indexSettings.getAsDouble(RANDOM_IO_EXCEPTION_RATE, 0.0d);
        randomIOExceptionRateOnOpen = indexSettings.getAsDouble(RANDOM_IO_EXCEPTION_RATE_ON_OPEN, 0.0d);
        preventDoubleWrite = indexSettings.getAsBoolean(RANDOM_PREVENT_DOUBLE_WRITE, true); // true is default in MDW
        noDeleteOpenFile = indexSettings.getAsBoolean(RANDOM_NO_DELETE_OPEN_FILE, random.nextBoolean()); // true is default in MDW
        random.nextInt(shardId.getId() + 1); // some randomness per shard
        throttle = Throttling.NEVER;
        crashIndex = indexSettings.getAsBoolean(CRASH_INDEX, true);

        if (logger.isDebugEnabled()) {
            logger.debug("Using MockDirWrapper with seed [{}] throttle: [{}] crashIndex: [{}]", SeedUtils.formatSeed(seed),
                    throttle, crashIndex);
        }
        this.indexSettings = indexSettings;
        this.shardId = shardId;
        this.logger = logger;
    }

    public Directory wrap(Directory dir) {
        final ElasticsearchMockDirectoryWrapper w = new ElasticsearchMockDirectoryWrapper(random, dir, logger, this.crashIndex);
        w.setRandomIOExceptionRate(randomIOExceptionRate);
        w.setRandomIOExceptionRateOnOpen(randomIOExceptionRateOnOpen);
        w.setThrottling(throttle);
        w.setCheckIndexOnClose(false); // we do this on the index level
        w.setPreventDoubleWrite(preventDoubleWrite);
        // TODO: make this test robust to virus scanner
        w.setEnableVirusScanner(false);
        w.setNoDeleteOpenFile(noDeleteOpenFile);
        w.setUseSlowOpenClosers(false);
        wrappers.add(w);
        return w;
    }

    public FsDirectoryService randomDirectorService(IndexStore indexStore, ShardPath path) {
        ImmutableSettings.Builder builder = ImmutableSettings.settingsBuilder();
        builder.put(indexSettings);
        builder.put(IndexStoreModule.STORE_TYPE, RandomPicks.randomFrom(random, IndexStoreModule.Type.values()));
        return new FsDirectoryService(builder.build(), indexStore, path);
    }

    public static final class ElasticsearchMockDirectoryWrapper extends MockDirectoryWrapper {

        private final ESLogger logger;
        private final boolean crash;
        private volatile RuntimeException closeException;
        private final Object lock = new Object();
        private final Set<String> superUnSyncedFiles;
        private final Random superRandomState;

        public ElasticsearchMockDirectoryWrapper(Random random, Directory delegate, ESLogger logger, boolean crash) {
            super(random, delegate);
            this.crash = crash;
            this.logger = logger;

            // TODO: remove all this and cutover to MockFS (DisableFsyncFS) instead
            try {
                Field field = MockDirectoryWrapper.class.getDeclaredField("unSyncedFiles");
                field.setAccessible(true);
                superUnSyncedFiles = (Set<String>) field.get(this);

                field = MockDirectoryWrapper.class.getDeclaredField("randomState");
                field.setAccessible(true);
                superRandomState = (Random) field.get(this);
            } catch (ReflectiveOperationException roe) {
                throw new RuntimeException(roe);
            }
        }

        @Override
        public synchronized void close() throws IOException {
            try {
                super.close();
            } catch (RuntimeException ex) {
                logger.info("MockDirectoryWrapper#close() threw exception", ex);
                closeException = ex;
                throw ex;
            } finally {
                synchronized (lock) {
                    lock.notifyAll();
                }
            }
        }

        /**
         * Returns true if {@link #in} must sync its files.
         * Currently, only {@link NRTCachingDirectory} requires sync'ing its files
         * because otherwise they are cached in an internal {@link org.apache.lucene.store.RAMDirectory}. If
         * other directories require that too, they should be added to this method.
         */
        private boolean mustSync() {
            Directory delegate = in;
            while (delegate instanceof FilterDirectory) {
                if (delegate instanceof NRTCachingDirectory) {
                    return true;
                }
                delegate = ((FilterDirectory) delegate).getDelegate();
            }
            return delegate instanceof NRTCachingDirectory;
        }

        @Override
        public synchronized void sync(Collection<String> names) throws IOException {
            // don't wear out our hardware so much in tests.
            if (superRandomState.nextInt(100) == 0 || mustSync()) {
                super.sync(names);
            } else {
                superUnSyncedFiles.removeAll(names);
            }
        }

        public void awaitClosed(long timeout) throws InterruptedException {
            synchronized (lock) {
                if(isOpen()) {
                    lock.wait(timeout);
                }
            }
        }

        public synchronized boolean successfullyClosed() {
            return closeException == null && !isOpen();
        }

        public synchronized RuntimeException closeException() {
            return closeException;
        }

        @Override
        public synchronized void crash() throws IOException {
            if (crash) {
                super.crash();
            }
        }
    }
}
