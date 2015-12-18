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

import com.carrotsearch.randomizedtesting.SeedUtils;
import com.carrotsearch.randomizedtesting.generators.RandomPicks;
import org.apache.lucene.index.CheckIndex;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.store.BaseDirectoryWrapper;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.LockFactory;
import org.apache.lucene.store.MockDirectoryWrapper;
import org.apache.lucene.store.StoreRateLimiting;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestRuleMarkFailure;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardPath;
import org.elasticsearch.index.store.FsDirectoryService;
import org.elasticsearch.index.store.IndexStore;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESTestCase;
import org.junit.Assert;

import java.io.Closeable;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.Random;

public class MockFSDirectoryService extends FsDirectoryService {

    public static final String RANDOM_IO_EXCEPTION_RATE_ON_OPEN = "index.store.mock.random.io_exception_rate_on_open";
    public static final String RANDOM_PREVENT_DOUBLE_WRITE = "index.store.mock.random.prevent_double_write";
    public static final String RANDOM_NO_DELETE_OPEN_FILE = "index.store.mock.random.no_delete_open_file";
    public static final String CRASH_INDEX = "index.store.mock.random.crash_index";

    private final FsDirectoryService delegateService;
    private final Random random;
    private final double randomIOExceptionRate;
    private final double randomIOExceptionRateOnOpen;
    private final MockDirectoryWrapper.Throttling throttle;
    private final boolean preventDoubleWrite;
    private final boolean noDeleteOpenFile;
    private final boolean crashIndex;

    @Inject
    public MockFSDirectoryService(IndexSettings idxSettings, IndexStore indexStore, final ShardPath path) {
        super(idxSettings, indexStore, path);
        Settings indexSettings = idxSettings.getSettings();
        final long seed = indexSettings.getAsLong(ESIntegTestCase.SETTING_INDEX_SEED, 0l);
        this.random = new Random(seed);

        randomIOExceptionRate = indexSettings.getAsDouble(RANDOM_IO_EXCEPTION_RATE, 0.0d);
        randomIOExceptionRateOnOpen = indexSettings.getAsDouble(RANDOM_IO_EXCEPTION_RATE_ON_OPEN, 0.0d);
        preventDoubleWrite = indexSettings.getAsBoolean(RANDOM_PREVENT_DOUBLE_WRITE, true); // true is default in MDW
        noDeleteOpenFile = indexSettings.getAsBoolean(RANDOM_NO_DELETE_OPEN_FILE, random.nextBoolean()); // true is default in MDW
        random.nextInt(shardId.getId() + 1); // some randomness per shard
        throttle = MockDirectoryWrapper.Throttling.NEVER;
        crashIndex = indexSettings.getAsBoolean(CRASH_INDEX, true);

        if (logger.isDebugEnabled()) {
            logger.debug("Using MockDirWrapper with seed [{}] throttle: [{}] crashIndex: [{}]", SeedUtils.formatSeed(seed),
                    throttle, crashIndex);
        }
        delegateService = randomDirectorService(indexStore, path);
    }


    @Override
    public Directory newDirectory() throws IOException {
        return wrap(delegateService.newDirectory());
    }

    @Override
    protected synchronized Directory newFSDirectory(Path location, LockFactory lockFactory) throws IOException {
        throw new UnsupportedOperationException();
    }

    public static void checkIndex(ESLogger logger, Store store, ShardId shardId) {
        if (store.tryIncRef()) {
            logger.info("start check index");
            try {
                Directory dir = store.directory();
                if (!Lucene.indexExists(dir)) {
                    return;
                }
                if (IndexWriter.isLocked(dir)) {
                    ESTestCase.checkIndexFailed = true;
                    throw new IllegalStateException("IndexWriter is still open on shard " + shardId);
                }
                try (CheckIndex checkIndex = new CheckIndex(dir)) {
                    BytesStreamOutput os = new BytesStreamOutput();
                    PrintStream out = new PrintStream(os, false, StandardCharsets.UTF_8.name());
                    checkIndex.setInfoStream(out);
                    out.flush();
                    CheckIndex.Status status = checkIndex.checkIndex();
                    if (!status.clean) {
                        ESTestCase.checkIndexFailed = true;
                        logger.warn("check index [failure] index files={}\n{}",
                                Arrays.toString(dir.listAll()),
                                new String(os.bytes().toBytes(), StandardCharsets.UTF_8));
                        throw new IOException("index check failure");
                    } else {
                        if (logger.isDebugEnabled()) {
                            logger.debug("check index [success]\n{}", new String(os.bytes().toBytes(), StandardCharsets.UTF_8));
                        }
                    }
                }
            } catch (Exception e) {
                logger.warn("failed to check index", e);
            } finally {
                logger.info("end check index");
                store.decRef();
            }
        }
    }

    @Override
    public void onPause(long nanos) {
        delegateService.onPause(nanos);
    }

    @Override
    public StoreRateLimiting rateLimiting() {
        return delegateService.rateLimiting();
    }

    @Override
    public long throttleTimeInNanos() {
        return delegateService.throttleTimeInNanos();
    }

    public static final String RANDOM_IO_EXCEPTION_RATE = "index.store.mock.random.io_exception_rate";

    private Directory wrap(Directory dir) {
        final ElasticsearchMockDirectoryWrapper w = new ElasticsearchMockDirectoryWrapper(random, dir, this.crashIndex);
        w.setRandomIOExceptionRate(randomIOExceptionRate);
        w.setRandomIOExceptionRateOnOpen(randomIOExceptionRateOnOpen);
        w.setThrottling(throttle);
        w.setCheckIndexOnClose(false); // we do this on the index level
        w.setPreventDoubleWrite(preventDoubleWrite);
        // TODO: make this test robust to virus scanner
        w.setEnableVirusScanner(false);
        w.setNoDeleteOpenFile(noDeleteOpenFile);
        w.setUseSlowOpenClosers(false);
        LuceneTestCase.closeAfterSuite(new CloseableDirectory(w));
        return w;
    }

    private FsDirectoryService randomDirectorService(IndexStore indexStore, ShardPath path) {
        final IndexSettings indexSettings = indexStore.getIndexSettings();
        final IndexMetaData build = IndexMetaData.builder(indexSettings.getIndexMetaData()).settings(Settings.builder().put(indexSettings.getSettings()).put(IndexModule.STORE_TYPE, RandomPicks.randomFrom(random, IndexModule.Type.values()).getSettingsKey())).build();
        final IndexSettings newIndexSettings = new IndexSettings(build, indexSettings.getNodeSettings(), Collections.emptyList());
        return new FsDirectoryService(newIndexSettings, indexStore, path);
    }

    public static final class ElasticsearchMockDirectoryWrapper extends MockDirectoryWrapper {

        private final boolean crash;

        public ElasticsearchMockDirectoryWrapper(Random random, Directory delegate, boolean crash) {
            super(random, delegate);
            this.crash = crash;
        }

        @Override
        public synchronized void crash() throws IOException {
            if (crash) {
                super.crash();
            }
        }
    }

    final class CloseableDirectory implements Closeable {
        private final BaseDirectoryWrapper dir;
        private final TestRuleMarkFailure failureMarker;

        public CloseableDirectory(BaseDirectoryWrapper dir) {
            this.dir = dir;
            this.failureMarker = ESTestCase.getSuiteFailureMarker();
        }

        @Override
        public void close() {
            // We only attempt to check open/closed state if there were no other test
            // failures.
            try {
                if (failureMarker.wasSuccessful() && dir.isOpen()) {
                    Assert.fail("Directory not closed: " + dir);
                }
            } finally {
                // TODO: perform real close of the delegate: LUCENE-4058
                // dir.close();
            }
        }
    }
}
