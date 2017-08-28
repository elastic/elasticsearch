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
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.CheckIndex;
import org.apache.lucene.store.BaseDirectoryWrapper;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.LockFactory;
import org.apache.lucene.store.LockObtainFailedException;
import org.apache.lucene.store.MockDirectoryWrapper;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestRuleMarkFailure;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
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
import java.util.Random;

public class MockFSDirectoryService extends FsDirectoryService {

    public static final Setting<Double> RANDOM_IO_EXCEPTION_RATE_ON_OPEN_SETTING =
        Setting.doubleSetting("index.store.mock.random.io_exception_rate_on_open", 0.0d,  0.0d, Property.IndexScope, Property.NodeScope);
    public static final Setting<Double> RANDOM_IO_EXCEPTION_RATE_SETTING =
        Setting.doubleSetting("index.store.mock.random.io_exception_rate", 0.0d,  0.0d, Property.IndexScope, Property.NodeScope);
    public static final Setting<Boolean> RANDOM_PREVENT_DOUBLE_WRITE_SETTING =
        Setting.boolSetting("index.store.mock.random.prevent_double_write", true, Property.IndexScope, Property.NodeScope);// true is default in MDW
    public static final Setting<Boolean> RANDOM_NO_DELETE_OPEN_FILE_SETTING =
        Setting.boolSetting("index.store.mock.random.no_delete_open_file", true, Property.IndexScope, Property.NodeScope);// true is default in MDW
    public static final Setting<Boolean> CRASH_INDEX_SETTING =
        Setting.boolSetting("index.store.mock.random.crash_index", true, Property.IndexScope, Property.NodeScope);// true is default in MDW

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
        final long seed = idxSettings.getValue(ESIntegTestCase.INDEX_TEST_SEED_SETTING);
        this.random = new Random(seed);

        randomIOExceptionRate = RANDOM_IO_EXCEPTION_RATE_SETTING.get(indexSettings);
        randomIOExceptionRateOnOpen = RANDOM_IO_EXCEPTION_RATE_ON_OPEN_SETTING.get(indexSettings);
        preventDoubleWrite = RANDOM_PREVENT_DOUBLE_WRITE_SETTING.get(indexSettings);
        noDeleteOpenFile = RANDOM_NO_DELETE_OPEN_FILE_SETTING.exists(indexSettings) ? RANDOM_NO_DELETE_OPEN_FILE_SETTING.get(indexSettings) : random.nextBoolean();
        random.nextInt(shardId.getId() + 1); // some randomness per shard
        throttle = MockDirectoryWrapper.Throttling.NEVER;
        crashIndex = CRASH_INDEX_SETTING.get(indexSettings);

        if (logger.isDebugEnabled()) {
            logger.debug("Using MockDirWrapper with seed [{}] throttle: [{}] crashIndex: [{}]", SeedUtils.formatSeed(seed),
                    throttle, crashIndex);
        }
        delegateService = randomDirectoryService(indexStore, path);
    }


    @Override
    public Directory newDirectory() throws IOException {
        return wrap(delegateService.newDirectory());
    }

    @Override
    protected synchronized Directory newFSDirectory(Path location, LockFactory lockFactory) throws IOException {
        throw new UnsupportedOperationException();
    }

    public static void checkIndex(Logger logger, Store store, ShardId shardId) {
        if (store.tryIncRef()) {
            logger.info("start check index");
            try {
                Directory dir = store.directory();
                if (!Lucene.indexExists(dir)) {
                    return;
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
                                os.bytes().utf8ToString());
                        throw new IOException("index check failure");
                    } else {
                        if (logger.isDebugEnabled()) {
                            logger.debug("check index [success]\n{}", os.bytes().utf8ToString());
                        }
                    }
                } catch (LockObtainFailedException e) {
                    ESTestCase.checkIndexFailed = true;
                    throw new IllegalStateException("IndexWriter is still open on shard " + shardId, e);
                }
            } catch (Exception e) {
                logger.warn("failed to check index", e);
            } finally {
                logger.info("end check index");
                store.decRef();
            }
        }
    }

    private Directory wrap(Directory dir) {
        final ElasticsearchMockDirectoryWrapper w = new ElasticsearchMockDirectoryWrapper(random, dir, this.crashIndex);
        w.setRandomIOExceptionRate(randomIOExceptionRate);
        w.setRandomIOExceptionRateOnOpen(randomIOExceptionRateOnOpen);
        w.setThrottling(throttle);
        w.setCheckIndexOnClose(false); // we do this on the index level
        // TODO: make this test robust to virus scanner
        w.setAssertNoDeleteOpenFile(false);
        w.setUseSlowOpenClosers(false);
        LuceneTestCase.closeAfterSuite(new CloseableDirectory(w));
        return w;
    }

    private FsDirectoryService randomDirectoryService(IndexStore indexStore, ShardPath path) {
        final IndexSettings indexSettings = indexStore.getIndexSettings();
        final IndexMetaData build = IndexMetaData.builder(indexSettings.getIndexMetaData())
            .settings(Settings.builder()
                // don't use the settings from indexSettings#getSettings() they are merged with node settings and might contain
                // secure settings that should not be copied in here since the new IndexSettings ctor below will barf if we do
                .put(indexSettings.getIndexMetaData().getSettings())
                .put(IndexModule.INDEX_STORE_TYPE_SETTING.getKey(),
                    RandomPicks.randomFrom(random, IndexModule.Type.values()).getSettingsKey()))
            .build();
        final IndexSettings newIndexSettings = new IndexSettings(build, indexSettings.getNodeSettings());
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

        CloseableDirectory(BaseDirectoryWrapper dir) {
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
