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
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.CheckIndex;
import org.apache.lucene.store.BaseDirectoryWrapper;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.LockObtainFailedException;
import org.apache.lucene.store.MockDirectoryWrapper;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestRuleMarkFailure;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardPath;
import org.elasticsearch.index.store.FsDirectoryFactory;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.plugins.IndexStorePlugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESTestCase;
import org.junit.Assert;

import java.io.Closeable;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Random;

public class MockFSDirectoryFactory implements IndexStorePlugin.DirectoryFactory {

    public static final Setting<Double> RANDOM_IO_EXCEPTION_RATE_ON_OPEN_SETTING =
        Setting.doubleSetting("index.store.mock.random.io_exception_rate_on_open", 0.0d,  0.0d, Property.IndexScope, Property.NodeScope);
    public static final Setting<Double> RANDOM_IO_EXCEPTION_RATE_SETTING =
        Setting.doubleSetting("index.store.mock.random.io_exception_rate", 0.0d,  0.0d, Property.IndexScope, Property.NodeScope);
    public static final Setting<Boolean> CRASH_INDEX_SETTING =
        Setting.boolSetting("index.store.mock.random.crash_index", true, Property.IndexScope, Property.NodeScope);

    @Override
    public Directory newDirectory(IndexSettings idxSettings, ShardPath path) throws IOException {
        Settings indexSettings = idxSettings.getSettings();
        Random random = new Random(idxSettings.getValue(ESIntegTestCase.INDEX_TEST_SEED_SETTING));
        return wrap(randomDirectoryService(random, idxSettings, path), random, indexSettings,
            path.getShardId());
    }

    public static void checkIndex(Logger logger, Store store, ShardId shardId) {
        if (store.tryIncRef()) {
            logger.info("start check index");
            try {
                Directory dir = store.directory();
                if (!Lucene.indexExists(dir)) {
                    return;
                }
                try {
                    BytesStreamOutput os = new BytesStreamOutput();
                    PrintStream out = new PrintStream(os, false, StandardCharsets.UTF_8.name());
                    CheckIndex.Status status = store.checkIndex(out);
                    out.flush();
                    if (!status.clean) {
                        ESTestCase.checkIndexFailed = true;
                        logger.warn("check index [failure] index files={}\n{}", Arrays.toString(dir.listAll()), os.bytes().utf8ToString());
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

    private Directory wrap(Directory dir, Random random, Settings indexSettings, ShardId shardId) {

        double randomIOExceptionRate = RANDOM_IO_EXCEPTION_RATE_SETTING.get(indexSettings);
        double randomIOExceptionRateOnOpen = RANDOM_IO_EXCEPTION_RATE_ON_OPEN_SETTING.get(indexSettings);
        random.nextInt(shardId.getId() + 1); // some randomness per shard
        MockDirectoryWrapper.Throttling throttle = MockDirectoryWrapper.Throttling.NEVER;
        boolean crashIndex = CRASH_INDEX_SETTING.get(indexSettings);
        final ElasticsearchMockDirectoryWrapper w = new ElasticsearchMockDirectoryWrapper(random, dir, crashIndex);
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

    private Directory randomDirectoryService(Random random, IndexSettings indexSettings, ShardPath path) throws IOException {
        final IndexMetaData build = IndexMetaData.builder(indexSettings.getIndexMetaData())
            .settings(Settings.builder()
                // don't use the settings from indexSettings#getSettings() they are merged with node settings and might contain
                // secure settings that should not be copied in here since the new IndexSettings ctor below will barf if we do
                .put(indexSettings.getIndexMetaData().getSettings())
                .put(IndexModule.INDEX_STORE_TYPE_SETTING.getKey(),
                    RandomPicks.randomFrom(random, IndexModule.Type.values()).getSettingsKey()))
            .build();
        final IndexSettings newIndexSettings = new IndexSettings(build, indexSettings.getNodeSettings());
        return new FsDirectoryFactory().newDirectory(newIndexSettings, path);
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

    static final class CloseableDirectory implements Closeable {
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
