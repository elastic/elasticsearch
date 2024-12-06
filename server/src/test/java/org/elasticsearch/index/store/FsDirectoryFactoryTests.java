/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.index.store;

import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.store.NIOFSDirectory;
import org.apache.lucene.store.NoLockFactory;
import org.apache.lucene.store.SleepingLockWrapper;
import org.apache.lucene.util.Constants;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardPath;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.IndexSettingsModule;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.BiPredicate;

public class FsDirectoryFactoryTests extends ESTestCase {

    final PreLoadExposingFsDirectoryFactory fsDirectoryFactory = new PreLoadExposingFsDirectoryFactory();

    public void testPreload() throws IOException {
        doTestPreload();
        doTestPreload("nvd", "dvd", "tim");
        doTestPreload("*");
        Settings build = Settings.builder()
            .put(IndexModule.INDEX_STORE_TYPE_SETTING.getKey(), IndexModule.Type.HYBRIDFS.name().toLowerCase(Locale.ROOT))
            .putList(IndexModule.INDEX_STORE_PRE_LOAD_SETTING.getKey(), "dvd", "tmp")
            .build();
        try (Directory directory = newDirectory(build)) {
            assertTrue(FsDirectoryFactory.isHybridFs(directory));
            FsDirectoryFactory.HybridDirectory hybridDirectory = (FsDirectoryFactory.HybridDirectory) FilterDirectory.unwrap(directory);
            assertTrue(FsDirectoryFactory.HybridDirectory.useDelegate("foo.dvd", newIOContext(random())));
            assertTrue(FsDirectoryFactory.HybridDirectory.useDelegate("foo.nvd", newIOContext(random())));
            assertTrue(FsDirectoryFactory.HybridDirectory.useDelegate("foo.tim", newIOContext(random())));
            assertTrue(FsDirectoryFactory.HybridDirectory.useDelegate("foo.tip", newIOContext(random())));
            assertTrue(FsDirectoryFactory.HybridDirectory.useDelegate("foo.cfs", newIOContext(random())));
            assertTrue(FsDirectoryFactory.HybridDirectory.useDelegate("foo.dim", newIOContext(random())));
            assertTrue(FsDirectoryFactory.HybridDirectory.useDelegate("foo.kdd", newIOContext(random())));
            assertTrue(FsDirectoryFactory.HybridDirectory.useDelegate("foo.kdi", newIOContext(random())));
            assertFalse(FsDirectoryFactory.HybridDirectory.useDelegate("foo.kdi", Store.READONCE_CHECKSUM));
            assertTrue(FsDirectoryFactory.HybridDirectory.useDelegate("foo.tmp", newIOContext(random())));
            assertTrue(FsDirectoryFactory.HybridDirectory.useDelegate("foo.fdt__0.tmp", newIOContext(random())));
            MMapDirectory delegate = hybridDirectory.getDelegate();
            assertThat(delegate, Matchers.instanceOf(MMapDirectory.class));
            var func = fsDirectoryFactory.preLoadFuncMap.get(delegate);
            assertTrue(func.test("foo.dvd", newIOContext(random())));
            assertTrue(func.test("foo.tmp", newIOContext(random())));
            fsDirectoryFactory.preLoadFuncMap.clear();
        }
    }

    private Directory newDirectory(Settings settings) throws IOException {
        IndexSettings idxSettings = IndexSettingsModule.newIndexSettings("foo", settings);
        Path tempDir = createTempDir().resolve(idxSettings.getUUID()).resolve("0");
        Files.createDirectories(tempDir);
        ShardPath path = new ShardPath(false, tempDir, tempDir, new ShardId(idxSettings.getIndex(), 0));
        return fsDirectoryFactory.newDirectory(idxSettings, path);
    }

    static class PreLoadExposingFsDirectoryFactory extends FsDirectoryFactory {

        // expose for testing
        final Map<MMapDirectory, BiPredicate<String, IOContext>> preLoadFuncMap = new HashMap<>();

        @Override
        public MMapDirectory setPreload(MMapDirectory mMapDirectory, Set<String> preLoadExtensions) {
            var preLoadFunc = FsDirectoryFactory.getPreloadFunc(preLoadExtensions);
            mMapDirectory.setPreload(preLoadFunc);
            preLoadFuncMap.put(mMapDirectory, preLoadFunc);
            return mMapDirectory;
        }
    }

    private void doTestPreload(String... preload) throws IOException {
        Settings build = Settings.builder()
            .put(IndexModule.INDEX_STORE_TYPE_SETTING.getKey(), "mmapfs")
            .putList(IndexModule.INDEX_STORE_PRE_LOAD_SETTING.getKey(), preload)
            .build();
        Directory directory = newDirectory(build);
        try (Directory dir = directory) {
            assertSame(dir, directory); // prevent warnings
            assertFalse(directory instanceof SleepingLockWrapper);
            var mmapDirectory = FilterDirectory.unwrap(directory);
            assertTrue(directory.toString(), mmapDirectory instanceof MMapDirectory);
            if (preload.length == 0) {
                assertEquals(fsDirectoryFactory.preLoadFuncMap.get(mmapDirectory), MMapDirectory.NO_FILES);
            } else if (Arrays.asList(preload).contains("*")) {
                assertEquals(fsDirectoryFactory.preLoadFuncMap.get(mmapDirectory), MMapDirectory.ALL_FILES);
            } else {
                var func = fsDirectoryFactory.preLoadFuncMap.get(mmapDirectory);
                assertNotEquals(fsDirectoryFactory.preLoadFuncMap.get(mmapDirectory), MMapDirectory.ALL_FILES);
                assertNotEquals(fsDirectoryFactory.preLoadFuncMap.get(mmapDirectory), MMapDirectory.NO_FILES);
                for (String ext : preload) {
                    assertTrue("ext: " + ext, func.test("foo." + ext, newIOContext(random())));
                }
                assertFalse(func.test("XXX", newIOContext(random())));
                mmapDirectory.close();
                expectThrows(AlreadyClosedException.class, () -> mmapDirectory.openInput("foo.tmp", IOContext.DEFAULT));
            }
        }
        expectThrows(
            AlreadyClosedException.class,
            () -> directory.openInput(randomBoolean() && preload.length != 0 ? "foo." + preload[0] : "foo.tmp", IOContext.DEFAULT)
        );
    }

    public void testStoreDirectory() throws IOException {
        Index index = new Index("foo", "fooUUID");
        final Path tempDir = createTempDir().resolve(index.getUUID()).resolve("0");
        // default
        doTestStoreDirectory(tempDir, null, IndexModule.Type.FS);
        // explicit directory impls
        for (IndexModule.Type type : IndexModule.Type.values()) {
            doTestStoreDirectory(tempDir, type.name().toLowerCase(Locale.ROOT), type);
        }
    }

    private void doTestStoreDirectory(Path tempDir, String typeSettingValue, IndexModule.Type type) throws IOException {
        Settings.Builder settingsBuilder = Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current());
        if (typeSettingValue != null) {
            settingsBuilder.put(IndexModule.INDEX_STORE_TYPE_SETTING.getKey(), typeSettingValue);
        }
        Settings settings = settingsBuilder.build();
        IndexSettings indexSettings = IndexSettingsModule.newIndexSettings("foo", settings);
        FsDirectoryFactory service = new FsDirectoryFactory();
        try (Directory directory = service.newFSDirectory(tempDir, NoLockFactory.INSTANCE, indexSettings)) {
            switch (type) {
                case HYBRIDFS:
                    assertTrue(FsDirectoryFactory.isHybridFs(directory));
                    break;
                case SIMPLEFS:
                case NIOFS:
                    assertTrue(type + " " + directory.toString(), directory instanceof NIOFSDirectory);
                    break;
                case MMAPFS:
                    assertTrue(
                        type + " " + directory.getClass().getName() + " " + directory,
                        FilterDirectory.unwrap(directory) instanceof MMapDirectory
                    );
                    break;
                case FS:
                    if (Constants.JRE_IS_64BIT) {
                        assertTrue(FsDirectoryFactory.isHybridFs(directory));
                    } else {
                        assertTrue(directory.toString(), directory instanceof NIOFSDirectory);
                    }
                    break;
                default:
                    fail();
            }
        }
    }
}
