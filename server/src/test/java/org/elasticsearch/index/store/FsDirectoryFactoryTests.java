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
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.store.MergeInfo;
import org.apache.lucene.store.NIOFSDirectory;
import org.apache.lucene.store.NativeFSLockFactory;
import org.apache.lucene.store.NoLockFactory;
import org.apache.lucene.store.SleepingLockWrapper;
import org.apache.lucene.util.Constants;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.codec.vectors.DirectIOWriteContext;
import org.elasticsearch.index.codec.vectors.es818.DirectIOHint;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardPath;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.IndexSettingsModule;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.BiPredicate;
import java.util.zip.CRC32;

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
            // Stored field tmp files that shouldn't preload:
            assertFalse(FsDirectoryFactory.HybridDirectory.useDelegate("foo.fdt__0.tmp", newIOContext(random())));
            assertFalse(FsDirectoryFactory.HybridDirectory.useDelegate("_0.fdt__1.tmp", newIOContext(random())));
            // Stored field tmp files that should preload:
            assertTrue(FsDirectoryFactory.HybridDirectory.useDelegate("_0.fdm__0.tmp", newIOContext(random())));
            assertTrue(FsDirectoryFactory.HybridDirectory.useDelegate("_0.fdx__4.tmp", newIOContext(random())));
            // es819 tsdb doc values tmp files that shouldn't preload:
            assertFalse(FsDirectoryFactory.HybridDirectory.useDelegate("foo.disi__0.tmp", newIOContext(random())));
            assertFalse(FsDirectoryFactory.HybridDirectory.useDelegate("foo.address-data__0.tmp", newIOContext(random())));
            MMapDirectory delegate = hybridDirectory.getDelegate();
            assertThat(delegate, Matchers.instanceOf(MMapDirectory.class));
            var func = fsDirectoryFactory.preLoadFuncMap.get(delegate);
            assertTrue(func.test("foo.dvd", newIOContext(random())));
            assertTrue(func.test("foo.tmp", newIOContext(random())));
            fsDirectoryFactory.preLoadFuncMap.clear();
        }
    }

    /**
     * {@code index.store.fs.direct_io.vector_merge} decides whether merge-context opens of hinted
     * files get a direct I/O delegate; rescore reads (DEFAULT context) keep theirs regardless, since
     * that is {@code on_disk_rescore}'s job, not this setting's. Asserted structurally so the test
     * runs on filesystems without direct I/O support too.
     */
    public void testDirectIOVectorMergeSetting() throws IOException {
        for (boolean merges : new boolean[] { true, false }) {
            Settings settings = Settings.builder()
                .put(IndexModule.INDEX_STORE_TYPE_SETTING.getKey(), IndexModule.Type.HYBRIDFS.name().toLowerCase(Locale.ROOT))
                .put(FsDirectoryFactory.DIRECT_IO_VECTOR_MERGE_SETTING.getKey(), merges)
                .build();
            try (Directory directory = newDirectory(settings)) {
                Directory unwrapped = FilterDirectory.unwrap(directory);
                assumeTrue("test requires hybridfs", unwrapped instanceof FsDirectoryFactory.HybridDirectory);
                FsDirectoryFactory.HybridDirectory hybrid = (FsDirectoryFactory.HybridDirectory) unwrapped;
                assumeTrue("test requires direct I/O support", hybrid.hasDirectIODelegate(IOContext.Context.DEFAULT));
                assertEquals(
                    "vector_merge=" + merges + ": merge delegate presence",
                    merges,
                    hybrid.hasDirectIODelegate(IOContext.Context.MERGE)
                );
                assertEquals(
                    "vector_merge=" + merges + ": isDirectIOForVectorMerges",
                    merges,
                    FsDirectoryFactory.isDirectIOForVectorMerges(directory)
                );
                assertTrue("rescore reads keep their delegate either way", hybrid.hasDirectIODelegate(IOContext.Context.DEFAULT));
            }
        }
    }

    /** Off by default: an unconfigured index merges through the page cache exactly as before. */
    public void testDirectIOVectorMergeDefaultsOff() throws IOException {
        Settings settings = Settings.builder()
            .put(IndexModule.INDEX_STORE_TYPE_SETTING.getKey(), IndexModule.Type.HYBRIDFS.name().toLowerCase(Locale.ROOT))
            .build();
        try (Directory directory = newDirectory(settings)) {
            Directory unwrapped = FilterDirectory.unwrap(directory);
            assumeTrue("test requires hybridfs", unwrapped instanceof FsDirectoryFactory.HybridDirectory);
            FsDirectoryFactory.HybridDirectory hybrid = (FsDirectoryFactory.HybridDirectory) unwrapped;
            assertFalse(hybrid.hasDirectIODelegate(IOContext.Context.MERGE));
            assertFalse(FsDirectoryFactory.isDirectIOForVectorMerges(directory));
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
        public MMapDirectory setMMapFunctions(MMapDirectory mMapDirectory, Set<String> preLoadExtensions) {
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

    /** A merge-context write of raw vectors with the direct I/O hint, as the codec issues it. */
    private static IOContext directIOMergeContext() {
        return new DirectIOWriteContext(
            IOContext.merge(new MergeInfo(randomIntBetween(1, 1000), randomLongBetween(1, 1 << 20), false, -1))
        );
    }

    /**
     * Whether merge-hinted creates through the given directory come out as direct I/O outputs: they
     * do exactly when the directory has its merge delegate and the filesystem accepts direct I/O
     * writes, and the answer is the same for every file of the directory. Decides which of the two
     * paths through {@code HybridDirectory#createOutput} a test exercises: the direct one where
     * supported, the fallback to a buffered output where not (macOS, some container filesystems).
     * Neither outcome is skipped: both paths have to work.
     */
    private static boolean mergeCreatesAreDirect(FsDirectoryFactory.HybridDirectory dir) throws IOException {
        boolean direct;
        try (IndexOutput probe = dir.createOutput("_probe.vec", directIOMergeContext())) {
            probe.writeInt(42);
            direct = probe.toString().contains("DirectIOIndexOutput");
        }
        dir.deleteFile("_probe.vec");
        if (dir.hasDirectIODelegate(IOContext.Context.MERGE) == false) {
            assertFalse("without a merge delegate no create can be direct", direct);
        }
        return direct;
    }

    public void testHybridDirectoryDirectIOWriteRoundTrip() throws IOException {
        Path path = createTempDir("directIOWriteRoundTrip");
        try (
            FsDirectoryFactory.HybridDirectory dir = new FsDirectoryFactory.HybridDirectory(
                NativeFSLockFactory.INSTANCE,
                new MMapDirectory(path),
                0,
                true
            )
        ) {
            boolean direct = mergeCreatesAreDirect(dir);

            // a merge create without the hint, and a hinted create outside a merge, both stay buffered
            try (IndexOutput plain = dir.createOutput("_0_plain.vec", IOContext.merge(new MergeInfo(10, 1024, false, -1)))) {
                assertFalse(plain.toString().contains("DirectIOIndexOutput"));
            }
            try (IndexOutput plain = dir.createOutput("_0_flush.vec", IOContext.DEFAULT.withHints(DirectIOHint.INSTANCE))) {
                assertFalse(plain.toString().contains("DirectIOIndexOutput"));
            }

            // deliberately not a multiple of any block or buffer size, to exercise the unaligned
            // final block (written as a full aligned buffer and then truncated on close)
            byte[] data = new byte[randomIntBetween(1, 4) * 256 * 1024 + (randomBoolean() ? 0 : randomIntBetween(1, 4095))];
            random().nextBytes(data);

            long checksum;
            try (IndexOutput out = dir.createOutput("_0_direct.vec", directIOMergeContext())) {
                assertEquals(
                    "a merge-hinted create must take the same path as every other on this directory",
                    direct,
                    out.toString().contains("DirectIOIndexOutput")
                );
                out.writeBytes(data, data.length);
                assertEquals(data.length, out.getFilePointer());
                checksum = out.getChecksum();
            }

            // logical length must be the truncated length, not the last aligned write
            assertEquals(data.length, dir.fileLength("_0_direct.vec"));

            try (IndexInput in = dir.openInput("_0_direct.vec", IOContext.DEFAULT)) {
                assertEquals(data.length, in.length());
                byte[] read = new byte[data.length];
                in.readBytes(read, 0, read.length);
                assertArrayEquals(data, read);
            }

            CRC32 crc = new CRC32();
            crc.update(data);
            assertEquals("checksum must cover logical bytes only", crc.getValue(), checksum);
        }
    }

    public void testHybridDirectoryDirectIOWriteExistingFile() throws IOException {
        Path path = createTempDir("directIOWriteExisting");
        try (
            FsDirectoryFactory.HybridDirectory dir = new FsDirectoryFactory.HybridDirectory(
                NativeFSLockFactory.INSTANCE,
                new MMapDirectory(path),
                0,
                true
            )
        ) {
            byte[] existing = new byte[randomIntBetween(1, 512)];
            random().nextBytes(existing);
            try (IndexOutput out = dir.createOutput("_0.vec", IOContext.DEFAULT)) {
                out.writeBytes(existing, existing.length);
            }

            // a merge-hinted create over an existing file must fail exactly like the buffered path
            // does, and must leave the existing file untouched, whether the direct open itself
            // rejects it or the filesystem declines direct I/O and the buffered path rejects it:
            // falling back to a buffered create after deleting it would silently clobber the file
            expectThrows(FileAlreadyExistsException.class, () -> dir.createOutput("_0.vec", directIOMergeContext()));

            try (IndexInput in = dir.openInput("_0.vec", IOContext.DEFAULT)) {
                assertEquals(existing.length, in.length());
                byte[] read = new byte[existing.length];
                in.readBytes(read, 0, read.length);
                assertArrayEquals(existing, read);
            }
        }
    }
}
