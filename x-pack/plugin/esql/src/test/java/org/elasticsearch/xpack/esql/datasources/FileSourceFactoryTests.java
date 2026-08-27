/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.glob.GlobExpander;
import org.elasticsearch.xpack.esql.datasources.spi.FileList;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReadContext;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.NoConfigFormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.PassThroughRowPositionStrategy;
import org.elasticsearch.xpack.esql.datasources.spi.RowPositionStrategy;
import org.elasticsearch.xpack.esql.datasources.spi.SourceMetadata;
import org.elasticsearch.xpack.esql.datasources.spi.SourceOperatorContext;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.datasources.spi.StorageProvider;
import org.elasticsearch.xpack.esql.datasources.spi.StorageProviderFactory;

import java.io.InputStream;
import java.time.Instant;
import java.util.List;
import java.util.Map;

/**
 * Regression tests for the wiring inside {@link FileSourceFactory#operatorFactory()}. The
 * single-file producer paths in {@link AsyncExternalSourceOperatorFactory} expose a
 * {@code lastModifiedMillis} builder setter that drives the synthesized {@code _version}
 * constant; the unit-level test in {@code AsyncExternalSourceOperatorFactoryMetadataMergeTests}
 * proves the merge behaviour when the value is set, but does <em>not</em> prove that the
 * production wiring in {@link FileSourceFactory#operatorFactory()} actually populates it from
 * the resolved {@link FileList}. Earlier the production chain dropped the setter call entirely
 * and {@code _version} silently rendered as SQL {@code NULL} on every single-file query. This
 * class pins the call at the factory boundary so future drift surfaces immediately.
 */
public class FileSourceFactoryTests extends ESTestCase {

    public void testSingleFileWiresLastModifiedMillisFromFileList() {
        long mtimeMillis = 1_700_000_000_000L;
        FileSourceFactory fileSourceFactory = newFileSourceFactory();

        StoragePath path = StoragePath.of("s3://bucket/data/single.parquet");
        FileList fileList = GlobExpander.fileListOf(
            List.of(new StorageEntry(path, 100, Instant.ofEpochMilli(mtimeMillis))),
            "s3://bucket/data/single.parquet"
        );

        SourceOperator.SourceOperatorFactory built = fileSourceFactory.operatorFactory()
            .create(SourceOperatorContext.builder().sourceType("file").path(path).executor(Runnable::run).fileList(fileList).build());
        AsyncExternalSourceOperatorFactory factory = (AsyncExternalSourceOperatorFactory) built;
        assertEquals(Long.valueOf(mtimeMillis), factory.lastModifiedMillis());
    }

    public void testMultiFileFirstEntryMtimeWins() {
        long firstMtimeMillis = 1_700_000_000_000L;
        long secondMtimeMillis = 1_800_000_000_000L;
        FileSourceFactory fileSourceFactory = newFileSourceFactory();

        StoragePath first = StoragePath.of("s3://bucket/data/a.parquet");
        StoragePath second = StoragePath.of("s3://bucket/data/b.parquet");
        FileList fileList = GlobExpander.fileListOf(
            List.of(
                new StorageEntry(first, 100, Instant.ofEpochMilli(firstMtimeMillis)),
                new StorageEntry(second, 200, Instant.ofEpochMilli(secondMtimeMillis))
            ),
            "s3://bucket/data/*.parquet"
        );

        SourceOperator.SourceOperatorFactory built = fileSourceFactory.operatorFactory()
            .create(SourceOperatorContext.builder().sourceType("file").path(first).executor(Runnable::run).fileList(fileList).build());
        AsyncExternalSourceOperatorFactory factory = (AsyncExternalSourceOperatorFactory) built;
        // The single-file fallback only fires when the multi-file path has no per-file mtime
        // carrier; passing the first entry's mtime is harmless on the multi-file path (which
        // reads mtime off each FileList entry directly and ignores the builder value) and is the
        // simplest invariant to pin against drift.
        assertEquals(Long.valueOf(firstMtimeMillis), factory.lastModifiedMillis());
    }

    public void testNullFileListYieldsNullMtime() {
        FileSourceFactory fileSourceFactory = newFileSourceFactory();

        StoragePath path = StoragePath.of("s3://bucket/data/single.parquet");
        SourceOperator.SourceOperatorFactory built = fileSourceFactory.operatorFactory()
            .create(SourceOperatorContext.builder().sourceType("file").path(path).executor(Runnable::run).build()
            // No fileList — context.fileList() returns null.
            );
        AsyncExternalSourceOperatorFactory factory = (AsyncExternalSourceOperatorFactory) built;
        assertNull(factory.lastModifiedMillis());
    }

    public void testEmptyFileListYieldsNullMtime() {
        FileSourceFactory fileSourceFactory = newFileSourceFactory();

        StoragePath path = StoragePath.of("s3://bucket/data/single.parquet");
        SourceOperator.SourceOperatorFactory built = fileSourceFactory.operatorFactory()
            .create(SourceOperatorContext.builder().sourceType("file").path(path).executor(Runnable::run).fileList(FileList.EMPTY).build());
        AsyncExternalSourceOperatorFactory factory = (AsyncExternalSourceOperatorFactory) built;
        assertNull(factory.lastModifiedMillis());
    }

    public void testUnresolvedFileListYieldsNullMtime() {
        FileSourceFactory fileSourceFactory = newFileSourceFactory();

        StoragePath path = StoragePath.of("s3://bucket/data/single.parquet");
        SourceOperator.SourceOperatorFactory built = fileSourceFactory.operatorFactory()
            .create(
                SourceOperatorContext.builder().sourceType("file").path(path).executor(Runnable::run).fileList(FileList.UNRESOLVED).build()
            );
        AsyncExternalSourceOperatorFactory factory = (AsyncExternalSourceOperatorFactory) built;
        assertNull(factory.lastModifiedMillis());
    }

    /**
     * The config-aware {@code canHandle} lets the file factory claim an extensionless resource when the
     * query config names an explicit, registered format on a scheme we have a storage provider for. Without
     * the override the path-only form rejects extensionless objects, which is why a dataset registered with
     * an explicit {@code format} on an extensionless resource failed to read end to end.
     */
    public void testCanHandleWithConfigClaimsExtensionlessWhenFormatIsExplicit() {
        FileSourceFactory fileSourceFactory = newFileSourceFactory();

        // Extension still wins on its own, with or without config.
        assertTrue(fileSourceFactory.canHandle("s3://bucket/data.parquet", Map.of()));

        // Extensionless: only claimed when an explicit, registered format is supplied.
        String extensionless = "s3://bucket/data";
        assertFalse("no config -> nothing to infer the format from", fileSourceFactory.canHandle(extensionless, Map.of()));
        assertTrue(
            "explicit registered format claims the extensionless resource",
            fileSourceFactory.canHandle(extensionless, Map.of(FileSourceFactory.CONFIG_FORMAT, "test-parquet"))
        );
        assertFalse(
            "an unregistered format is not claimed",
            fileSourceFactory.canHandle(extensionless, Map.of(FileSourceFactory.CONFIG_FORMAT, "not-a-real-format"))
        );
        assertFalse(
            "no storage provider for the scheme -> not claimed even with an explicit format",
            fileSourceFactory.canHandle("gs://bucket/data", Map.of(FileSourceFactory.CONFIG_FORMAT, "test-parquet"))
        );
    }

    /**
     * An explicit `format` is authoritative: it names the reader directly, so the factory must claim the
     * resource regardless of whether the location has an object name to infer an extension from. Detection
     * (extension-based inference) is only for `auto`/absent, which still require an object name. This mirrors
     * resolveReader, which honors an explicit format unconditionally; canHandle must not reject what
     * resolveReader would resolve.
     */
    public void testCanHandleWithExplicitFormatIsAuthoritativeRegardlessOfObjectName() {
        FileSourceFactory fileSourceFactory = newFileSourceFactory();
        Map<String, Object> explicitFormat = Map.of(FileSourceFactory.CONFIG_FORMAT, "test-parquet");

        // Empty-objectName resources: a bare prefix and a bare authority carry no extension, but an explicit
        // format resolves the reader, so they are claimed.
        assertTrue(
            "trailing-slash prefix with an explicit format is claimed",
            fileSourceFactory.canHandle("s3://bucket/logs/", explicitFormat)
        );
        assertTrue("bare authority with an explicit format is claimed", fileSourceFactory.canHandle("s3://bucket", explicitFormat));

        // file:// has an empty authority but a real absolute path; with an explicit format it must be claimed.
        // Regression: an over-eager host check rejected every file:// URI, breaking extensionless file datasets.
        assertTrue(
            "file:// with an empty authority but a real path is claimed",
            fileSourceFactory.canHandle("file:///opt/data/employees_no_ext", explicitFormat)
        );

        // Regression: a glob already has a non-empty object name ("*") and stays claimed with an explicit format.
        assertTrue("glob with an explicit format is claimed", fileSourceFactory.canHandle("s3://bucket/logs/*", explicitFormat));

        // Without an authoritative format, empty-objectName resources stay on the detection path, which has no
        // extension to work with -> not claimed. `auto` is equivalent to absent here.
        assertFalse(
            "trailing-slash prefix, no format -> detection has no extension",
            fileSourceFactory.canHandle("s3://bucket/logs/", Map.of())
        );
        assertFalse(
            "trailing-slash prefix, format=auto -> falls through to detection, not claimed",
            fileSourceFactory.canHandle("s3://bucket/logs/", Map.of(FileSourceFactory.CONFIG_FORMAT, "auto"))
        );

        // A scheme-only location has no host and is never claimed, even with an explicit format.
        assertFalse("scheme-only location is not claimed", fileSourceFactory.canHandle("s3://", explicitFormat));
    }

    private static FileSourceFactory newFileSourceFactory() {
        FormatReader stubReader = new StubFormatReader();
        FormatReaderRegistry formatRegistry = new FormatReaderRegistry(new DecompressionCodecRegistry());
        formatRegistry.registerLazy("test-parquet", (s, bf) -> stubReader, Settings.EMPTY, null);
        formatRegistry.registerExtension(".parquet", "test-parquet");

        StorageProviderRegistry storageRegistry = new StorageProviderRegistry(Settings.EMPTY);
        StorageProvider stubProvider = new StubStorageProvider();
        storageRegistry.registerFactory("s3", StorageProviderFactory.noConfigKeys(() -> stubProvider));
        // file:// has an empty authority; registering it lets the tests exercise that shape (a real path,
        // no host) as well as the s3 host-bearing shape.
        storageRegistry.registerFactory("file", StorageProviderFactory.noConfigKeys(() -> stubProvider));

        return new FileSourceFactory(storageRegistry, formatRegistry, new DecompressionCodecRegistry(), Settings.EMPTY);
    }

    /** Stub reader: no-op {@code read}, claims {@code .parquet} so the factory registry resolves. */
    private static final class StubFormatReader implements NoConfigFormatReader {
        @Override
        public RowPositionStrategy rowPositionStrategy() {
            return PassThroughRowPositionStrategy.INSTANCE;
        }

        @Override
        public SourceMetadata metadata(StorageObject object) {
            return null;
        }

        @Override
        public org.elasticsearch.compute.operator.CloseableIterator<org.elasticsearch.compute.data.Page> read(
            StorageObject object,
            FormatReadContext context
        ) {
            throw new UnsupportedOperationException("operator is not driven in these tests");
        }

        @Override
        public String formatName() {
            return "test-parquet";
        }

        @Override
        public List<String> fileExtensions() {
            return List.of(".parquet");
        }

        @Override
        public void close() {}
    }

    /** Minimal storage provider: hands back a stub object for any path. Never actually read. */
    private static final class StubStorageProvider implements StorageProvider {
        @Override
        public StorageObject newObject(StoragePath path) {
            return new StubStorageObject(path);
        }

        @Override
        public StorageObject newObject(StoragePath path, long length) {
            return new StubStorageObject(path);
        }

        @Override
        public StorageObject newObject(StoragePath path, long length, Instant lastModified) {
            return new StubStorageObject(path);
        }

        @Override
        public StorageIterator listObjects(StoragePath prefix, boolean recursive) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean exists(StoragePath path) {
            return true;
        }

        @Override
        public List<String> supportedSchemes() {
            return List.of("s3");
        }

        @Override
        public void close() {}
    }

    private static final class StubStorageObject implements StorageObject {
        private final StoragePath path;

        StubStorageObject(StoragePath path) {
            this.path = path;
        }

        @Override
        public InputStream newStream() {
            return InputStream.nullInputStream();
        }

        @Override
        public InputStream newStream(long position, long length) {
            return InputStream.nullInputStream();
        }

        @Override
        public long length() {
            return 0;
        }

        @Override
        public Instant lastModified() {
            return Instant.EPOCH;
        }

        @Override
        public boolean exists() {
            return true;
        }

        @Override
        public StoragePath path() {
            return path;
        }
    }

}
