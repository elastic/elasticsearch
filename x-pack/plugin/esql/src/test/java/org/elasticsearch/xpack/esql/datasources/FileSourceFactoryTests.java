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

    private static FileSourceFactory newFileSourceFactory() {
        FormatReader stubReader = new StubFormatReader();
        FormatReaderRegistry formatRegistry = new FormatReaderRegistry(new DecompressionCodecRegistry());
        formatRegistry.registerLazy("test-parquet", (s, bf) -> stubReader, Settings.EMPTY, null);
        formatRegistry.registerExtension(".parquet", "test-parquet");

        StorageProviderRegistry storageRegistry = new StorageProviderRegistry(Settings.EMPTY);
        StorageProvider stubProvider = new StubStorageProvider();
        storageRegistry.registerFactory("s3", StorageProviderFactory.noConfigKeys(() -> stubProvider));

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
