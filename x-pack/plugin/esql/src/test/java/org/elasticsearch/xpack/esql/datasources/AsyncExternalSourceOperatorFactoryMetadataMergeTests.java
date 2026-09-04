/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.CloseableIterator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.ExternalMetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.datasources.glob.GlobExpander;
import org.elasticsearch.xpack.esql.datasources.spi.FileList;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReadContext;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.NoConfigFormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.PassThroughRowPositionStrategy;
import org.elasticsearch.xpack.esql.datasources.spi.RowPositionStrategy;
import org.elasticsearch.xpack.esql.datasources.spi.SourceMetadata;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.datasources.spi.StorageProvider;

import java.io.InputStream;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.Executor;

import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Regression tests for the standard-metadata merge behaviour on the three producer paths in
 * {@link AsyncExternalSourceOperatorFactory}: the synthesized standard metadata constants
 * ({@code _index}, {@code _version}, ...) must obey a stable precedence and the single-file
 * paths must populate {@code _version} from the factory's {@code lastModifiedMillis} when no
 * per-file mtime carrier exists.
 */
public class AsyncExternalSourceOperatorFactoryMetadataMergeTests extends ESTestCase {

    private static final BlockFactory TEST_BLOCK_FACTORY = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE)
        .breaker(new NoopCircuitBreaker("test"))
        .build();

    /**
     * Standard metadata names are dedicated: when a reserved key like {@code _index} reaches the
     * per-file merge (only possible from a non-Hive path — {@code HivePartitionDetector} renames
     * colliding partition columns to {@code _partition.*} upstream), the spec-defined constant
     * (dataset name) must win over the smuggled value. The spec promises {@code _index} = dataset
     * name; a layout cannot redefine it.
     */
    public void testSynthesizedIndexWinsOverSmuggledPartitionKeyInMultiFilePath() throws Exception {
        BytesRef hiveIndex = new BytesRef("smuggled-index-loses");
        Page page = runMultiFilePathWithIndex(hiveIndex);
        try {
            int indexBlockChannel = 1; // attributes order: value(data), _index(partition)
            BytesRefBlock indexBlock = page.getBlock(indexBlockChannel);
            BytesRef out = indexBlock.getBytesRef(indexBlock.getFirstValueIndex(0), new BytesRef());
            assertEquals("spec-defined _index (dataset name) must win on reserved-key collision", new BytesRef("dataset-wins"), out);
        } finally {
            page.releaseBlocks();
        }
    }

    /**
     * Regression for {@code _version} being null on the single-file producer path: the factory
     * accepts a {@code lastModifiedMillis} which {@link
     * AsyncExternalSourceOperatorFactory.Builder#lastModifiedMillis} threads into
     * {@code mergeStandardMetadata} so the synthesized {@code _version} constant is non-null on
     * the sync-wrapper / native-async single-file paths (which have no per-file mtime carrier
     * like the slice-queue's {@code FileSplit.partitionValues} or the multi-file path's
     * {@code FileList} entry).
     */
    public void testSingleFileVersionPopulatedFromLastModifiedMillis() throws Exception {
        long mtimeMillis = 1_700_000_000_000L;
        Page page = runSyncWrapperSingleFileWithVersion(mtimeMillis);
        try {
            int versionBlockChannel = 1; // attributes order: value(data), _version(metadata)
            LongBlock versionBlock = page.getBlock(versionBlockChannel);
            assertFalse("single-file _version must not be null when lastModifiedMillis is set", versionBlock.isNull(0));
            assertEquals(mtimeMillis, versionBlock.getLong(versionBlock.getFirstValueIndex(0)));
        } finally {
            page.releaseBlocks();
        }
    }

    private Page runSyncWrapperSingleFileWithVersion(long mtimeMillis) throws Exception {
        StoragePath filePath = StoragePath.of("s3://bucket/data/single.parquet");

        FormatReader formatReader = new SingleIntPageFormatReader();
        StorageProvider storageProvider = new StubStorageProvider();

        List<Attribute> attributes = List.of(
            new FieldAttribute(
                Source.EMPTY,
                "value",
                new EsField("value", DataType.INTEGER, Map.of(), false, EsField.TimeSeriesFieldType.NONE)
            ),
            new ExternalMetadataAttribute(Source.EMPTY, "_version", DataType.LONG)
        );

        Executor sameThread = Runnable::run;
        DriverContext driverContext = newDriverContext();

        AsyncExternalSourceOperatorFactory factory = AsyncExternalSourceOperatorFactory.builder(
            storageProvider,
            formatReader,
            filePath,
            attributes,
            100,
            10,
            sameThread
        )
            // No fileList → sync-wrapper single-file path.
            .lastModifiedMillis(mtimeMillis)
            .producerBlockFactory(TEST_BLOCK_FACTORY)
            .build();

        return drainSinglePage(factory, driverContext);
    }

    private Page runMultiFilePathWithIndex(BytesRef hivePartitionValue) throws Exception {
        StoragePath filePath = StoragePath.of("s3://bucket/data/year=2026/_index=" + hivePartitionValue.utf8ToString() + "/f.parquet");
        List<StorageEntry> entries = List.of(new StorageEntry(filePath, 100, Instant.EPOCH));
        FileList fileList = GlobExpander.fileListOf(entries, "s3://bucket/data/**" + "/f.parquet");

        FormatReader formatReader = new SingleIntPageFormatReader();
        StorageProvider storageProvider = new StubStorageProvider();

        List<Attribute> attributes = List.of(
            new FieldAttribute(
                Source.EMPTY,
                "value",
                new EsField("value", DataType.INTEGER, Map.of(), false, EsField.TimeSeriesFieldType.NONE)
            ),
            // Hive partition column literally named _index. Bound as ExternalMetadataAttribute
            // because that is how the analyzer represents standard metadata names; the producer
            // pipeline routes both Hive partitions and standard-metadata columns through the
            // same partitionColumnNames union.
            new ExternalMetadataAttribute(Source.EMPTY, "_index", DataType.KEYWORD)
        );

        Executor sameThread = Runnable::run;
        DriverContext driverContext = newDriverContext();

        AsyncExternalSourceOperatorFactory factory = AsyncExternalSourceOperatorFactory.builder(
            storageProvider,
            formatReader,
            filePath,
            attributes,
            100,
            10,
            sameThread
        )
            .fileList(fileList)
            .partitionColumnNames(Set.of("_index"))
            .partitionValues(Map.of("_index", hivePartitionValue))
            .datasetName("dataset-wins")
            .producerBlockFactory(TEST_BLOCK_FACTORY)
            .build();

        return drainSinglePage(factory, driverContext);
    }

    private static Page drainSinglePage(AsyncExternalSourceOperatorFactory factory, DriverContext driverContext) {
        SourceOperator operator = factory.get(driverContext);
        Page first = null;
        try {
            while (operator.isFinished() == false) {
                Page p = operator.getOutput();
                if (p != null) {
                    if (first == null) {
                        first = p;
                    } else {
                        p.releaseBlocks();
                    }
                }
            }
        } finally {
            operator.close();
        }
        assertNotNull("expected at least one page", first);
        return first;
    }

    private static DriverContext newDriverContext() {
        DriverContext driverContext = mock(DriverContext.class);
        when(driverContext.blockFactory()).thenReturn(TEST_BLOCK_FACTORY);
        doAnswer(inv -> null).when(driverContext).addAsyncAction();
        doAnswer(inv -> null).when(driverContext).removeAsyncAction();
        return driverContext;
    }

    /** Reader that returns a single one-row Page with the integer 42 in column 0. */
    private static class SingleIntPageFormatReader implements NoConfigFormatReader {
        @Override
        public RowPositionStrategy rowPositionStrategy() {
            return PassThroughRowPositionStrategy.INSTANCE;
        }

        @Override
        public SourceMetadata metadata(StorageObject object) {
            return null;
        }

        @Override
        public CloseableIterator<Page> read(StorageObject object, FormatReadContext context) {
            IntBlock block = TEST_BLOCK_FACTORY.newIntBlockBuilder(1).appendInt(42).build();
            Page page = new Page(block);
            return new CloseableIterator<>() {
                private boolean consumed = false;

                @Override
                public boolean hasNext() {
                    return consumed == false;
                }

                @Override
                public Page next() {
                    if (consumed) {
                        throw new NoSuchElementException();
                    }
                    consumed = true;
                    return page;
                }

                @Override
                public void close() {}
            };
        }

        @Override
        public String formatName() {
            return "test-int";
        }

        @Override
        public List<String> fileExtensions() {
            return List.of(".parquet");
        }

        @Override
        public void close() {}
    }

    /** Minimal StorageProvider: hands back StubStorageObject for any path. */
    private static class StubStorageProvider implements StorageProvider {
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

    private static class StubStorageObject implements StorageObject {
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
