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
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.CloseableIterator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
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
import org.elasticsearch.xpack.esql.datasources.spi.SplitDiscoveryContext;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.datasources.spi.StorageProvider;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNotNull;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNull;

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
 * Regression tests for the standard-metadata merge behaviour on the producer paths in
 * {@link AsyncExternalSourceOperatorFactory}: the synthesized standard metadata constants
 * must obey a stable precedence against the per-file partition values they overlay.
 */
public class AsyncExternalSourceOperatorFactoryMetadataMergeTests extends ESTestCase {

    private static final BlockFactory TEST_BLOCK_FACTORY = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE)
        .breaker(new NoopCircuitBreaker("test"))
        .build();

    /**
     * Standard metadata names are dedicated: when a reserved key like {@code _index} reaches the
     * per-file merge (only possible from a non-Hive path — {@code HivePartitionDetector} renames
     * colliding partition columns to {@code _partition.*} upstream), the engine constant must win
     * over the smuggled value. On a dataset that constant is SQL NULL, and NULL winning is the
     * point: a layout cannot redefine a reserved name, not even by supplying the only value on
     * offer.
     */
    public void testSynthesizedIndexWinsOverSmuggledPartitionKeyInMultiFilePath() throws Exception {
        BytesRef hiveIndex = new BytesRef("smuggled-index-loses");
        Page page = runMultiFilePathWithIndex(hiveIndex);
        try {
            int indexBlockChannel = 1; // attributes order: value(data), _index(partition)
            assertTrue("the engine's null _index must win on reserved-key collision", page.getBlock(indexBlockChannel).isNull(0));
        } finally {
            page.releaseBlocks();
        }
    }

    /**
     * {@code _index} answers null on a dataset whichever way it binds: as engine metadata it is a null
     * per-file constant, and as a data column absent from the file it is null-filled. What this pins is
     * that discovery and the reader agree about that — a filter that discovery keeps must be one the
     * reader's rows can satisfy, and one it certifies away must be one they cannot.
     */
    public void testIndexBindingAgreesBetweenDiscoveryAndReader() throws Exception {
        StoragePath path = StoragePath.of("s3://bucket/data/file.parquet");
        FileList fileList = GlobExpander.fileListOf(List.of(new StorageEntry(path, 100, Instant.EPOCH)), path.toString());
        Attribute value = new FieldAttribute(
            Source.EMPTY,
            "value",
            new EsField("value", DataType.INTEGER, Map.of(), false, EsField.TimeSeriesFieldType.NONE)
        );
        ExternalSchema fileSchema = new ExternalSchema(List.of(value));
        for (boolean metadata : List.of(false, true)) {
            Attribute index = metadata
                ? new ExternalMetadataAttribute(Source.EMPTY, "_index", DataType.KEYWORD)
                : new FieldAttribute(
                    Source.EMPTY,
                    "_index",
                    new EsField("_index", DataType.KEYWORD, Map.of(), false, EsField.TimeSeriesFieldType.NONE)
                );
            List<Attribute> output = List.of(value, index);
            ExternalSchema querySchema = ExternalSchema.dataAttributesOf(output);
            Map<StoragePath, SchemaReconciliation.FileSchemaInfo> schemas = Map.of(
                path,
                new SchemaReconciliation.FileSchemaInfo(
                    fileSchema,
                    ColumnMapping.alignToQuery(querySchema, fileSchema.attributes(), List.of("value")),
                    null
                )
            );
            int keptByIsNull = new FileSplitProvider().discoverSplits(
                discoveryContext(fileList, schemas, querySchema, output, new IsNull(Source.EMPTY, index))
            ).filesScanned();
            int keptByIsNotNull = new FileSplitProvider().discoverSplits(
                discoveryContext(fileList, schemas, querySchema, output, new IsNotNull(Source.EMPTY, index))
            ).filesScanned();

            AsyncExternalSourceOperatorFactory factory = AsyncExternalSourceOperatorFactory.builder(
                new StubStorageProvider(),
                new SingleIntPageFormatReader(),
                path,
                output,
                100,
                10,
                Runnable::run
            ).fileList(fileList).schemaMap(schemas).producerBlockFactory(TEST_BLOCK_FACTORY).build();
            Page page = drainSinglePage(factory, newDriverContext());
            try {
                assertEquals(1, page.getPositionCount());
                assertTrue("_index is null on a dataset under either binding", page.getBlock(1).isNull(0));
                assertEquals("discovery must keep the file IS NULL can match", 1, keptByIsNull);
                assertEquals("discovery must certify away the file IS NOT NULL cannot match", 0, keptByIsNotNull);
            } finally {
                page.releaseBlocks();
            }
        }
    }

    private static SplitDiscoveryContext discoveryContext(
        FileList fileList,
        Map<StoragePath, SchemaReconciliation.FileSchemaInfo> schemas,
        ExternalSchema querySchema,
        List<Attribute> output,
        Expression filter
    ) {
        return new SplitDiscoveryContext(
            null,
            fileList,
            schemas,
            Map.of(),
            PartitionMetadata.EMPTY,
            List.of(filter),
            querySchema,
            ExternalMetadataColumns.metadataNames(output)
        );
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
