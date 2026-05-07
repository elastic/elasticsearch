/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.CloseableIterator;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.QlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.datasources.glob.GlobExpander;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalSplit;
import org.elasticsearch.xpack.esql.datasources.spi.FileList;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReadContext;
import org.elasticsearch.xpack.esql.datasources.spi.RangeAwareFormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.RangeAwareFormatReader.SplitRange;
import org.elasticsearch.xpack.esql.datasources.spi.RangeReadContext;
import org.elasticsearch.xpack.esql.datasources.spi.SegmentableFormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.SourceMetadata;
import org.elasticsearch.xpack.esql.datasources.spi.SplitDiscoveryContext;
import org.elasticsearch.xpack.esql.datasources.spi.SplitProvider;
import org.elasticsearch.xpack.esql.datasources.spi.SplittableDecompressionCodec;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.datasources.spi.StorageProvider;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNotNull;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNull;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThan;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThanOrEqual;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.In;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThan;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.NotEquals;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class FileSplitProviderTests extends ESTestCase {

    private final FileSplitProvider provider = new FileSplitProvider();

    public void testNFilesProduceNSplits() {
        StorageEntry e1 = new StorageEntry(StoragePath.of("s3://b/a.parquet"), 100, Instant.EPOCH);
        StorageEntry e2 = new StorageEntry(StoragePath.of("s3://b/b.parquet"), 200, Instant.EPOCH);
        StorageEntry e3 = new StorageEntry(StoragePath.of("s3://b/c.parquet"), 300, Instant.EPOCH);
        FileList fileList = GlobExpander.fileListOf(List.of(e1, e2, e3), "s3://b/*.parquet");

        SplitDiscoveryContext ctx = new SplitDiscoveryContext(null, fileList, Map.of(), PartitionMetadata.EMPTY, List.of());
        List<ExternalSplit> splits = provider.discoverSplits(ctx);

        assertEquals(3, splits.size());
        for (int i = 0; i < splits.size(); i++) {
            FileSplit fs = (FileSplit) splits.get(i);
            assertEquals("file", fs.sourceType());
            assertEquals(0, fs.offset());
            assertEquals(".parquet", fs.format());
        }

        assertEquals(StoragePath.of("s3://b/a.parquet"), ((FileSplit) splits.get(0)).path());
        assertEquals(100, ((FileSplit) splits.get(0)).length());
        assertEquals(StoragePath.of("s3://b/b.parquet"), ((FileSplit) splits.get(1)).path());
        assertEquals(200, ((FileSplit) splits.get(1)).length());
        assertEquals(StoragePath.of("s3://b/c.parquet"), ((FileSplit) splits.get(2)).path());
        assertEquals(300, ((FileSplit) splits.get(2)).length());
    }

    public void testPartitionValuesAttached() {
        StoragePath path1 = StoragePath.of("s3://b/year=2024/file.parquet");
        StoragePath path2 = StoragePath.of("s3://b/year=2023/file.parquet");
        StorageEntry e1 = new StorageEntry(path1, 100, Instant.EPOCH);
        StorageEntry e2 = new StorageEntry(path2, 200, Instant.EPOCH);
        FileList fileList = GlobExpander.fileListOf(List.of(e1, e2), "s3://b/year=*/*.parquet");

        PartitionMetadata partitions = new PartitionMetadata(
            Map.of("year", DataType.INTEGER),
            Map.of(path1, Map.of("year", 2024), path2, Map.of("year", 2023))
        );

        SplitDiscoveryContext ctx = new SplitDiscoveryContext(null, fileList, Map.of(), partitions, List.of());
        List<ExternalSplit> splits = provider.discoverSplits(ctx);

        assertEquals(2, splits.size());
        assertEquals(Map.of("year", 2024), ((FileSplit) splits.get(0)).partitionValues());
        assertEquals(Map.of("year", 2023), ((FileSplit) splits.get(1)).partitionValues());
    }

    public void testNoPartitionMetadataProducesEmptyPartitionValues() {
        StorageEntry e1 = new StorageEntry(StoragePath.of("s3://b/file.parquet"), 100, Instant.EPOCH);
        FileList fileList = GlobExpander.fileListOf(List.of(e1), "s3://b/*.parquet");

        SplitDiscoveryContext ctx = new SplitDiscoveryContext(null, fileList, Map.of(), null, List.of());
        List<ExternalSplit> splits = provider.discoverSplits(ctx);

        assertEquals(1, splits.size());
        assertEquals(Map.of(), ((FileSplit) splits.get(0)).partitionValues());
    }

    public void testEmptyFileListProducesNoSplits() {
        SplitDiscoveryContext ctx = new SplitDiscoveryContext(null, FileList.EMPTY, Map.of(), PartitionMetadata.EMPTY, List.of());
        List<ExternalSplit> splits = provider.discoverSplits(ctx);
        assertTrue(splits.isEmpty());
    }

    public void testUnresolvedFileListProducesNoSplits() {
        SplitDiscoveryContext ctx = new SplitDiscoveryContext(null, FileList.UNRESOLVED, Map.of(), PartitionMetadata.EMPTY, List.of());
        List<ExternalSplit> splits = provider.discoverSplits(ctx);
        assertTrue(splits.isEmpty());
    }

    public void testConfigPassedThrough() {
        StorageEntry e1 = new StorageEntry(StoragePath.of("s3://b/file.parquet"), 100, Instant.EPOCH);
        FileList fileList = GlobExpander.fileListOf(List.of(e1), "s3://b/*.parquet");
        Map<String, Object> config = Map.of("endpoint", "https://s3.example.com");

        SplitDiscoveryContext ctx = new SplitDiscoveryContext(null, fileList, config, PartitionMetadata.EMPTY, List.of());
        List<ExternalSplit> splits = provider.discoverSplits(ctx);

        assertEquals(1, splits.size());
        assertEquals(config, ((FileSplit) splits.get(0)).config());
    }

    public void testSingleSplitProvider() {
        List<ExternalSplit> splits = SplitProvider.SINGLE.discoverSplits(
            new SplitDiscoveryContext(null, FileList.EMPTY, Map.of(), null, List.of())
        );
        assertTrue(splits.isEmpty());
    }

    public void testFormatExtraction() {
        StorageEntry csv = new StorageEntry(StoragePath.of("s3://b/data.csv"), 50, Instant.EPOCH);
        StorageEntry noExt = new StorageEntry(StoragePath.of("s3://b/data"), 50, Instant.EPOCH);
        FileList fileList = GlobExpander.fileListOf(List.of(csv, noExt), "s3://b/*");

        SplitDiscoveryContext ctx = new SplitDiscoveryContext(null, fileList, Map.of(), PartitionMetadata.EMPTY, List.of());
        List<ExternalSplit> splits = provider.discoverSplits(ctx);

        assertEquals(2, splits.size());
        assertEquals(".csv", ((FileSplit) splits.get(0)).format());
        assertNull(((FileSplit) splits.get(1)).format());
    }

    // -- L1 partition pruning with full Expression evaluation --

    public void testEqualsFilterPrunesNonMatchingPartitions() {
        StoragePath path2024 = StoragePath.of("s3://b/year=2024/file.parquet");
        StoragePath path2023 = StoragePath.of("s3://b/year=2023/file.parquet");
        FileList fileList = GlobExpander.fileListOf(
            List.of(new StorageEntry(path2024, 100, Instant.EPOCH), new StorageEntry(path2023, 200, Instant.EPOCH)),
            "s3://b/year=*/*.parquet"
        );
        PartitionMetadata partitions = new PartitionMetadata(
            Map.of("year", DataType.INTEGER),
            Map.of(path2024, Map.of("year", 2024), path2023, Map.of("year", 2023))
        );

        Expression filter = new Equals(SRC, fieldAttr("year"), intLiteral(2024));
        SplitDiscoveryContext ctx = new SplitDiscoveryContext(null, fileList, Map.of(), partitions, List.of(filter));
        List<ExternalSplit> splits = provider.discoverSplits(ctx);

        assertEquals(1, splits.size());
        assertEquals(path2024, ((FileSplit) splits.get(0)).path());
    }

    public void testGreaterThanOrEqualFilterPrunes() {
        StoragePath path2024 = StoragePath.of("s3://b/year=2024/file.parquet");
        StoragePath path2020 = StoragePath.of("s3://b/year=2020/file.parquet");
        StoragePath path2023 = StoragePath.of("s3://b/year=2023/file.parquet");
        FileList fileList = GlobExpander.fileListOf(
            List.of(
                new StorageEntry(path2024, 100, Instant.EPOCH),
                new StorageEntry(path2020, 100, Instant.EPOCH),
                new StorageEntry(path2023, 100, Instant.EPOCH)
            ),
            "s3://b/year=*/*.parquet"
        );
        PartitionMetadata partitions = new PartitionMetadata(
            Map.of("year", DataType.INTEGER),
            Map.of(path2024, Map.of("year", 2024), path2020, Map.of("year", 2020), path2023, Map.of("year", 2023))
        );

        Expression filter = new GreaterThanOrEqual(SRC, fieldAttr("year"), intLiteral(2023), null);
        SplitDiscoveryContext ctx = new SplitDiscoveryContext(null, fileList, Map.of(), partitions, List.of(filter));
        List<ExternalSplit> splits = provider.discoverSplits(ctx);

        assertEquals(2, splits.size());
        assertEquals(path2024, ((FileSplit) splits.get(0)).path());
        assertEquals(path2023, ((FileSplit) splits.get(1)).path());
    }

    public void testLessThanFilterPrunes() {
        StoragePath path2024 = StoragePath.of("s3://b/year=2024/file.parquet");
        StoragePath path2020 = StoragePath.of("s3://b/year=2020/file.parquet");
        FileList fileList = GlobExpander.fileListOf(
            List.of(new StorageEntry(path2024, 100, Instant.EPOCH), new StorageEntry(path2020, 100, Instant.EPOCH)),
            "s3://b/year=*/*.parquet"
        );
        PartitionMetadata partitions = new PartitionMetadata(
            Map.of("year", DataType.INTEGER),
            Map.of(path2024, Map.of("year", 2024), path2020, Map.of("year", 2020))
        );

        Expression filter = new LessThan(SRC, fieldAttr("year"), intLiteral(2023), null);
        SplitDiscoveryContext ctx = new SplitDiscoveryContext(null, fileList, Map.of(), partitions, List.of(filter));
        List<ExternalSplit> splits = provider.discoverSplits(ctx);

        assertEquals(1, splits.size());
        assertEquals(path2020, ((FileSplit) splits.get(0)).path());
    }

    public void testNotEqualsFilterPrunes() {
        StoragePath path2024 = StoragePath.of("s3://b/year=2024/file.parquet");
        StoragePath path2023 = StoragePath.of("s3://b/year=2023/file.parquet");
        FileList fileList = GlobExpander.fileListOf(
            List.of(new StorageEntry(path2024, 100, Instant.EPOCH), new StorageEntry(path2023, 200, Instant.EPOCH)),
            "s3://b/year=*/*.parquet"
        );
        PartitionMetadata partitions = new PartitionMetadata(
            Map.of("year", DataType.INTEGER),
            Map.of(path2024, Map.of("year", 2024), path2023, Map.of("year", 2023))
        );

        Expression filter = new NotEquals(SRC, fieldAttr("year"), intLiteral(2024), null);
        SplitDiscoveryContext ctx = new SplitDiscoveryContext(null, fileList, Map.of(), partitions, List.of(filter));
        List<ExternalSplit> splits = provider.discoverSplits(ctx);

        assertEquals(1, splits.size());
        assertEquals(path2023, ((FileSplit) splits.get(0)).path());
    }

    public void testInFilterPrunes() {
        StoragePath path2024 = StoragePath.of("s3://b/year=2024/file.parquet");
        StoragePath path2023 = StoragePath.of("s3://b/year=2023/file.parquet");
        StoragePath path2020 = StoragePath.of("s3://b/year=2020/file.parquet");
        FileList fileList = GlobExpander.fileListOf(
            List.of(
                new StorageEntry(path2024, 100, Instant.EPOCH),
                new StorageEntry(path2023, 100, Instant.EPOCH),
                new StorageEntry(path2020, 100, Instant.EPOCH)
            ),
            "s3://b/year=*/*.parquet"
        );
        PartitionMetadata partitions = new PartitionMetadata(
            Map.of("year", DataType.INTEGER),
            Map.of(path2024, Map.of("year", 2024), path2023, Map.of("year", 2023), path2020, Map.of("year", 2020))
        );

        Expression filter = new In(SRC, fieldAttr("year"), List.of(intLiteral(2023), intLiteral(2024)));
        SplitDiscoveryContext ctx = new SplitDiscoveryContext(null, fileList, Map.of(), partitions, List.of(filter));
        List<ExternalSplit> splits = provider.discoverSplits(ctx);

        assertEquals(2, splits.size());
    }

    public void testCombinedFiltersYearAndMonth() {
        StoragePath pathA = StoragePath.of("s3://b/year=2024/month=6/file.parquet");
        StoragePath pathB = StoragePath.of("s3://b/year=2024/month=1/file.parquet");
        StoragePath pathC = StoragePath.of("s3://b/year=2023/month=6/file.parquet");
        FileList fileList = GlobExpander.fileListOf(
            List.of(
                new StorageEntry(pathA, 100, Instant.EPOCH),
                new StorageEntry(pathB, 100, Instant.EPOCH),
                new StorageEntry(pathC, 100, Instant.EPOCH)
            ),
            "s3://b/year=*/month=*/*.parquet"
        );
        PartitionMetadata partitions = new PartitionMetadata(
            Map.of("year", DataType.INTEGER, "month", DataType.INTEGER),
            Map.of(
                pathA,
                Map.of("year", 2024, "month", 6),
                pathB,
                Map.of("year", 2024, "month", 1),
                pathC,
                Map.of("year", 2023, "month", 6)
            )
        );

        List<Expression> filters = List.of(
            new Equals(SRC, fieldAttr("year"), intLiteral(2024)),
            new GreaterThanOrEqual(SRC, fieldAttr("month"), intLiteral(6), null)
        );
        SplitDiscoveryContext ctx = new SplitDiscoveryContext(null, fileList, Map.of(), partitions, filters);
        List<ExternalSplit> splits = provider.discoverSplits(ctx);

        assertEquals(1, splits.size());
        assertEquals(pathA, ((FileSplit) splits.get(0)).path());
    }

    public void testNonPartitionColumnFilterDoesNotPrune() {
        StoragePath path1 = StoragePath.of("s3://b/year=2024/file.parquet");
        StoragePath path2 = StoragePath.of("s3://b/year=2023/file.parquet");
        FileList fileList = GlobExpander.fileListOf(
            List.of(new StorageEntry(path1, 100, Instant.EPOCH), new StorageEntry(path2, 200, Instant.EPOCH)),
            "s3://b/year=*/*.parquet"
        );
        PartitionMetadata partitions = new PartitionMetadata(
            Map.of("year", DataType.INTEGER),
            Map.of(path1, Map.of("year", 2024), path2, Map.of("year", 2023))
        );

        Expression filter = new Equals(SRC, fieldAttr("name"), new Literal(SRC, new BytesRef("test"), DataType.KEYWORD));
        SplitDiscoveryContext ctx = new SplitDiscoveryContext(null, fileList, Map.of(), partitions, List.of(filter));
        List<ExternalSplit> splits = provider.discoverSplits(ctx);

        assertEquals(2, splits.size());
    }

    public void testNoFilterHintsNoPruning() {
        StoragePath path1 = StoragePath.of("s3://b/year=2024/file.parquet");
        StoragePath path2 = StoragePath.of("s3://b/year=2023/file.parquet");
        FileList fileList = GlobExpander.fileListOf(
            List.of(new StorageEntry(path1, 100, Instant.EPOCH), new StorageEntry(path2, 200, Instant.EPOCH)),
            "s3://b/year=*/*.parquet"
        );
        PartitionMetadata partitions = new PartitionMetadata(
            Map.of("year", DataType.INTEGER),
            Map.of(path1, Map.of("year", 2024), path2, Map.of("year", 2023))
        );

        SplitDiscoveryContext ctx = new SplitDiscoveryContext(null, fileList, Map.of(), partitions, List.of());
        List<ExternalSplit> splits = provider.discoverSplits(ctx);

        assertEquals(2, splits.size());
    }

    public void testMatchesPartitionFiltersAllMatch() {
        Map<String, Object> values = Map.of("year", 2024, "month", 6);
        List<Expression> filters = List.of(
            new Equals(SRC, fieldAttr("year"), intLiteral(2024)),
            new Equals(SRC, fieldAttr("month"), intLiteral(6))
        );
        assertTrue(FileSplitProvider.matchesPartitionFilters(values, filters));
    }

    public void testMatchesPartitionFiltersOneFails() {
        Map<String, Object> values = Map.of("year", 2024, "month", 1);
        List<Expression> filters = List.of(
            new Equals(SRC, fieldAttr("year"), intLiteral(2024)),
            new Equals(SRC, fieldAttr("month"), intLiteral(6))
        );
        assertFalse(FileSplitProvider.matchesPartitionFilters(values, filters));
    }

    public void testEvaluateFilterUnknownExpressionReturnsNull() {
        assertNull(FileSplitProvider.evaluateFilter(new Literal(SRC, true, DataType.BOOLEAN), Map.of("year", 2024)));
    }

    // -- sub-file splitting --

    public void testLargeNdjsonFileIsNotByteSplitEvenWithSmallProviderTarget() {
        long targetSize = 1000;
        FileSplitProvider splitter = new FileSplitProvider(targetSize);

        StorageEntry entry = new StorageEntry(StoragePath.of("s3://b/big.ndjson"), 3500, Instant.EPOCH);
        FileList fileList = GlobExpander.fileListOf(List.of(entry), "s3://b/*.ndjson");

        SplitDiscoveryContext ctx = new SplitDiscoveryContext(null, fileList, Map.of(), PartitionMetadata.EMPTY, List.of());
        List<ExternalSplit> splits = splitter.discoverSplits(ctx);

        assertEquals(1, splits.size());
        FileSplit whole = (FileSplit) splits.get(0);
        assertEquals(0, whole.offset());
        assertEquals(3500, whole.length());
    }

    public void testSmallFileIsNotSplit() {
        long targetSize = 1000;
        FileSplitProvider splitter = new FileSplitProvider(targetSize);

        StorageEntry entry = new StorageEntry(StoragePath.of("s3://b/small.csv"), 500, Instant.EPOCH);
        FileList fileList = GlobExpander.fileListOf(List.of(entry), "s3://b/*.csv");

        SplitDiscoveryContext ctx = new SplitDiscoveryContext(null, fileList, Map.of(), PartitionMetadata.EMPTY, List.of());
        List<ExternalSplit> splits = splitter.discoverSplits(ctx);

        assertEquals(1, splits.size());
        FileSplit fs = (FileSplit) splits.get(0);
        assertEquals(0, fs.offset());
        assertEquals(500, fs.length());
    }

    public void testParquetFileIsNotSplit() {
        long targetSize = 100;
        FileSplitProvider splitter = new FileSplitProvider(targetSize);

        StorageEntry entry = new StorageEntry(StoragePath.of("s3://b/data.parquet"), 5000, Instant.EPOCH);
        FileList fileList = GlobExpander.fileListOf(List.of(entry), "s3://b/*.parquet");

        SplitDiscoveryContext ctx = new SplitDiscoveryContext(null, fileList, Map.of(), PartitionMetadata.EMPTY, List.of());
        List<ExternalSplit> splits = splitter.discoverSplits(ctx);

        assertEquals(1, splits.size());
        assertEquals(0, ((FileSplit) splits.get(0)).offset());
        assertEquals(5000, ((FileSplit) splits.get(0)).length());
    }

    public void testDefaultProviderDoesNotSplitSmallFile() {
        StorageEntry entry = new StorageEntry(StoragePath.of("s3://b/big.csv"), 10_000_000, Instant.EPOCH);
        FileList fileList = GlobExpander.fileListOf(List.of(entry), "s3://b/*.csv");

        SplitDiscoveryContext ctx = new SplitDiscoveryContext(null, fileList, Map.of(), PartitionMetadata.EMPTY, List.of());
        List<ExternalSplit> splits = provider.discoverSplits(ctx);

        assertEquals(1, splits.size());
        assertEquals(0, ((FileSplit) splits.get(0)).offset());
    }

    public void testDefaultProviderDoesNotByteRangeSplitLargeNdjson() {
        long fileSize = 500 * 1024 * 1024L; // 500 MB — above default split threshold
        StorageEntry entry = new StorageEntry(StoragePath.of("s3://b/huge.ndjson"), fileSize, Instant.EPOCH);
        FileList fileList = GlobExpander.fileListOf(List.of(entry), "s3://b/*.ndjson");

        SplitDiscoveryContext ctx = new SplitDiscoveryContext(null, fileList, Map.of(), PartitionMetadata.EMPTY, List.of());
        List<ExternalSplit> splits = provider.discoverSplits(ctx);

        assertEquals(
            "Without format registry and storage, NDJSON cannot discover newline boundaries and stays one split per file",
            1,
            splits.size()
        );
        FileSplit whole = (FileSplit) splits.get(0);
        assertEquals(0, whole.offset());
        assertEquals(fileSize, whole.length());
        assertNull(whole.config().get(FileSplitProvider.FIRST_SPLIT_KEY));
        assertNull(whole.config().get(FileSplitProvider.LAST_SPLIT_KEY));
    }

    public void testNewlineAlignedNdjsonMacroSplitsAreDisjointAndMarked() throws IOException {
        SegmentableFormatReader mockReader = mock(SegmentableFormatReader.class);
        when(mockReader.minimumSegmentSize()).thenReturn(1024L);
        when(mockReader.findNextRecordBoundary(any())).thenAnswer(invocation -> {
            InputStream in = invocation.getArgument(0);
            long consumed = 0;
            int b;
            while ((b = in.read()) >= 0) {
                consumed++;
                if (b == '\n') {
                    return consumed;
                }
            }
            return -1L;
        });

        String line = "abcdefgh\n";
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 4000; i++) {
            sb.append(line);
        }
        byte[] payload = sb.toString().getBytes(StandardCharsets.UTF_8);
        long fileLength = payload.length;

        FormatReaderRegistry formatRegistry = new FormatReaderRegistry(new DecompressionCodecRegistry());
        formatRegistry.registerLazy("ndjson-macro-test", (s, bf) -> mockReader, Settings.EMPTY, null);
        formatRegistry.registerExtension(".ndjson", "ndjson-macro-test");
        formatRegistry.byName("ndjson-macro-test");

        StorageProviderRegistry storageRegistry = createPayloadStorageRegistry(payload);

        long stride = 3000;
        FileSplitProvider splitter = new FileSplitProvider(
            stride,
            new DecompressionCodecRegistry(),
            storageRegistry,
            formatRegistry,
            Settings.EMPTY
        );

        StorageEntry entry = new StorageEntry(StoragePath.of("s3://b/lines.ndjson"), fileLength, Instant.EPOCH);
        FileList fileList = GlobExpander.fileListOf(List.of(entry), "s3://b/*.ndjson");

        SplitDiscoveryContext ctx = new SplitDiscoveryContext(null, fileList, Map.of(), PartitionMetadata.EMPTY, List.of());
        List<ExternalSplit> splits = splitter.discoverSplits(ctx);

        assertTrue("Expected multiple newline-aligned macro splits", splits.size() > 1);
        long expectedOffset = 0;
        for (int i = 0; i < splits.size(); i++) {
            FileSplit fs = (FileSplit) splits.get(i);
            assertEquals("true", fs.config().get(FileSplitProvider.RECORD_ALIGNED_MACRO_SPLIT_KEY));
            assertEquals(expectedOffset, fs.offset());
            expectedOffset += fs.length();
            if (i == 0) {
                assertEquals("true", fs.config().get(FileSplitProvider.FIRST_SPLIT_KEY));
            } else {
                assertNull(fs.config().get(FileSplitProvider.FIRST_SPLIT_KEY));
            }
            if (i == splits.size() - 1) {
                assertEquals("true", fs.config().get(FileSplitProvider.LAST_SPLIT_KEY));
            } else {
                assertNull(fs.config().get(FileSplitProvider.LAST_SPLIT_KEY));
            }
        }
        assertEquals(fileLength, expectedOffset);
        verify(mockReader, atLeastOnce()).findNextRecordBoundary(any());
    }

    public void testTargetSplitSizeConfigOverride() {
        StorageEntry entry = new StorageEntry(StoragePath.of("s3://b/data.ndjson"), 3000, Instant.EPOCH);
        FileList fileList = GlobExpander.fileListOf(List.of(entry), "s3://b/*.ndjson");

        Map<String, Object> config = Map.of(FileSplitProvider.CONFIG_TARGET_SPLIT_SIZE, "1kb");
        SplitDiscoveryContext ctx = new SplitDiscoveryContext(null, fileList, config, PartitionMetadata.EMPTY, List.of());
        List<ExternalSplit> splits = provider.discoverSplits(ctx);

        assertEquals(1, splits.size());
        assertEquals(3000, ((FileSplit) splits.get(0)).length());
    }

    public void testTargetSplitSizeConfigOverrideMb() {
        long fileSize = 300 * 1024 * 1024L; // 300 MB
        StorageEntry entry = new StorageEntry(StoragePath.of("s3://b/data.ndjson"), fileSize, Instant.EPOCH);
        FileList fileList = GlobExpander.fileListOf(List.of(entry), "s3://b/*.ndjson");

        Map<String, Object> config = Map.of(FileSplitProvider.CONFIG_TARGET_SPLIT_SIZE, "32mb");
        SplitDiscoveryContext ctx = new SplitDiscoveryContext(null, fileList, config, PartitionMetadata.EMPTY, List.of());
        List<ExternalSplit> splits = provider.discoverSplits(ctx);

        assertEquals(1, splits.size());
        assertEquals(fileSize, ((FileSplit) splits.get(0)).length());
    }

    public void testTargetSplitSizeInvalidValue() {
        StorageEntry entry = new StorageEntry(StoragePath.of("s3://b/data.ndjson"), 3000, Instant.EPOCH);
        FileList fileList = GlobExpander.fileListOf(List.of(entry), "s3://b/*.ndjson");

        Map<String, Object> config = Map.of(FileSplitProvider.CONFIG_TARGET_SPLIT_SIZE, "not_a_number");
        SplitDiscoveryContext ctx = new SplitDiscoveryContext(null, fileList, config, PartitionMetadata.EMPTY, List.of());
        expectThrows(ElasticsearchParseException.class, () -> provider.discoverSplits(ctx));
    }

    public void testTargetSplitSizeUnitlessIsRejected() {
        StorageEntry entry = new StorageEntry(StoragePath.of("s3://b/data.ndjson"), 3000, Instant.EPOCH);
        FileList fileList = GlobExpander.fileListOf(List.of(entry), "s3://b/*.ndjson");

        Map<String, Object> config = Map.of(FileSplitProvider.CONFIG_TARGET_SPLIT_SIZE, "1024");
        SplitDiscoveryContext ctx = new SplitDiscoveryContext(null, fileList, config, PartitionMetadata.EMPTY, List.of());
        expectThrows(ElasticsearchParseException.class, () -> provider.discoverSplits(ctx));
    }

    public void testTargetSplitSizeZeroIsRejected() {
        StorageEntry entry = new StorageEntry(StoragePath.of("s3://b/data.ndjson"), 3000, Instant.EPOCH);
        FileList fileList = GlobExpander.fileListOf(List.of(entry), "s3://b/*.ndjson");

        Map<String, Object> config = Map.of(FileSplitProvider.CONFIG_TARGET_SPLIT_SIZE, "0b");
        SplitDiscoveryContext ctx = new SplitDiscoveryContext(null, fileList, config, PartitionMetadata.EMPTY, List.of());
        expectThrows(QlIllegalArgumentException.class, () -> provider.discoverSplits(ctx));
    }

    public void testFileSizeExactlyEqualsSplitSizeProducesSingleSplit() {
        long targetSize = 1000;
        FileSplitProvider splitter = new FileSplitProvider(targetSize);

        StorageEntry entry = new StorageEntry(StoragePath.of("s3://b/exact.ndjson"), targetSize, Instant.EPOCH);
        FileList fileList = GlobExpander.fileListOf(List.of(entry), "s3://b/*.ndjson");

        SplitDiscoveryContext ctx = new SplitDiscoveryContext(null, fileList, Map.of(), PartitionMetadata.EMPTY, List.of());
        List<ExternalSplit> splits = splitter.discoverSplits(ctx);

        assertEquals("File exactly equal to split size should produce one split", 1, splits.size());
        FileSplit fs = (FileSplit) splits.get(0);
        assertEquals(0, fs.offset());
        assertEquals(targetSize, fs.length());
    }

    public void testDefaultProviderDoesNotByteRangeSplitLargeParquet() {
        long fileSize = 500 * 1024 * 1024L; // 500 MB — well above the 64MB default
        StorageEntry entry = new StorageEntry(StoragePath.of("s3://b/huge.parquet"), fileSize, Instant.EPOCH);
        FileList fileList = GlobExpander.fileListOf(List.of(entry), "s3://b/*.parquet");

        SplitDiscoveryContext ctx = new SplitDiscoveryContext(null, fileList, Map.of(), PartitionMetadata.EMPTY, List.of());
        List<ExternalSplit> splits = provider.discoverSplits(ctx);

        assertEquals("Parquet must not be byte-range split even with positive default split size", 1, splits.size());
        FileSplit fs = (FileSplit) splits.get(0);
        assertEquals(0, fs.offset());
        assertEquals(fileSize, fs.length());
    }

    public void testFileSplitSizeMatchesOriginal() {
        long targetSize = 1000;
        long fileSize = 2500;
        FileSplitProvider splitter = new FileSplitProvider(targetSize);

        StorageEntry entry = new StorageEntry(StoragePath.of("s3://b/data.ndjson"), fileSize, Instant.EPOCH);
        FileList fileList = GlobExpander.fileListOf(List.of(entry), "s3://b/*.ndjson");

        SplitDiscoveryContext ctx = new SplitDiscoveryContext(null, fileList, Map.of(), PartitionMetadata.EMPTY, List.of());
        List<ExternalSplit> splits = splitter.discoverSplits(ctx);

        long totalBytes = 0;
        for (ExternalSplit split : splits) {
            totalBytes += ((FileSplit) split).length();
        }
        assertEquals(fileSize, totalBytes);
    }

    public void testRangeAwareSplitsForParquet() {
        SplitRange[] fakeRanges = {
            new SplitRange(100, 500, Map.of("_stats.row_count", 100L)),
            new SplitRange(700, 600, Map.of("_stats.row_count", 200L)),
            new SplitRange(1400, 400, Map.of("_stats.row_count", 300L)) };

        RangeAwareFormatReader mockReader = createMockRangeReader(List.of(fakeRanges[0], fakeRanges[1], fakeRanges[2]));

        FormatReaderRegistry formatRegistry = new FormatReaderRegistry(new DecompressionCodecRegistry());
        formatRegistry.registerLazy("parquet", (s, bf) -> mockReader, Settings.EMPTY, null);
        formatRegistry.byName("parquet");

        StorageProviderRegistry storageRegistry = createMockStorageRegistry();

        FileSplitProvider splitter = new FileSplitProvider(
            FileSplitProvider.DEFAULT_TARGET_SPLIT_SIZE,
            new DecompressionCodecRegistry(),
            storageRegistry,
            formatRegistry,
            Settings.EMPTY
        );

        StorageEntry entry = new StorageEntry(StoragePath.of("s3://b/data.parquet"), 2000, Instant.EPOCH);
        FileList fileList = GlobExpander.fileListOf(List.of(entry), "s3://b/*.parquet");

        SplitDiscoveryContext ctx = new SplitDiscoveryContext(null, fileList, Map.of(), PartitionMetadata.EMPTY, List.of());
        List<ExternalSplit> splits = splitter.discoverSplits(ctx);

        assertEquals(3, splits.size());
        for (int i = 0; i < splits.size(); i++) {
            FileSplit fs = (FileSplit) splits.get(i);
            assertEquals(fakeRanges[i].offset(), fs.offset());
            assertEquals(fakeRanges[i].length(), fs.length());
            assertEquals("true", fs.config().get(FileSplitProvider.RANGE_SPLIT_KEY));
            assertEquals("2000", fs.config().get(FileSplitProvider.FILE_LENGTH_KEY));
            assertNotNull(fs.statistics());
            assertEquals(fakeRanges[i].statistics().get("_stats.row_count"), fs.statistics().get("_stats.row_count"));
        }
    }

    public void testRangeAwareFallbackForEmptyRanges() {
        RangeAwareFormatReader mockReader = createMockRangeReader(List.<SplitRange>of());

        FormatReaderRegistry formatRegistry = new FormatReaderRegistry(new DecompressionCodecRegistry());
        formatRegistry.registerLazy("parquet", (s, bf) -> mockReader, Settings.EMPTY, null);
        formatRegistry.byName("parquet");

        StorageProviderRegistry storageRegistry = createMockStorageRegistry();

        FileSplitProvider splitter = new FileSplitProvider(
            FileSplitProvider.DEFAULT_TARGET_SPLIT_SIZE,
            new DecompressionCodecRegistry(),
            storageRegistry,
            formatRegistry,
            Settings.EMPTY
        );

        StorageEntry entry = new StorageEntry(StoragePath.of("s3://b/small.parquet"), 500, Instant.EPOCH);
        FileList fileList = GlobExpander.fileListOf(List.of(entry), "s3://b/*.parquet");

        SplitDiscoveryContext ctx = new SplitDiscoveryContext(null, fileList, Map.of(), PartitionMetadata.EMPTY, List.of());
        List<ExternalSplit> splits = splitter.discoverSplits(ctx);

        assertEquals("Empty ranges should produce single whole-file split", 1, splits.size());
        FileSplit fs = (FileSplit) splits.get(0);
        assertEquals(0, fs.offset());
        assertEquals(500, fs.length());
        assertNull("Whole-file split should not have RANGE_SPLIT_KEY", fs.config().get(FileSplitProvider.RANGE_SPLIT_KEY));
    }

    public void testRangeAwareSingleRowGroupReturnsOneRangeWithStats() {
        SplitRange singleRange = new SplitRange(4, 496, Map.of("_stats.row_count", 1000L, "_stats.columns.id.null_count", 0L));
        RangeAwareFormatReader mockReader = createMockRangeReader(List.of(singleRange));

        FormatReaderRegistry formatRegistry = new FormatReaderRegistry(new DecompressionCodecRegistry());
        formatRegistry.registerLazy("parquet", (s, bf) -> mockReader, Settings.EMPTY, null);
        formatRegistry.byName("parquet");

        StorageProviderRegistry storageRegistry = createMockStorageRegistry();

        FileSplitProvider splitter = new FileSplitProvider(
            FileSplitProvider.DEFAULT_TARGET_SPLIT_SIZE,
            new DecompressionCodecRegistry(),
            storageRegistry,
            formatRegistry,
            Settings.EMPTY
        );

        StorageEntry entry = new StorageEntry(StoragePath.of("s3://b/one_rg.parquet"), 500, Instant.EPOCH);
        FileList fileList = GlobExpander.fileListOf(List.of(entry), "s3://b/*.parquet");

        SplitDiscoveryContext ctx = new SplitDiscoveryContext(null, fileList, Map.of(), PartitionMetadata.EMPTY, List.of());
        List<ExternalSplit> splits = splitter.discoverSplits(ctx);

        assertEquals("Single range should produce one split", 1, splits.size());
        FileSplit fs = (FileSplit) splits.get(0);
        assertEquals(4, fs.offset());
        assertEquals(496, fs.length());
        assertEquals("true", fs.config().get(FileSplitProvider.RANGE_SPLIT_KEY));
        assertNotNull("Split stats should be populated", fs.statistics());
        assertEquals(1000L, fs.statistics().get("_stats.row_count"));
        assertEquals(0L, fs.statistics().get("_stats.columns.id.null_count"));
    }

    public void testMultiFileEachSingleRowGroupProducesSplitsWithStats() {
        SplitRange range1 = new SplitRange(4, 496, Map.of("_stats.row_count", 500L));
        SplitRange range2 = new SplitRange(4, 296, Map.of("_stats.row_count", 300L));
        SplitRange range3 = new SplitRange(4, 196, Map.of("_stats.row_count", 200L));

        RangeAwareFormatReader mockReader = new RangeAwareFormatReader() {
            private int callCount = 0;
            private final List<List<SplitRange>> perFileRanges = List.of(List.of(range1), List.of(range2), List.of(range3));

            @Override
            public List<SplitRange> discoverSplitRanges(StorageObject object) {
                return perFileRanges.get(callCount++);
            }

            @Override
            public CloseableIterator<Page> readRange(StorageObject object, RangeReadContext context) {
                throw new UnsupportedOperationException();
            }

            @Override
            public SourceMetadata metadata(StorageObject object) {
                throw new UnsupportedOperationException();
            }

            @Override
            public CloseableIterator<Page> read(StorageObject object, FormatReadContext context) {
                throw new UnsupportedOperationException();
            }

            @Override
            public String formatName() {
                return "parquet";
            }

            @Override
            public List<String> fileExtensions() {
                return List.of(".parquet");
            }

            @Override
            public void close() {}
        };

        FormatReaderRegistry formatRegistry = new FormatReaderRegistry(new DecompressionCodecRegistry());
        formatRegistry.registerLazy("parquet", (s, bf) -> mockReader, Settings.EMPTY, null);
        formatRegistry.byName("parquet");

        StorageProviderRegistry storageRegistry = createMockStorageRegistry();

        FileSplitProvider splitter = new FileSplitProvider(
            FileSplitProvider.DEFAULT_TARGET_SPLIT_SIZE,
            new DecompressionCodecRegistry(),
            storageRegistry,
            formatRegistry,
            Settings.EMPTY
        );

        FileList fileList = GlobExpander.fileListOf(
            List.of(
                new StorageEntry(StoragePath.of("s3://b/file1.parquet"), 500, Instant.EPOCH),
                new StorageEntry(StoragePath.of("s3://b/file2.parquet"), 300, Instant.EPOCH),
                new StorageEntry(StoragePath.of("s3://b/file3.parquet"), 200, Instant.EPOCH)
            ),
            "s3://b/*.parquet"
        );

        SplitDiscoveryContext ctx = new SplitDiscoveryContext(null, fileList, Map.of(), PartitionMetadata.EMPTY, List.of());
        List<ExternalSplit> splits = splitter.discoverSplits(ctx);

        assertEquals("Each file should produce one split", 3, splits.size());
        long totalRowCount = 0;
        for (ExternalSplit split : splits) {
            FileSplit fs = (FileSplit) split;
            assertNotNull("Each split should have stats", fs.statistics());
            assertNotNull("Each split should have row count", fs.statistics().get("_stats.row_count"));
            totalRowCount += ((Number) fs.statistics().get("_stats.row_count")).longValue();
        }
        assertEquals("Total row count across splits should be sum of all files", 1000L, totalRowCount);
    }

    private static RangeAwareFormatReader createMockRangeReader(List<SplitRange> ranges) {
        return new RangeAwareFormatReader() {
            @Override
            public List<SplitRange> discoverSplitRanges(StorageObject object) {
                return ranges;
            }

            @Override
            public CloseableIterator<Page> readRange(StorageObject object, RangeReadContext context) {
                throw new UnsupportedOperationException("not called during split discovery");
            }

            @Override
            public SourceMetadata metadata(StorageObject object) {
                return null;
            }

            @Override
            public CloseableIterator<Page> read(StorageObject object, FormatReadContext context) {
                return null;
            }

            @Override
            public String formatName() {
                return "parquet";
            }

            @Override
            public List<String> fileExtensions() {
                return List.of(".parquet", ".parq");
            }

            @Override
            public void close() {}
        };
    }

    private static StorageProviderRegistry createMockStorageRegistry() {
        StorageProviderRegistry registry = new StorageProviderRegistry(Settings.EMPTY);
        registry.registerFactory("s3", settings -> new StorageProvider() {
            @Override
            public StorageObject newObject(StoragePath path) {
                return newObject(path, 0);
            }

            @Override
            public StorageObject newObject(StoragePath path, long length) {
                return newObject(path, length, Instant.EPOCH);
            }

            @Override
            public StorageObject newObject(StoragePath path, long length, Instant lastModified) {
                return new StorageObject() {
                    @Override
                    public InputStream newStream() {
                        return new ByteArrayInputStream(new byte[0]);
                    }

                    @Override
                    public InputStream newStream(long position, long len) {
                        return new ByteArrayInputStream(new byte[0]);
                    }

                    @Override
                    public long length() {
                        return length;
                    }

                    @Override
                    public Instant lastModified() {
                        return lastModified;
                    }

                    @Override
                    public boolean exists() {
                        return true;
                    }

                    @Override
                    public StoragePath path() {
                        return path;
                    }
                };
            }

            @Override
            public StorageIterator listObjects(StoragePath prefix, boolean recursive) {
                return new StorageIterator() {
                    @Override
                    public boolean hasNext() {
                        return false;
                    }

                    @Override
                    public StorageEntry next() {
                        throw new java.util.NoSuchElementException();
                    }

                    @Override
                    public void close() {}
                };
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
        });
        return registry;
    }

    /** S3 mock that serves {@code payload} for range reads (newline boundary scanning during split discovery). */
    private static StorageProviderRegistry createPayloadStorageRegistry(byte[] payload) {
        StorageProviderRegistry registry = new StorageProviderRegistry(Settings.EMPTY);
        registry.registerFactory("s3", settings -> new StorageProvider() {
            @Override
            public StorageObject newObject(StoragePath path) {
                return newObject(path, payload.length);
            }

            @Override
            public StorageObject newObject(StoragePath path, long length) {
                return newObject(path, length, Instant.EPOCH);
            }

            @Override
            public StorageObject newObject(StoragePath path, long length, Instant lastModified) {
                assertEquals(payload.length, length);
                return new StorageObject() {
                    @Override
                    public InputStream newStream() {
                        return new ByteArrayInputStream(payload);
                    }

                    @Override
                    public InputStream newStream(long position, long len) {
                        int p = Math.toIntExact(position);
                        int l = Math.toIntExact(len);
                        return new ByteArrayInputStream(payload, p, l);
                    }

                    @Override
                    public long length() {
                        return payload.length;
                    }

                    @Override
                    public Instant lastModified() {
                        return lastModified;
                    }

                    @Override
                    public boolean exists() {
                        return true;
                    }

                    @Override
                    public StoragePath path() {
                        return path;
                    }
                };
            }

            @Override
            public StorageIterator listObjects(StoragePath prefix, boolean recursive) {
                return new StorageIterator() {
                    @Override
                    public boolean hasNext() {
                        return false;
                    }

                    @Override
                    public StorageEntry next() {
                        throw new java.util.NoSuchElementException();
                    }

                    @Override
                    public void close() {}
                };
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
        });
        return registry;
    }

    // -- UNION_BY_NAME file skipping --

    public void testSkipsFileWithNoProjColumnOverlap() {
        StoragePath pathA = StoragePath.of("s3://b/a.parquet");
        StoragePath pathB = StoragePath.of("s3://b/b.parquet");
        StoragePath pathC = StoragePath.of("s3://b/c.parquet");
        FileList fileList = GlobExpander.fileListOf(
            List.of(
                new StorageEntry(pathA, 100, Instant.EPOCH),
                new StorageEntry(pathB, 200, Instant.EPOCH),
                new StorageEntry(pathC, 300, Instant.EPOCH)
            ),
            "s3://b/*.parquet"
        );

        Map<StoragePath, SchemaReconciliation.FileSchemaInfo> schemaInfo = new HashMap<>();
        schemaInfo.put(pathA, new SchemaReconciliation.FileSchemaInfo(List.of(refAttr("id"), refAttr("name")), null, null));
        schemaInfo.put(
            pathB,
            new SchemaReconciliation.FileSchemaInfo(List.of(refAttr("id"), refAttr("name"), refAttr("bonus")), null, null)
        );
        schemaInfo.put(pathC, new SchemaReconciliation.FileSchemaInfo(List.of(refAttr("bonus")), null, null));
        fileList = GlobExpander.withSchemaInfo(fileList, schemaInfo);

        SplitDiscoveryContext ctx = new SplitDiscoveryContext(
            null,
            fileList,
            Map.of(),
            PartitionMetadata.EMPTY,
            List.of(),
            Set.of("id", "name")
        );
        List<ExternalSplit> splits = provider.discoverSplits(ctx);

        assertEquals(2, splits.size());
        assertEquals(pathA, ((FileSplit) splits.get(0)).path());
        assertEquals(pathB, ((FileSplit) splits.get(1)).path());
    }

    public void testKeepsFileWithPartialOverlap() {
        StoragePath pathA = StoragePath.of("s3://b/a.parquet");
        StoragePath pathB = StoragePath.of("s3://b/b.parquet");
        FileList fileList = GlobExpander.fileListOf(
            List.of(new StorageEntry(pathA, 100, Instant.EPOCH), new StorageEntry(pathB, 200, Instant.EPOCH)),
            "s3://b/*.parquet"
        );

        Map<StoragePath, SchemaReconciliation.FileSchemaInfo> schemaInfo = new HashMap<>();
        schemaInfo.put(pathA, new SchemaReconciliation.FileSchemaInfo(List.of(refAttr("id"), refAttr("name")), null, null));
        schemaInfo.put(pathB, new SchemaReconciliation.FileSchemaInfo(List.of(refAttr("bonus"), refAttr("name")), null, null));
        fileList = GlobExpander.withSchemaInfo(fileList, schemaInfo);

        SplitDiscoveryContext ctx = new SplitDiscoveryContext(
            null,
            fileList,
            Map.of(),
            PartitionMetadata.EMPTY,
            List.of(),
            Set.of("id", "name")
        );
        List<ExternalSplit> splits = provider.discoverSplits(ctx);

        assertEquals(2, splits.size());
        assertEquals(pathA, ((FileSplit) splits.get(0)).path());
        assertEquals(pathB, ((FileSplit) splits.get(1)).path());
    }

    public void testKeepsAllFilesWhenProjectedSetEmpty() {
        StoragePath pathA = StoragePath.of("s3://b/a.parquet");
        StoragePath pathB = StoragePath.of("s3://b/b.parquet");
        FileList fileList = GlobExpander.fileListOf(
            List.of(new StorageEntry(pathA, 100, Instant.EPOCH), new StorageEntry(pathB, 200, Instant.EPOCH)),
            "s3://b/*.parquet"
        );

        Map<StoragePath, SchemaReconciliation.FileSchemaInfo> schemaInfo = new HashMap<>();
        schemaInfo.put(pathA, new SchemaReconciliation.FileSchemaInfo(List.of(refAttr("id")), null, null));
        schemaInfo.put(pathB, new SchemaReconciliation.FileSchemaInfo(List.of(refAttr("bonus")), null, null));
        fileList = GlobExpander.withSchemaInfo(fileList, schemaInfo);

        SplitDiscoveryContext ctx = new SplitDiscoveryContext(null, fileList, Map.of(), PartitionMetadata.EMPTY, List.of(), Set.of());
        List<ExternalSplit> splits = provider.discoverSplits(ctx);

        assertEquals("All files retained when projected set is empty (e.g. COUNT(*))", 2, splits.size());
    }

    public void testKeepsAllFilesWhenNoSchemaInfo() {
        StoragePath pathA = StoragePath.of("s3://b/a.parquet");
        StoragePath pathB = StoragePath.of("s3://b/b.parquet");
        FileList fileList = GlobExpander.fileListOf(
            List.of(new StorageEntry(pathA, 100, Instant.EPOCH), new StorageEntry(pathB, 200, Instant.EPOCH)),
            "s3://b/*.parquet"
        );

        SplitDiscoveryContext ctx = new SplitDiscoveryContext(
            null,
            fileList,
            Map.of(),
            PartitionMetadata.EMPTY,
            List.of(),
            Set.of("id", "name")
        );
        List<ExternalSplit> splits = provider.discoverSplits(ctx);

        assertEquals("All files retained when no schema info (FIRST_FILE_WINS)", 2, splits.size());
    }

    public void testKeepsFileWhenProjectionIsOnlyPartitionColumns() {
        StoragePath pathA = StoragePath.of("s3://b/year=2024/a.parquet");
        StoragePath pathB = StoragePath.of("s3://b/year=2024/b.parquet");
        FileList fileList = GlobExpander.fileListOf(
            List.of(new StorageEntry(pathA, 100, Instant.EPOCH), new StorageEntry(pathB, 200, Instant.EPOCH)),
            "s3://b/year=*/*.parquet"
        );

        PartitionMetadata partitions = new PartitionMetadata(
            Map.of("year", DataType.INTEGER),
            Map.of(pathA, Map.of("year", 2024), pathB, Map.of("year", 2024))
        );

        Map<StoragePath, SchemaReconciliation.FileSchemaInfo> schemaInfo = new HashMap<>();
        schemaInfo.put(pathA, new SchemaReconciliation.FileSchemaInfo(List.of(refAttr("id")), null, null));
        schemaInfo.put(pathB, new SchemaReconciliation.FileSchemaInfo(List.of(refAttr("bonus")), null, null));
        fileList = GlobExpander.withSchemaInfo(fileList, schemaInfo);

        SplitDiscoveryContext ctx = new SplitDiscoveryContext(null, fileList, Map.of(), partitions, List.of(), Set.of("year"));
        List<ExternalSplit> splits = provider.discoverSplits(ctx);

        assertEquals("All files retained when projection is only partition columns", 2, splits.size());
    }

    public void testKeepsFileWhenSchemaInfoEntryMissing() {
        StoragePath pathA = StoragePath.of("s3://b/a.parquet");
        StoragePath pathB = StoragePath.of("s3://b/b.parquet");
        FileList fileList = GlobExpander.fileListOf(
            List.of(new StorageEntry(pathA, 100, Instant.EPOCH), new StorageEntry(pathB, 200, Instant.EPOCH)),
            "s3://b/*.parquet"
        );

        Map<StoragePath, SchemaReconciliation.FileSchemaInfo> schemaInfo = new HashMap<>();
        schemaInfo.put(pathA, new SchemaReconciliation.FileSchemaInfo(List.of(refAttr("id"), refAttr("name")), null, null));
        // pathB intentionally has no entry in schemaInfo
        fileList = GlobExpander.withSchemaInfo(fileList, schemaInfo);

        SplitDiscoveryContext ctx = new SplitDiscoveryContext(
            null,
            fileList,
            Map.of(),
            PartitionMetadata.EMPTY,
            List.of(),
            Set.of("id", "name")
        );
        List<ExternalSplit> splits = provider.discoverSplits(ctx);

        assertEquals("File without schema info entry is kept (conservative)", 2, splits.size());
    }

    public void testSkippingWithPartitionPruningCombined() {
        StoragePath pathA = StoragePath.of("s3://b/year=2024/a.parquet");
        StoragePath pathB = StoragePath.of("s3://b/year=2024/b.parquet");
        StoragePath pathC = StoragePath.of("s3://b/year=2023/c.parquet");
        FileList fileList = GlobExpander.fileListOf(
            List.of(
                new StorageEntry(pathA, 100, Instant.EPOCH),
                new StorageEntry(pathB, 200, Instant.EPOCH),
                new StorageEntry(pathC, 300, Instant.EPOCH)
            ),
            "s3://b/year=*/*.parquet"
        );

        PartitionMetadata partitions = new PartitionMetadata(
            Map.of("year", DataType.INTEGER),
            Map.of(pathA, Map.of("year", 2024), pathB, Map.of("year", 2024), pathC, Map.of("year", 2023))
        );

        Map<StoragePath, SchemaReconciliation.FileSchemaInfo> schemaInfo = new HashMap<>();
        schemaInfo.put(pathA, new SchemaReconciliation.FileSchemaInfo(List.of(refAttr("id"), refAttr("name")), null, null));
        schemaInfo.put(pathB, new SchemaReconciliation.FileSchemaInfo(List.of(refAttr("bonus")), null, null));
        schemaInfo.put(pathC, new SchemaReconciliation.FileSchemaInfo(List.of(refAttr("id"), refAttr("name")), null, null));
        fileList = GlobExpander.withSchemaInfo(fileList, schemaInfo);

        Expression yearFilter = new Equals(SRC, fieldAttr("year"), intLiteral(2024));
        SplitDiscoveryContext ctx = new SplitDiscoveryContext(
            null,
            fileList,
            Map.of(),
            partitions,
            List.of(yearFilter),
            Set.of("id", "name")
        );
        List<ExternalSplit> splits = provider.discoverSplits(ctx);

        // pathC pruned by partition filter (year=2023), pathB pruned by column skipping (only 'bonus')
        assertEquals(1, splits.size());
        assertEquals(pathA, ((FileSplit) splits.get(0)).path());
    }

    // -- filter-based file skipping --

    public void testSkipIfFilterOnMissingColumn_comparison() {
        Expression filter = new GreaterThan(SRC, fieldAttr("price"), intLiteral(100), null);
        assertTrue(
            "File missing 'price' column should be skipped for price > 100",
            FileSplitProvider.skipIfFilterOnMissingColumns(List.of(filter), Set.of("name", "id"))
        );
    }

    public void testSkipIfFilterOnMissingColumn_isNull() {
        Expression filter = new IsNull(SRC, fieldAttr("price"));
        assertFalse(
            "IS NULL on missing column evaluates to TRUE — file should NOT be skipped",
            FileSplitProvider.skipIfFilterOnMissingColumns(List.of(filter), Set.of("name", "id"))
        );
    }

    public void testSkipIfFilterOnMissingColumn_isNotNull() {
        Expression filter = new IsNotNull(SRC, fieldAttr("price"));
        assertTrue(
            "IS NOT NULL on missing column evaluates to FALSE — file should be skipped",
            FileSplitProvider.skipIfFilterOnMissingColumns(List.of(filter), Set.of("name", "id"))
        );
    }

    public void testSkipIfFilterOnMissingColumn_columnPresent() {
        Expression filter = new Equals(SRC, fieldAttr("price"), intLiteral(100));
        assertFalse(
            "Column exists in file — should NOT be skipped",
            FileSplitProvider.skipIfFilterOnMissingColumns(List.of(filter), Set.of("price", "name"))
        );
    }

    public void testSkipIfFilterOnMissingColumn_noSchemaInfoIntegration() {
        // When no schema info is available, discoverSplits does not call skipIfFilterOnMissingColumns
        StoragePath pathA = StoragePath.of("s3://b/a.parquet");
        FileList fileList = GlobExpander.fileListOf(List.of(new StorageEntry(pathA, 100, Instant.EPOCH)), "s3://b/*.parquet");

        Expression filter = new GreaterThan(SRC, fieldAttr("price"), intLiteral(100), null);
        SplitDiscoveryContext ctx = new SplitDiscoveryContext(null, fileList, Map.of(), PartitionMetadata.EMPTY, List.of(filter));
        List<ExternalSplit> splits = provider.discoverSplits(ctx);

        assertEquals("File without schema info should NOT be skipped (conservative)", 1, splits.size());
    }

    public void testSkipIfFilterOnMissingColumn_literalOnlyConjunct() {
        Expression filter = new Literal(SRC, true, DataType.BOOLEAN);
        assertFalse(
            "Unrecognized expression should be conservative — should NOT skip",
            FileSplitProvider.skipIfFilterOnMissingColumns(List.of(filter), Set.of("name"))
        );
    }

    public void testSkipIfFilterOnMissingColumn_literalOpColumn() {
        // Literal on the left: 100 < price (equivalent to price > 100)
        Expression filter = new GreaterThan(SRC, intLiteral(100), fieldAttr("price"), null);
        assertTrue(
            "Literal-op-column form with missing column should also skip",
            FileSplitProvider.skipIfFilterOnMissingColumns(List.of(filter), Set.of("name"))
        );
    }

    public void testSkipIfFilterOnMissingColumn_equalsMissing() {
        Expression filter = new Equals(SRC, fieldAttr("status"), intLiteral(1));
        assertTrue("Equals on missing column should skip", FileSplitProvider.skipIfFilterOnMissingColumns(List.of(filter), Set.of("name")));
    }

    public void testSkipIfFilterOnMissingColumn_partitionColumnNotTreatedAsMissing() {
        // Partition column 'year' is not in fileSchema but is in partitionValues
        StoragePath pathA = StoragePath.of("s3://b/year=2024/a.parquet");
        FileList fileList = GlobExpander.fileListOf(List.of(new StorageEntry(pathA, 100, Instant.EPOCH)), "s3://b/year=*/*.parquet");

        PartitionMetadata partitions = new PartitionMetadata(Map.of("year", DataType.INTEGER), Map.of(pathA, Map.of("year", 2024)));

        Map<StoragePath, SchemaReconciliation.FileSchemaInfo> schemaInfo = new HashMap<>();
        schemaInfo.put(pathA, new SchemaReconciliation.FileSchemaInfo(List.of(refAttr("id"), refAttr("name")), null, null));
        fileList = GlobExpander.withSchemaInfo(fileList, schemaInfo);

        Expression yearFilter = new Equals(SRC, fieldAttr("year"), intLiteral(2024));
        SplitDiscoveryContext ctx = new SplitDiscoveryContext(null, fileList, Map.of(), partitions, List.of(yearFilter), Set.of("id"));
        List<ExternalSplit> splits = provider.discoverSplits(ctx);

        assertEquals("Partition column should not be treated as missing — file should NOT be skipped", 1, splits.size());
    }

    public void testSkipIfFilterOnMissingColumn_inExpression() {
        Expression filter = new In(SRC, fieldAttr("status"), List.of(intLiteral(1), intLiteral(2)));
        assertTrue("IN on missing column should skip", FileSplitProvider.skipIfFilterOnMissingColumns(List.of(filter), Set.of("name")));
    }

    public void testSkipIfFilterOnMissingColumn_multipleConjuncts() {
        Expression priceFilter = new GreaterThan(SRC, fieldAttr("price"), intLiteral(100), null);
        Expression nameFilter = new Equals(SRC, fieldAttr("name"), new Literal(SRC, new BytesRef("test"), DataType.KEYWORD));
        // price is missing, name is present — should skip because price > 100 is UNKNOWN → FALSE
        assertTrue(
            "Any conjunct on missing column should trigger skip",
            FileSplitProvider.skipIfFilterOnMissingColumns(List.of(priceFilter, nameFilter), Set.of("name", "id"))
        );
    }

    public void testSkipIfFilterOnMissingColumn_allConjunctsPresent() {
        Expression priceFilter = new GreaterThan(SRC, fieldAttr("price"), intLiteral(100), null);
        Expression nameFilter = new Equals(SRC, fieldAttr("name"), new Literal(SRC, new BytesRef("test"), DataType.KEYWORD));
        assertFalse(
            "All filter columns present — should NOT skip",
            FileSplitProvider.skipIfFilterOnMissingColumns(List.of(priceFilter, nameFilter), Set.of("price", "name"))
        );
    }

    public void testSkipIfFilterOnMissingColumn_combinedWithPartitionPruning() {
        StoragePath pathA = StoragePath.of("s3://b/year=2024/a.parquet");
        StoragePath pathB = StoragePath.of("s3://b/year=2024/b.parquet");
        StoragePath pathC = StoragePath.of("s3://b/year=2023/c.parquet");
        FileList fileList = GlobExpander.fileListOf(
            List.of(
                new StorageEntry(pathA, 100, Instant.EPOCH),
                new StorageEntry(pathB, 200, Instant.EPOCH),
                new StorageEntry(pathC, 300, Instant.EPOCH)
            ),
            "s3://b/year=*/*.parquet"
        );

        PartitionMetadata partitions = new PartitionMetadata(
            Map.of("year", DataType.INTEGER),
            Map.of(pathA, Map.of("year", 2024), pathB, Map.of("year", 2024), pathC, Map.of("year", 2023))
        );

        Map<StoragePath, SchemaReconciliation.FileSchemaInfo> schemaInfo = new HashMap<>();
        schemaInfo.put(pathA, new SchemaReconciliation.FileSchemaInfo(List.of(refAttr("id"), refAttr("price")), null, null));
        schemaInfo.put(pathB, new SchemaReconciliation.FileSchemaInfo(List.of(refAttr("id")), null, null));
        schemaInfo.put(pathC, new SchemaReconciliation.FileSchemaInfo(List.of(refAttr("id"), refAttr("price")), null, null));
        fileList = GlobExpander.withSchemaInfo(fileList, schemaInfo);

        // year=2024 filter prunes pathC; price > 100 filter prunes pathB (missing 'price')
        List<Expression> filters = List.of(
            new Equals(SRC, fieldAttr("year"), intLiteral(2024)),
            new GreaterThan(SRC, fieldAttr("price"), intLiteral(100), null)
        );
        SplitDiscoveryContext ctx = new SplitDiscoveryContext(null, fileList, Map.of(), partitions, filters, Set.of("id", "price"));
        List<ExternalSplit> splits = provider.discoverSplits(ctx);

        assertEquals("Only pathA should survive partition + filter-column pruning", 1, splits.size());
        assertEquals(pathA, ((FileSplit) splits.get(0)).path());
    }

    public void testStorageObjectForSplit_wholeFileUsesRangeWrapper() {
        StoragePath path = StoragePath.of("file:///tmp/x.ndjson.gz");
        StorageObject delegate = mock(StorageObject.class);
        StorageProvider storage = mock(StorageProvider.class);
        when(storage.newObject(path)).thenReturn(delegate);
        FileSplit split = new FileSplit("file", path, 0, 42L, ".gz", Map.of(), Map.of());
        StorageObject got = FileSplitProvider.storageObjectForSplit(storage, split);
        assertThat(got, instanceOf(RangeStorageObject.class));
        RangeStorageObject range = (RangeStorageObject) got;
        assertEquals(0, range.offset());
        assertEquals(42L, range.length());
        verify(storage).newObject(path);
        verify(storage, never()).newObject(eq(path), eq(42L));
    }

    public void testStorageObjectForSplit_firstMacroSegmentUsesRangeWrapper() {
        StoragePath path = StoragePath.of("file:///tmp/x.ndjson.bz2");
        StorageObject delegate = mock(StorageObject.class);
        StorageProvider storage = mock(StorageProvider.class);
        when(storage.newObject(path)).thenReturn(delegate);
        Map<String, Object> cfg = Map.of(FileSplitProvider.FIRST_SPLIT_KEY, "true");
        FileSplit split = new FileSplit("file", path, 0, 10L, ".bz2", cfg, Map.of());
        StorageObject got = FileSplitProvider.storageObjectForSplit(storage, split);
        assertThat(got, instanceOf(RangeStorageObject.class));
        verify(storage).newObject(path);
        verify(storage, never()).newObject(eq(path), eq(10L));
    }

    public void testStorageObjectForSplit_positiveOffsetUsesRangeWrapper() {
        StoragePath path = StoragePath.of("file:///tmp/x.ndjson");
        StorageObject delegate = mock(StorageObject.class);
        StorageProvider storage = mock(StorageProvider.class);
        when(storage.newObject(path)).thenReturn(delegate);
        FileSplit split = new FileSplit("file", path, 7, 10L, ".ndjson", Map.of(), Map.of());
        StorageObject got = FileSplitProvider.storageObjectForSplit(storage, split);
        assertThat(got, instanceOf(RangeStorageObject.class));
        verify(storage).newObject(path);
    }

    /**
     * Multi-group grouping must place each boundary into exactly one macro-split, and groups must
     * cover all boundaries in order without gaps.
     */
    public void testGroupBoundariesProducesContiguousGroups() {
        long[] boundaries = { 0, 10, 20, 35, 55, 80, 110 };
        long fileLength = 150;
        // target = 30: first group 0..2 (span to index 3 = 35 >= 30), then 3..4 (span to idx 5 = 45 >= 30), then last 5..6
        int[][] groups = FileSplitProvider.groupBoundaries(boundaries, fileLength, 30);
        assertTrue("At least two groups expected", groups.length >= 2);

        assertEquals("First group starts at block 0", 0, groups[0][0]);
        for (int i = 1; i < groups.length; i++) {
            assertEquals("Group " + i + " must start where previous ended + 1", groups[i - 1][1] + 1, groups[i][0]);
        }
        assertEquals("Last group ends at the last block index", boundaries.length - 1, groups[groups.length - 1][1]);
    }

    /**
     * End-to-end check for the macro-split disjointness invariant: for a file with a splittable
     * codec, the generated splits must satisfy {@code split[m+1].offset == split[m].offset + split[m].length},
     * never overlap, and collectively cover the full file. Overlaps here would cause record duplication
     * at the NDJSON reader level (regression guard).
     */
    public void testBlockAlignedMacroSplitsAreDisjoint() {
        long fileLength = 1_000_000_000L;
        // Spaced ~5 MB per block for 200 blocks; macro target (32 MB) groups ~7 blocks per macro-split.
        long[] boundaries = new long[200];
        for (int i = 0; i < boundaries.length; i++) {
            boundaries[i] = (long) i * 5_000_000L;
        }

        DecompressionCodecRegistry codecRegistry = new DecompressionCodecRegistry();
        codecRegistry.register(new FakeSplittableCodec(boundaries));

        StorageProviderRegistry storageRegistry = createMockStorageRegistry();
        FormatReaderRegistry formatRegistry = new FormatReaderRegistry(codecRegistry);

        FileSplitProvider splitter = new FileSplitProvider(
            FileSplitProvider.DEFAULT_TARGET_SPLIT_SIZE,
            codecRegistry,
            storageRegistry,
            formatRegistry,
            Settings.EMPTY
        );

        StorageEntry entry = new StorageEntry(StoragePath.of("s3://b/huge.ndjson.bz2"), fileLength, Instant.EPOCH);
        FileList fileList = GlobExpander.fileListOf(List.of(entry), "s3://b/*.ndjson.bz2");
        SplitDiscoveryContext ctx = new SplitDiscoveryContext(null, fileList, Map.of(), PartitionMetadata.EMPTY, List.of());
        List<ExternalSplit> splits = splitter.discoverSplits(ctx);

        assertTrue("Expected multiple macro-splits", splits.size() >= 3);

        FileSplit first = (FileSplit) splits.get(0);
        assertEquals("First split starts at block 0 boundary (== 0)", 0L, first.offset());
        assertEquals("First split is marked first", "true", first.config().get(FileSplitProvider.FIRST_SPLIT_KEY));

        for (int i = 1; i < splits.size(); i++) {
            FileSplit prev = (FileSplit) splits.get(i - 1);
            FileSplit cur = (FileSplit) splits.get(i);
            assertEquals(
                "Split " + i + " must start exactly where split " + (i - 1) + " ends (disjoint, no overlap, no gap)",
                prev.offset() + prev.length(),
                cur.offset()
            );
            assertNull("Only the first split may carry the first-split marker", cur.config().get(FileSplitProvider.FIRST_SPLIT_KEY));
        }

        FileSplit last = (FileSplit) splits.get(splits.size() - 1);
        assertEquals("Last split must cover up to file length", fileLength, last.offset() + last.length());
        assertEquals("Last split is marked last", "true", last.config().get(FileSplitProvider.LAST_SPLIT_KEY));
    }

    /**
     * Small-file fast path: when the block boundaries fit inside a single macro-split target, the
     * splitter must emit exactly one {@link FileSplit} that covers the whole file and carries
     * both the first- and last-split markers. Exercises the {@code isLastMacroSplit && m == 0}
     * branch of {@code tryBlockAlignedSplits}.
     */
    public void testBlockAlignedSingleMacroSplit() {
        long fileLength = 1_000_000L; // well under DEFAULT_MACRO_SPLIT_TARGET (32 MB)
        long[] boundaries = { 0L, 200_000L, 500_000L, 800_000L };

        DecompressionCodecRegistry codecRegistry = new DecompressionCodecRegistry();
        codecRegistry.register(new FakeSplittableCodec(boundaries));

        StorageProviderRegistry storageRegistry = createMockStorageRegistry();
        FormatReaderRegistry formatRegistry = new FormatReaderRegistry(codecRegistry);

        FileSplitProvider splitter = new FileSplitProvider(
            FileSplitProvider.DEFAULT_TARGET_SPLIT_SIZE,
            codecRegistry,
            storageRegistry,
            formatRegistry,
            Settings.EMPTY
        );

        StorageEntry entry = new StorageEntry(StoragePath.of("s3://b/small.ndjson.bz2"), fileLength, Instant.EPOCH);
        FileList fileList = GlobExpander.fileListOf(List.of(entry), "s3://b/*.ndjson.bz2");
        SplitDiscoveryContext ctx = new SplitDiscoveryContext(null, fileList, Map.of(), PartitionMetadata.EMPTY, List.of());
        List<ExternalSplit> splits = splitter.discoverSplits(ctx);

        assertEquals("Small file must produce a single macro-split", 1, splits.size());
        FileSplit only = (FileSplit) splits.get(0);
        assertEquals("Single split must start at offset 0", 0L, only.offset());
        assertEquals("Single split must cover the full file", fileLength, only.length());
        assertEquals("Single split must carry the first-split marker", "true", only.config().get(FileSplitProvider.FIRST_SPLIT_KEY));
        assertEquals("Single split must carry the last-split marker", "true", only.config().get(FileSplitProvider.LAST_SPLIT_KEY));
    }

    /** Fake SplittableDecompressionCodec returning canned block boundaries, for unit-testing split logic. */
    private static final class FakeSplittableCodec implements SplittableDecompressionCodec {
        private final long[] boundaries;

        FakeSplittableCodec(long[] boundaries) {
            this.boundaries = boundaries;
        }

        @Override
        public String name() {
            return "fake-bz2";
        }

        @Override
        public List<String> extensions() {
            return List.of(".bz2");
        }

        @Override
        public InputStream decompress(InputStream raw) {
            return raw;
        }

        @Override
        public long[] findBlockBoundaries(StorageObject object, long start, long end) {
            return boundaries.clone();
        }

        @Override
        public InputStream decompressRange(StorageObject object, long blockStart, long nextBlockStart) {
            return new ByteArrayInputStream(new byte[0]);
        }
    }

    // -- helpers --

    private static final Source SRC = Source.EMPTY;

    private static FieldAttribute fieldAttr(String name) {
        return new FieldAttribute(SRC, name, new EsField(name, DataType.INTEGER, Map.of(), false, EsField.TimeSeriesFieldType.NONE));
    }

    private static Literal intLiteral(int value) {
        return new Literal(SRC, value, DataType.INTEGER);
    }

    private static Attribute refAttr(String name) {
        return new ReferenceAttribute(SRC, name, DataType.KEYWORD);
    }
}
