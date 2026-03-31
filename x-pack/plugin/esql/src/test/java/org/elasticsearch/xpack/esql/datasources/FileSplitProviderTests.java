/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.CloseableIterator;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.datasources.spi.ErrorPolicy;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalSplit;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReadContext;
import org.elasticsearch.xpack.esql.datasources.spi.RangeAwareFormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.SourceMetadata;
import org.elasticsearch.xpack.esql.datasources.spi.SplitDiscoveryContext;
import org.elasticsearch.xpack.esql.datasources.spi.SplitProvider;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.datasources.spi.StorageProvider;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThanOrEqual;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.In;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThan;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.NotEquals;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.time.Instant;
import java.util.List;
import java.util.Map;

public class FileSplitProviderTests extends ESTestCase {

    private final FileSplitProvider provider = new FileSplitProvider();

    public void testNFilesProduceNSplits() {
        StorageEntry e1 = new StorageEntry(StoragePath.of("s3://b/a.parquet"), 100, Instant.EPOCH);
        StorageEntry e2 = new StorageEntry(StoragePath.of("s3://b/b.parquet"), 200, Instant.EPOCH);
        StorageEntry e3 = new StorageEntry(StoragePath.of("s3://b/c.parquet"), 300, Instant.EPOCH);
        FileSet fileSet = new FileSet(List.of(e1, e2, e3), "s3://b/*.parquet");

        SplitDiscoveryContext ctx = new SplitDiscoveryContext(null, fileSet, Map.of(), PartitionMetadata.EMPTY, List.of());
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
        FileSet fileSet = new FileSet(List.of(e1, e2), "s3://b/year=*/*.parquet");

        PartitionMetadata partitions = new PartitionMetadata(
            Map.of("year", DataType.INTEGER),
            Map.of(path1, Map.of("year", 2024), path2, Map.of("year", 2023))
        );

        SplitDiscoveryContext ctx = new SplitDiscoveryContext(null, fileSet, Map.of(), partitions, List.of());
        List<ExternalSplit> splits = provider.discoverSplits(ctx);

        assertEquals(2, splits.size());
        assertEquals(Map.of("year", 2024), ((FileSplit) splits.get(0)).partitionValues());
        assertEquals(Map.of("year", 2023), ((FileSplit) splits.get(1)).partitionValues());
    }

    public void testNoPartitionMetadataProducesEmptyPartitionValues() {
        StorageEntry e1 = new StorageEntry(StoragePath.of("s3://b/file.parquet"), 100, Instant.EPOCH);
        FileSet fileSet = new FileSet(List.of(e1), "s3://b/*.parquet");

        SplitDiscoveryContext ctx = new SplitDiscoveryContext(null, fileSet, Map.of(), null, List.of());
        List<ExternalSplit> splits = provider.discoverSplits(ctx);

        assertEquals(1, splits.size());
        assertEquals(Map.of(), ((FileSplit) splits.get(0)).partitionValues());
    }

    public void testEmptyFileSetProducesNoSplits() {
        SplitDiscoveryContext ctx = new SplitDiscoveryContext(null, FileSet.EMPTY, Map.of(), PartitionMetadata.EMPTY, List.of());
        List<ExternalSplit> splits = provider.discoverSplits(ctx);
        assertTrue(splits.isEmpty());
    }

    public void testUnresolvedFileSetProducesNoSplits() {
        SplitDiscoveryContext ctx = new SplitDiscoveryContext(null, FileSet.UNRESOLVED, Map.of(), PartitionMetadata.EMPTY, List.of());
        List<ExternalSplit> splits = provider.discoverSplits(ctx);
        assertTrue(splits.isEmpty());
    }

    public void testConfigPassedThrough() {
        StorageEntry e1 = new StorageEntry(StoragePath.of("s3://b/file.parquet"), 100, Instant.EPOCH);
        FileSet fileSet = new FileSet(List.of(e1), "s3://b/*.parquet");
        Map<String, Object> config = Map.of("endpoint", "https://s3.example.com");

        SplitDiscoveryContext ctx = new SplitDiscoveryContext(null, fileSet, config, PartitionMetadata.EMPTY, List.of());
        List<ExternalSplit> splits = provider.discoverSplits(ctx);

        assertEquals(1, splits.size());
        assertEquals(config, ((FileSplit) splits.get(0)).config());
    }

    public void testSingleSplitProvider() {
        List<ExternalSplit> splits = SplitProvider.SINGLE.discoverSplits(
            new SplitDiscoveryContext(null, FileSet.EMPTY, Map.of(), null, List.of())
        );
        assertTrue(splits.isEmpty());
    }

    public void testFormatExtraction() {
        StorageEntry csv = new StorageEntry(StoragePath.of("s3://b/data.csv"), 50, Instant.EPOCH);
        StorageEntry noExt = new StorageEntry(StoragePath.of("s3://b/data"), 50, Instant.EPOCH);
        FileSet fileSet = new FileSet(List.of(csv, noExt), "s3://b/*");

        SplitDiscoveryContext ctx = new SplitDiscoveryContext(null, fileSet, Map.of(), PartitionMetadata.EMPTY, List.of());
        List<ExternalSplit> splits = provider.discoverSplits(ctx);

        assertEquals(2, splits.size());
        assertEquals(".csv", ((FileSplit) splits.get(0)).format());
        assertNull(((FileSplit) splits.get(1)).format());
    }

    // -- L1 partition pruning with full Expression evaluation --

    public void testEqualsFilterPrunesNonMatchingPartitions() {
        StoragePath path2024 = StoragePath.of("s3://b/year=2024/file.parquet");
        StoragePath path2023 = StoragePath.of("s3://b/year=2023/file.parquet");
        FileSet fileSet = new FileSet(
            List.of(new StorageEntry(path2024, 100, Instant.EPOCH), new StorageEntry(path2023, 200, Instant.EPOCH)),
            "s3://b/year=*/*.parquet"
        );
        PartitionMetadata partitions = new PartitionMetadata(
            Map.of("year", DataType.INTEGER),
            Map.of(path2024, Map.of("year", 2024), path2023, Map.of("year", 2023))
        );

        Expression filter = new Equals(SRC, fieldAttr("year"), intLiteral(2024));
        SplitDiscoveryContext ctx = new SplitDiscoveryContext(null, fileSet, Map.of(), partitions, List.of(filter));
        List<ExternalSplit> splits = provider.discoverSplits(ctx);

        assertEquals(1, splits.size());
        assertEquals(path2024, ((FileSplit) splits.get(0)).path());
    }

    public void testGreaterThanOrEqualFilterPrunes() {
        StoragePath path2024 = StoragePath.of("s3://b/year=2024/file.parquet");
        StoragePath path2020 = StoragePath.of("s3://b/year=2020/file.parquet");
        StoragePath path2023 = StoragePath.of("s3://b/year=2023/file.parquet");
        FileSet fileSet = new FileSet(
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
        SplitDiscoveryContext ctx = new SplitDiscoveryContext(null, fileSet, Map.of(), partitions, List.of(filter));
        List<ExternalSplit> splits = provider.discoverSplits(ctx);

        assertEquals(2, splits.size());
        assertEquals(path2024, ((FileSplit) splits.get(0)).path());
        assertEquals(path2023, ((FileSplit) splits.get(1)).path());
    }

    public void testLessThanFilterPrunes() {
        StoragePath path2024 = StoragePath.of("s3://b/year=2024/file.parquet");
        StoragePath path2020 = StoragePath.of("s3://b/year=2020/file.parquet");
        FileSet fileSet = new FileSet(
            List.of(new StorageEntry(path2024, 100, Instant.EPOCH), new StorageEntry(path2020, 100, Instant.EPOCH)),
            "s3://b/year=*/*.parquet"
        );
        PartitionMetadata partitions = new PartitionMetadata(
            Map.of("year", DataType.INTEGER),
            Map.of(path2024, Map.of("year", 2024), path2020, Map.of("year", 2020))
        );

        Expression filter = new LessThan(SRC, fieldAttr("year"), intLiteral(2023), null);
        SplitDiscoveryContext ctx = new SplitDiscoveryContext(null, fileSet, Map.of(), partitions, List.of(filter));
        List<ExternalSplit> splits = provider.discoverSplits(ctx);

        assertEquals(1, splits.size());
        assertEquals(path2020, ((FileSplit) splits.get(0)).path());
    }

    public void testNotEqualsFilterPrunes() {
        StoragePath path2024 = StoragePath.of("s3://b/year=2024/file.parquet");
        StoragePath path2023 = StoragePath.of("s3://b/year=2023/file.parquet");
        FileSet fileSet = new FileSet(
            List.of(new StorageEntry(path2024, 100, Instant.EPOCH), new StorageEntry(path2023, 200, Instant.EPOCH)),
            "s3://b/year=*/*.parquet"
        );
        PartitionMetadata partitions = new PartitionMetadata(
            Map.of("year", DataType.INTEGER),
            Map.of(path2024, Map.of("year", 2024), path2023, Map.of("year", 2023))
        );

        Expression filter = new NotEquals(SRC, fieldAttr("year"), intLiteral(2024), null);
        SplitDiscoveryContext ctx = new SplitDiscoveryContext(null, fileSet, Map.of(), partitions, List.of(filter));
        List<ExternalSplit> splits = provider.discoverSplits(ctx);

        assertEquals(1, splits.size());
        assertEquals(path2023, ((FileSplit) splits.get(0)).path());
    }

    public void testInFilterPrunes() {
        StoragePath path2024 = StoragePath.of("s3://b/year=2024/file.parquet");
        StoragePath path2023 = StoragePath.of("s3://b/year=2023/file.parquet");
        StoragePath path2020 = StoragePath.of("s3://b/year=2020/file.parquet");
        FileSet fileSet = new FileSet(
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
        SplitDiscoveryContext ctx = new SplitDiscoveryContext(null, fileSet, Map.of(), partitions, List.of(filter));
        List<ExternalSplit> splits = provider.discoverSplits(ctx);

        assertEquals(2, splits.size());
    }

    public void testCombinedFiltersYearAndMonth() {
        StoragePath pathA = StoragePath.of("s3://b/year=2024/month=6/file.parquet");
        StoragePath pathB = StoragePath.of("s3://b/year=2024/month=1/file.parquet");
        StoragePath pathC = StoragePath.of("s3://b/year=2023/month=6/file.parquet");
        FileSet fileSet = new FileSet(
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
        SplitDiscoveryContext ctx = new SplitDiscoveryContext(null, fileSet, Map.of(), partitions, filters);
        List<ExternalSplit> splits = provider.discoverSplits(ctx);

        assertEquals(1, splits.size());
        assertEquals(pathA, ((FileSplit) splits.get(0)).path());
    }

    public void testNonPartitionColumnFilterDoesNotPrune() {
        StoragePath path1 = StoragePath.of("s3://b/year=2024/file.parquet");
        StoragePath path2 = StoragePath.of("s3://b/year=2023/file.parquet");
        FileSet fileSet = new FileSet(
            List.of(new StorageEntry(path1, 100, Instant.EPOCH), new StorageEntry(path2, 200, Instant.EPOCH)),
            "s3://b/year=*/*.parquet"
        );
        PartitionMetadata partitions = new PartitionMetadata(
            Map.of("year", DataType.INTEGER),
            Map.of(path1, Map.of("year", 2024), path2, Map.of("year", 2023))
        );

        Expression filter = new Equals(SRC, fieldAttr("name"), new Literal(SRC, new BytesRef("test"), DataType.KEYWORD));
        SplitDiscoveryContext ctx = new SplitDiscoveryContext(null, fileSet, Map.of(), partitions, List.of(filter));
        List<ExternalSplit> splits = provider.discoverSplits(ctx);

        assertEquals(2, splits.size());
    }

    public void testNoFilterHintsNoPruning() {
        StoragePath path1 = StoragePath.of("s3://b/year=2024/file.parquet");
        StoragePath path2 = StoragePath.of("s3://b/year=2023/file.parquet");
        FileSet fileSet = new FileSet(
            List.of(new StorageEntry(path1, 100, Instant.EPOCH), new StorageEntry(path2, 200, Instant.EPOCH)),
            "s3://b/year=*/*.parquet"
        );
        PartitionMetadata partitions = new PartitionMetadata(
            Map.of("year", DataType.INTEGER),
            Map.of(path1, Map.of("year", 2024), path2, Map.of("year", 2023))
        );

        SplitDiscoveryContext ctx = new SplitDiscoveryContext(null, fileSet, Map.of(), partitions, List.of());
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

    public void testLargeCsvFileIsSplitIntoChunks() {
        long targetSize = 1000;
        FileSplitProvider splitter = new FileSplitProvider(targetSize);

        StorageEntry entry = new StorageEntry(StoragePath.of("s3://b/big.csv"), 3500, Instant.EPOCH);
        FileSet fileSet = new FileSet(List.of(entry), "s3://b/*.csv");

        SplitDiscoveryContext ctx = new SplitDiscoveryContext(null, fileSet, Map.of(), PartitionMetadata.EMPTY, List.of());
        List<ExternalSplit> splits = splitter.discoverSplits(ctx);

        assertEquals(4, splits.size());
        FileSplit s0 = (FileSplit) splits.get(0);
        assertEquals(0, s0.offset());
        assertEquals(1000, s0.length());
        FileSplit s1 = (FileSplit) splits.get(1);
        assertEquals(1000, s1.offset());
        assertEquals(1000, s1.length());
        FileSplit s2 = (FileSplit) splits.get(2);
        assertEquals(2000, s2.offset());
        assertEquals(1000, s2.length());
        FileSplit s3 = (FileSplit) splits.get(3);
        assertEquals(3000, s3.offset());
        assertEquals(500, s3.length());
    }

    public void testSmallFileIsNotSplit() {
        long targetSize = 1000;
        FileSplitProvider splitter = new FileSplitProvider(targetSize);

        StorageEntry entry = new StorageEntry(StoragePath.of("s3://b/small.csv"), 500, Instant.EPOCH);
        FileSet fileSet = new FileSet(List.of(entry), "s3://b/*.csv");

        SplitDiscoveryContext ctx = new SplitDiscoveryContext(null, fileSet, Map.of(), PartitionMetadata.EMPTY, List.of());
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
        FileSet fileSet = new FileSet(List.of(entry), "s3://b/*.parquet");

        SplitDiscoveryContext ctx = new SplitDiscoveryContext(null, fileSet, Map.of(), PartitionMetadata.EMPTY, List.of());
        List<ExternalSplit> splits = splitter.discoverSplits(ctx);

        assertEquals(1, splits.size());
        assertEquals(0, ((FileSplit) splits.get(0)).offset());
        assertEquals(5000, ((FileSplit) splits.get(0)).length());
    }

    public void testDefaultProviderDoesNotSplit() {
        StorageEntry entry = new StorageEntry(StoragePath.of("s3://b/big.csv"), 10_000_000, Instant.EPOCH);
        FileSet fileSet = new FileSet(List.of(entry), "s3://b/*.csv");

        SplitDiscoveryContext ctx = new SplitDiscoveryContext(null, fileSet, Map.of(), PartitionMetadata.EMPTY, List.of());
        List<ExternalSplit> splits = provider.discoverSplits(ctx);

        assertEquals(1, splits.size());
        assertEquals(0, ((FileSplit) splits.get(0)).offset());
    }

    public void testIsSplittableFormat() {
        assertTrue(FileSplitProvider.isSplittableFormat(".csv"));
        assertTrue(FileSplitProvider.isSplittableFormat(".tsv"));
        assertTrue(FileSplitProvider.isSplittableFormat(".ndjson"));
        assertTrue(FileSplitProvider.isSplittableFormat(".jsonl"));
        assertTrue(FileSplitProvider.isSplittableFormat(".txt"));
        assertFalse(FileSplitProvider.isSplittableFormat(".parquet"));
        assertFalse(FileSplitProvider.isSplittableFormat(".avro"));
        assertFalse(FileSplitProvider.isSplittableFormat(null));
    }

    public void testFileSplitSizeMatchesOriginal() {
        long targetSize = 1000;
        long fileSize = 2500;
        FileSplitProvider splitter = new FileSplitProvider(targetSize);

        StorageEntry entry = new StorageEntry(StoragePath.of("s3://b/data.ndjson"), fileSize, Instant.EPOCH);
        FileSet fileSet = new FileSet(List.of(entry), "s3://b/*.ndjson");

        SplitDiscoveryContext ctx = new SplitDiscoveryContext(null, fileSet, Map.of(), PartitionMetadata.EMPTY, List.of());
        List<ExternalSplit> splits = splitter.discoverSplits(ctx);

        long totalBytes = 0;
        for (ExternalSplit split : splits) {
            totalBytes += ((FileSplit) split).length();
        }
        assertEquals(fileSize, totalBytes);
    }

    public void testRangeAwareSplitsForParquet() {
        long[][] fakeRanges = { { 100, 500 }, { 700, 600 }, { 1400, 400 } };

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
        FileSet fileSet = new FileSet(List.of(entry), "s3://b/*.parquet");

        SplitDiscoveryContext ctx = new SplitDiscoveryContext(null, fileSet, Map.of(), PartitionMetadata.EMPTY, List.of());
        List<ExternalSplit> splits = splitter.discoverSplits(ctx);

        assertEquals(3, splits.size());
        for (int i = 0; i < splits.size(); i++) {
            FileSplit fs = (FileSplit) splits.get(i);
            assertEquals(fakeRanges[i][0], fs.offset());
            assertEquals(fakeRanges[i][1], fs.length());
            assertEquals("true", fs.config().get(FileSplitProvider.RANGE_SPLIT_KEY));
            assertEquals("2000", fs.config().get(FileSplitProvider.FILE_LENGTH_KEY));
        }
    }

    public void testRangeAwareFallbackForSingleRowGroup() {
        RangeAwareFormatReader mockReader = createMockRangeReader(List.of());

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
        FileSet fileSet = new FileSet(List.of(entry), "s3://b/*.parquet");

        SplitDiscoveryContext ctx = new SplitDiscoveryContext(null, fileSet, Map.of(), PartitionMetadata.EMPTY, List.of());
        List<ExternalSplit> splits = splitter.discoverSplits(ctx);

        assertEquals("Single row group should produce single split", 1, splits.size());
        FileSplit fs = (FileSplit) splits.get(0);
        assertEquals(0, fs.offset());
        assertEquals(500, fs.length());
        assertNull("Single split should not have RANGE_SPLIT_KEY", fs.config().get(FileSplitProvider.RANGE_SPLIT_KEY));
    }

    private static RangeAwareFormatReader createMockRangeReader(List<long[]> ranges) {
        return new RangeAwareFormatReader() {
            @Override
            public List<long[]> discoverSplitRanges(StorageObject object) {
                return ranges;
            }

            @Override
            public CloseableIterator<Page> readRange(
                StorageObject object,
                List<String> projectedColumns,
                int batchSize,
                long rangeStart,
                long rangeEnd,
                List<Attribute> resolvedAttributes,
                ErrorPolicy errorPolicy
            ) {
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

    // -- helpers --

    private static final Source SRC = Source.EMPTY;

    private static FieldAttribute fieldAttr(String name) {
        return new FieldAttribute(SRC, name, new EsField(name, DataType.INTEGER, Map.of(), false, EsField.TimeSeriesFieldType.NONE));
    }

    private static Literal intLiteral(int value) {
        return new Literal(SRC, value, DataType.INTEGER);
    }
}
