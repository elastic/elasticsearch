/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.CloseableIterator;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.datasource.csv.CsvFormatOptions;
import org.elasticsearch.xpack.esql.datasource.csv.CsvFormatReader;
import org.elasticsearch.xpack.esql.datasources.glob.GlobExpander;
import org.elasticsearch.xpack.esql.datasources.spi.Configured;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalSplit;
import org.elasticsearch.xpack.esql.datasources.spi.FileList;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReadContext;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.PassThroughRowPositionStrategy;
import org.elasticsearch.xpack.esql.datasources.spi.RangeAwareFormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.RangeAwareFormatReader.SplitRange;
import org.elasticsearch.xpack.esql.datasources.spi.RangeReadContext;
import org.elasticsearch.xpack.esql.datasources.spi.RecordSplitter;
import org.elasticsearch.xpack.esql.datasources.spi.RowPositionStrategy;
import org.elasticsearch.xpack.esql.datasources.spi.SegmentableFormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.SourceMetadata;
import org.elasticsearch.xpack.esql.datasources.spi.SplitDiscoveryContext;
import org.elasticsearch.xpack.esql.datasources.spi.SplitDiscoveryResult;
import org.elasticsearch.xpack.esql.datasources.spi.SplitProvider;
import org.elasticsearch.xpack.esql.datasources.spi.SplittableDecompressionCodec;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.datasources.spi.StorageProvider;
import org.elasticsearch.xpack.esql.datasources.spi.StorageProviderFactory;
import org.elasticsearch.xpack.esql.expression.predicate.logical.And;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Not;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Or;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNotNull;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNull;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThan;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThanOrEqual;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.In;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThan;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.NotEquals;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
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
        List<ExternalSplit> splits = provider.discoverSplits(ctx).splits();

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

    public void testFilesScannedCountsSurvivingFiles() {
        StorageEntry e1 = new StorageEntry(StoragePath.of("s3://b/a.parquet"), 100, Instant.EPOCH);
        StorageEntry e2 = new StorageEntry(StoragePath.of("s3://b/b.parquet"), 200, Instant.EPOCH);
        StorageEntry e3 = new StorageEntry(StoragePath.of("s3://b/c.parquet"), 300, Instant.EPOCH);
        FileList fileList = GlobExpander.fileListOf(List.of(e1, e2, e3), "s3://b/*.parquet");

        SplitDiscoveryContext ctx = new SplitDiscoveryContext(null, fileList, Map.of(), PartitionMetadata.EMPTY, List.of());
        SplitDiscoveryResult result = provider.discoverSplits(ctx);

        // Whole-file splits: one split per file, so files_scanned matches the split count here.
        assertEquals(3, result.filesScanned());
        assertEquals(3, result.splits().size());
    }

    public void testFilesScannedExcludesPartitionPrunedFiles() {
        StoragePath path2024 = StoragePath.of("s3://b/year=2024/file.parquet");
        StoragePath path2023 = StoragePath.of("s3://b/year=2023/file.parquet");
        StorageEntry e1 = new StorageEntry(path2024, 100, Instant.EPOCH);
        StorageEntry e2 = new StorageEntry(path2023, 200, Instant.EPOCH);
        FileList fileList = GlobExpander.fileListOf(List.of(e1, e2), "s3://b/year=*/*.parquet");

        PartitionMetadata partitions = new PartitionMetadata(
            Map.of("year", DataType.INTEGER),
            Map.of(path2024, Map.of("year", 2024), path2023, Map.of("year", 2023))
        );

        FieldAttribute year = new FieldAttribute(
            Source.EMPTY,
            "year",
            new EsField("year", DataType.INTEGER, Map.of(), false, EsField.TimeSeriesFieldType.NONE)
        );
        Expression filter = new Equals(Source.EMPTY, year, new Literal(Source.EMPTY, 2024, DataType.INTEGER));

        SplitDiscoveryContext ctx = new SplitDiscoveryContext(null, fileList, Map.of(), partitions, List.of(filter));
        SplitDiscoveryResult result = provider.discoverSplits(ctx);

        // Only the year=2024 partition survives the filter, so just one file is scanned.
        assertEquals(1, result.filesScanned());
        assertEquals(1, result.splits().size());
    }

    /**
     * The signal the coordinator relies on to swap in {@link FileList#EMPTY}: when a partition filter prunes every
     * file of a resolved, non-empty fileList, {@link FileSplitProvider} emits zero splits, reports
     * {@code filesScanned == 0}, and flags the result {@code exhaustivelyPruned} — because the files were removed by a
     * row-count-preserving filter contradiction, so a full read would emit zero rows too.
     */
    public void testAllPartitionsPrunedYieldsNoSplitsAndExhaustivePrune() {
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
        // No file carries year == 1999, so every partition is pruned.
        Expression filter = new Equals(SRC, fieldAttr("year"), intLiteral(1999));

        SplitDiscoveryContext ctx = new SplitDiscoveryContext(null, fileList, Map.of(), partitions, List.of(filter));
        SplitDiscoveryResult result = provider.discoverSplits(ctx);

        assertTrue("a zero-match partition filter must prune every file", result.splits().isEmpty());
        assertEquals("filesScanned reports survivors only, so it must be 0 when nothing survives", 0, result.filesScanned());
        assertTrue("a filter-contradiction prune of a resolved, non-empty fileList is exhaustive", result.exhaustivelyPruned());
        assertTrue("the fileList itself is still resolved and non-empty", fileList.isResolved() && fileList.fileCount() > 0);
    }

    /**
     * An unresolved or already-empty fileList yields zero splits, but that is NOT an exhaustive prune: there is
     * nothing to read anyway (empty) or the listing happens at runtime (unresolved), so the coordinator must not
     * treat it as "pruned to nothing" and swap in {@link FileList#EMPTY}.
     */
    public void testEmptyOrUnresolvedFileListIsNotExhaustivePrune() {
        SplitDiscoveryContext empty = new SplitDiscoveryContext(null, FileList.EMPTY, Map.of(), PartitionMetadata.EMPTY, List.of());
        assertFalse("an already-empty fileList is not an exhaustive prune", provider.discoverSplits(empty).exhaustivelyPruned());

        SplitDiscoveryContext unresolved = new SplitDiscoveryContext(
            null,
            FileList.UNRESOLVED,
            Map.of(),
            PartitionMetadata.EMPTY,
            List.of()
        );
        assertFalse("an unresolved fileList is not an exhaustive prune", provider.discoverSplits(unresolved).exhaustivelyPruned());
    }

    public void testFilesScannedZeroForEmptyOrUnresolved() {
        SplitDiscoveryContext empty = new SplitDiscoveryContext(null, FileList.EMPTY, Map.of(), PartitionMetadata.EMPTY, List.of());
        assertEquals(0, provider.discoverSplits(empty).filesScanned());

        SplitDiscoveryContext unresolved = new SplitDiscoveryContext(
            null,
            FileList.UNRESOLVED,
            Map.of(),
            PartitionMetadata.EMPTY,
            List.of()
        );
        assertEquals(0, provider.discoverSplits(unresolved).filesScanned());
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
        List<ExternalSplit> splits = provider.discoverSplits(ctx).splits();

        assertEquals(2, splits.size());
        Map<String, Object> split0Values = ((FileSplit) splits.get(0)).partitionValues();
        assertEquals(2024, split0Values.get("year"));
        assertTrue(split0Values.containsKey("_file.path"));
        Map<String, Object> split1Values = ((FileSplit) splits.get(1)).partitionValues();
        assertEquals(2023, split1Values.get("year"));
        assertTrue(split1Values.containsKey("_file.path"));
    }

    public void testNoPartitionMetadataStillHasFileMetadata() {
        StorageEntry e1 = new StorageEntry(StoragePath.of("s3://b/file.parquet"), 100, Instant.EPOCH);
        FileList fileList = GlobExpander.fileListOf(List.of(e1), "s3://b/*.parquet");

        SplitDiscoveryContext ctx = new SplitDiscoveryContext(null, fileList, Map.of(), null, List.of());
        List<ExternalSplit> splits = provider.discoverSplits(ctx).splits();

        assertEquals(1, splits.size());
        Map<String, Object> values = ((FileSplit) splits.get(0)).partitionValues();
        assertTrue(values.containsKey("_file.path"));
        assertTrue(values.containsKey("_file.name"));
        assertTrue(values.containsKey("_file.directory"));
        assertTrue(values.containsKey("_file.size"));
        assertTrue(values.containsKey("_file.modified"));
        assertEquals(100L, values.get("_file.size"));
    }

    public void testEmptyFileListProducesNoSplits() {
        SplitDiscoveryContext ctx = new SplitDiscoveryContext(null, FileList.EMPTY, Map.of(), PartitionMetadata.EMPTY, List.of());
        List<ExternalSplit> splits = provider.discoverSplits(ctx).splits();
        assertTrue(splits.isEmpty());
    }

    public void testUnresolvedFileListProducesNoSplits() {
        SplitDiscoveryContext ctx = new SplitDiscoveryContext(null, FileList.UNRESOLVED, Map.of(), PartitionMetadata.EMPTY, List.of());
        List<ExternalSplit> splits = provider.discoverSplits(ctx).splits();
        assertTrue(splits.isEmpty());
    }

    public void testConfigPassedThrough() {
        StorageEntry e1 = new StorageEntry(StoragePath.of("s3://b/file.parquet"), 100, Instant.EPOCH);
        FileList fileList = GlobExpander.fileListOf(List.of(e1), "s3://b/*.parquet");
        Map<String, Object> config = Map.of("endpoint", "https://s3.example.com");

        SplitDiscoveryContext ctx = new SplitDiscoveryContext(null, fileList, config, PartitionMetadata.EMPTY, List.of());
        List<ExternalSplit> splits = provider.discoverSplits(ctx).splits();

        assertEquals(1, splits.size());
        Map<String, Object> splitConfig = ((FileSplit) splits.get(0)).config();
        // Every caller-supplied entry reaches the split untouched. The split config is a superset, not a
        // copy: the provider also stamps this split's position in its file (here, a whole-file split, so
        // both first and last), which readers need for the record-boundary protocol.
        assertEquals("https://s3.example.com", splitConfig.get("endpoint"));
        assertEquals("true", splitConfig.get(FileSplitProvider.FIRST_SPLIT_KEY));
        assertEquals("true", splitConfig.get(FileSplitProvider.LAST_SPLIT_KEY));
    }

    public void testSingleSplitProvider() {
        List<ExternalSplit> splits = SplitProvider.SINGLE.discoverSplits(
            new SplitDiscoveryContext(null, FileList.EMPTY, Map.of(), null, List.of())
        ).splits();
        assertTrue(splits.isEmpty());
    }

    public void testFormatExtraction() {
        StorageEntry csv = new StorageEntry(StoragePath.of("s3://b/data.csv"), 50, Instant.EPOCH);
        StorageEntry noExt = new StorageEntry(StoragePath.of("s3://b/data"), 50, Instant.EPOCH);
        FileList fileList = GlobExpander.fileListOf(List.of(csv, noExt), "s3://b/*");

        SplitDiscoveryContext ctx = new SplitDiscoveryContext(null, fileList, Map.of(), PartitionMetadata.EMPTY, List.of());
        List<ExternalSplit> splits = provider.discoverSplits(ctx).splits();

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
        List<ExternalSplit> splits = provider.discoverSplits(ctx).splits();

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
        List<ExternalSplit> splits = provider.discoverSplits(ctx).splits();

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
        List<ExternalSplit> splits = provider.discoverSplits(ctx).splits();

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
        List<ExternalSplit> splits = provider.discoverSplits(ctx).splits();

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
        List<ExternalSplit> splits = provider.discoverSplits(ctx).splits();

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
        List<ExternalSplit> splits = provider.discoverSplits(ctx).splits();

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
        List<ExternalSplit> splits = provider.discoverSplits(ctx).splits();

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
        List<ExternalSplit> splits = provider.discoverSplits(ctx).splits();

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

    public void testEvaluateFilterIsNullOnNullPartitionMatches() {
        Map<String, Object> values = new HashMap<>();
        values.put("lang", null);
        Expression filter = new IsNull(SRC, fieldAttr("lang"));
        assertEquals(Boolean.TRUE, FileSplitProvider.evaluateFilter(filter, values));
    }

    public void testEvaluateFilterIsNullOnNonNullPartitionDoesNotMatch() {
        Expression filter = new IsNull(SRC, fieldAttr("lang"));
        assertEquals(Boolean.FALSE, FileSplitProvider.evaluateFilter(filter, Map.of("lang", 3)));
    }

    public void testEvaluateFilterIsNotNullOnNullPartitionDoesNotMatch() {
        Map<String, Object> values = new HashMap<>();
        values.put("lang", null);
        Expression filter = new IsNotNull(SRC, fieldAttr("lang"));
        assertEquals(Boolean.FALSE, FileSplitProvider.evaluateFilter(filter, values));
    }

    public void testEvaluateFilterIsNotNullOnNonNullPartitionMatches() {
        Expression filter = new IsNotNull(SRC, fieldAttr("lang"));
        assertEquals(Boolean.TRUE, FileSplitProvider.evaluateFilter(filter, Map.of("lang", 3)));
    }

    public void testEvaluateFilterIsNullOnUnknownColumnIsNull() {
        Expression filter = new IsNull(SRC, fieldAttr("missing"));
        assertNull(FileSplitProvider.evaluateFilter(filter, Map.of("lang", 3)));
    }

    public void testEvaluateFilterInOnNullPartitionIsUnknown() {
        // lang IN (1, 2) where the partition value is null: under three-valued logic the comparison
        // is unknown, so the file must be kept (null result, not false).
        Map<String, Object> values = new HashMap<>();
        values.put("lang", null);
        Expression filter = new In(SRC, fieldAttr("lang"), List.of(intLiteral(1), intLiteral(2)));
        assertNull(FileSplitProvider.evaluateFilter(filter, values));
    }

    public void testEvaluateFilterOrPruneOnlyWhenAllBranchesFalse() {
        Expression left = new Equals(SRC, fieldAttr("lang"), intLiteral(1));
        Expression right = new Equals(SRC, fieldAttr("lang"), intLiteral(2));
        Expression or = new Or(SRC, left, right);
        assertEquals(Boolean.TRUE, FileSplitProvider.evaluateFilter(or, Map.of("lang", 1)));
        assertEquals(Boolean.FALSE, FileSplitProvider.evaluateFilter(or, Map.of("lang", 3)));
    }

    public void testEvaluateFilterOrWithIsNullPrunesCorrectly() {
        Map<String, Object> nullValues = new HashMap<>();
        nullValues.put("lang", null);
        Expression or = new Or(SRC, new Equals(SRC, fieldAttr("lang"), intLiteral(1)), new IsNull(SRC, fieldAttr("lang")));
        assertEquals(Boolean.TRUE, FileSplitProvider.evaluateFilter(or, nullValues));
        assertEquals(Boolean.TRUE, FileSplitProvider.evaluateFilter(or, Map.of("lang", 1)));
        assertEquals(Boolean.FALSE, FileSplitProvider.evaluateFilter(or, Map.of("lang", 2)));
    }

    public void testEvaluateFilterAndPrunesOnAnyFalseBranch() {
        Expression and = new And(
            SRC,
            new Equals(SRC, fieldAttr("lang"), intLiteral(1)),
            new Equals(SRC, fieldAttr("year"), intLiteral(2024))
        );
        assertEquals(Boolean.TRUE, FileSplitProvider.evaluateFilter(and, Map.of("lang", 1, "year", 2024)));
        assertEquals(Boolean.FALSE, FileSplitProvider.evaluateFilter(and, Map.of("lang", 1, "year", 2023)));
        assertEquals(Boolean.FALSE, FileSplitProvider.evaluateFilter(and, Map.of("lang", 2, "year", 2024)));
    }

    public void testEvaluateFilterAndWithUnknownBranchReturnsNullUnlessOtherIsFalse() {
        Expression and = new And(
            SRC,
            new Equals(SRC, fieldAttr("lang"), intLiteral(1)),
            new Equals(SRC, fieldAttr("unknown"), intLiteral(0))
        );
        assertNull(FileSplitProvider.evaluateFilter(and, Map.of("lang", 1)));
        assertEquals(Boolean.FALSE, FileSplitProvider.evaluateFilter(and, Map.of("lang", 2)));
    }

    public void testEvaluateFilterNotInvertsKnownValuesAndPropagatesNull() {
        Expression eq = new Equals(SRC, fieldAttr("lang"), intLiteral(1));
        Expression not = new Not(SRC, eq);
        assertEquals(Boolean.FALSE, FileSplitProvider.evaluateFilter(not, Map.of("lang", 1)));
        assertEquals(Boolean.TRUE, FileSplitProvider.evaluateFilter(not, Map.of("lang", 2)));
        assertNull(FileSplitProvider.evaluateFilter(not, Map.of("year", 2024)));
    }

    // -- sub-file splitting --

    public void testLargeNdjsonFileIsNotByteSplitEvenWithSmallProviderTarget() {
        long targetSize = 1000;
        FileSplitProvider splitter = new FileSplitProvider(targetSize);

        StorageEntry entry = new StorageEntry(StoragePath.of("s3://b/big.ndjson"), 3500, Instant.EPOCH);
        FileList fileList = GlobExpander.fileListOf(List.of(entry), "s3://b/*.ndjson");

        SplitDiscoveryContext ctx = new SplitDiscoveryContext(null, fileList, Map.of(), PartitionMetadata.EMPTY, List.of());
        List<ExternalSplit> splits = splitter.discoverSplits(ctx).splits();

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
        List<ExternalSplit> splits = splitter.discoverSplits(ctx).splits();

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
        List<ExternalSplit> splits = splitter.discoverSplits(ctx).splits();

        assertEquals(1, splits.size());
        assertEquals(0, ((FileSplit) splits.get(0)).offset());
        assertEquals(5000, ((FileSplit) splits.get(0)).length());
    }

    public void testDefaultProviderDoesNotSplitSmallFile() {
        StorageEntry entry = new StorageEntry(StoragePath.of("s3://b/big.csv"), 10_000_000, Instant.EPOCH);
        FileList fileList = GlobExpander.fileListOf(List.of(entry), "s3://b/*.csv");

        SplitDiscoveryContext ctx = new SplitDiscoveryContext(null, fileList, Map.of(), PartitionMetadata.EMPTY, List.of());
        List<ExternalSplit> splits = provider.discoverSplits(ctx).splits();

        assertEquals(1, splits.size());
        assertEquals(0, ((FileSplit) splits.get(0)).offset());
    }

    public void testDefaultProviderDoesNotByteRangeSplitLargeNdjson() {
        long fileSize = 500 * 1024 * 1024L; // 500 MB — above default split threshold
        StorageEntry entry = new StorageEntry(StoragePath.of("s3://b/huge.ndjson"), fileSize, Instant.EPOCH);
        FileList fileList = GlobExpander.fileListOf(List.of(entry), "s3://b/*.ndjson");

        SplitDiscoveryContext ctx = new SplitDiscoveryContext(null, fileList, Map.of(), PartitionMetadata.EMPTY, List.of());
        List<ExternalSplit> splits = provider.discoverSplits(ctx).splits();

        assertEquals(
            "Without format registry and storage, NDJSON cannot discover newline boundaries and stays one split per file",
            1,
            splits.size()
        );
        FileSplit whole = (FileSplit) splits.get(0);
        assertEquals(0, whole.offset());
        assertEquals(fileSize, whole.length());
        // A whole-file split states its position explicitly: it is both the first and the last split of
        // the file. Readers run the split-boundary protocol off these flags — a non-last split drops its
        // trailing partial record because the next split re-reads those bytes — so leaving them unstamped
        // made a whole-file read discard a final record that no other split would ever read.
        assertEquals("true", whole.config().get(FileSplitProvider.FIRST_SPLIT_KEY));
        assertEquals("true", whole.config().get(FileSplitProvider.LAST_SPLIT_KEY));
    }

    public void testNewlineMacroSplitCandidateExtensionsIncludeCsvAndTsv() {
        assertTrue(FileSplitProvider.isNewlineMacroSplitCandidateExtension(".csv"));
        assertTrue(FileSplitProvider.isNewlineMacroSplitCandidateExtension(".tsv"));
        assertTrue(FileSplitProvider.isNewlineMacroSplitCandidateExtension(".ndjson"));
        assertFalse(FileSplitProvider.isNewlineMacroSplitCandidateExtension(".parquet"));
    }

    public void testNewlineAlignedNdjsonMacroSplitsAreDisjointAndMarked() throws IOException {
        assertNewlineAlignedMacroSplitsDisjointAndMarked(".ndjson", "ndjson-macro-test", "abcdefgh\n", "s3://b/*.ndjson");
    }

    public void testNewlineAlignedCsvMacroSplitsAreDisjointAndMarked() throws IOException {
        assertNewlineAlignedMacroSplitsDisjointAndMarked(".csv", "csv-macro-test", "a,b,c\n", "s3://b/*.csv");
    }

    private void assertNewlineAlignedMacroSplitsDisjointAndMarked(
        String extension,
        String registryName,
        String lineContent,
        String globPattern
    ) throws IOException {
        SegmentableFormatReader mockReader = mock(SegmentableFormatReader.class);
        when(mockReader.rowPositionStrategy()).thenReturn(PassThroughRowPositionStrategy.INSTANCE);
        when(mockReader.minimumSegmentSize()).thenReturn(1024L);
        RecordSplitter mockSplitter = mock(RecordSplitter.class);
        when(mockReader.recordSplitter(anyInt())).thenReturn(mockSplitter);
        // Newline splitting is strided-safe; the mock must say so or the macro-split guard rejects it.
        when(mockSplitter.supportsStridedProbing()).thenReturn(true);
        when(mockSplitter.findNextRecordBoundary(any())).thenAnswer(invocation -> {
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

        // The payload runs several strides long, plus slack past the last offset so the final split is not a runt.
        long stride = 256 * 1024;
        StringBuilder sb = new StringBuilder();
        while (sb.length() < 4 * stride + stride / 2) {
            sb.append(lineContent);
        }
        byte[] payload = sb.toString().getBytes(StandardCharsets.UTF_8);
        long fileLength = payload.length;

        FormatReaderRegistry formatRegistry = new FormatReaderRegistry(new DecompressionCodecRegistry());
        formatRegistry.registerLazy(registryName, (s, bf) -> mockReader, Settings.EMPTY, null);
        formatRegistry.registerExtension(extension, registryName);
        formatRegistry.byName(registryName);

        StorageProviderRegistry storageRegistry = createPayloadStorageRegistry(payload);

        FileSplitProvider splitter = new FileSplitProvider(
            stride,
            new DecompressionCodecRegistry(),
            storageRegistry,
            formatRegistry,
            Settings.EMPTY
        );

        String fileName = "lines" + extension;
        StorageEntry entry = new StorageEntry(StoragePath.of("s3://b/" + fileName), fileLength, Instant.EPOCH);
        FileList fileList = GlobExpander.fileListOf(List.of(entry), globPattern);

        SplitDiscoveryContext ctx = new SplitDiscoveryContext(null, fileList, Map.of(), PartitionMetadata.EMPTY, List.of());
        List<ExternalSplit> splits = splitter.discoverSplits(ctx).splits();

        assertTrue("Expected multiple newline-aligned macro splits for " + extension, splits.size() > 1);
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
        verify(mockSplitter, atLeastOnce()).findNextRecordBoundary(any());
    }

    // CSV's minimum segment size is a fixed 1 MiB, so files must clear ~2 MiB before macro-splitting engages.
    // Both tests use a payload above that floor so a single split proves the quoting gate, not mere smallness.
    private static final long CSV_MIN_SEGMENT_BYTES = 1024 * 1024L;

    /**
     * A probe window twice the drain threshold, which is the width at which the drain-versus-abort rule is
     * observable in both directions: a probe that scans none of it aborts, one that scans half of it drains.
     */
    private static final long FULL_WIDTH_WINDOW_BYTES = 2 * RecordBoundaryProbe.MAX_DRAIN_BYTES;

    public void testQuotedCsvMacroSplits() {
        // Default CSV is mode=quoted: a quoted field may embed newlines, so it cannot be probed at arbitrary
        // byte offsets. It is still macro-split for cross-node parallelism via the proven-probe path, which
        // proves a record start at each emitted boundary rather than assuming every newline terminates a record.
        List<ExternalSplit> splits = discoverRealDelimitedSplits(Map.of(), "quoted.csv", ".csv", CsvFormatOptions.DEFAULT, "a,b,c\n");

        assertTrue("quoted CSV should macro-split", splits.size() > 1);
        for (ExternalSplit s : splits) {
            FileSplit fileSplit = (FileSplit) s;
            if (fileSplit.offset() == 0) {
                continue;
            }
            assertEquals("true", fileSplit.config().get(FileSplitProvider.RECORD_ALIGNED_MACRO_SPLIT_KEY));
        }
    }

    public void testPlainCsvStillMacroSplits() {
        // mode=plain turns quoting off, so every newline is an unambiguous record boundary and the file is
        // still macro-split for cross-node parallelism, exactly as before the quoting gate was added.
        List<ExternalSplit> splits = discoverRealDelimitedSplits(
            Map.of("mode", "plain"),
            "plain.csv",
            ".csv",
            CsvFormatOptions.DEFAULT,
            "a,b,c\n"
        );

        assertTrue("plain CSV should still be macro-split", splits.size() > 1);
        for (ExternalSplit s : splits) {
            assertEquals("true", ((FileSplit) s).config().get(FileSplitProvider.RECORD_ALIGNED_MACRO_SPLIT_KEY));
        }
    }

    public void testPlainTsvBaselineStillMacroSplits() {
        // TSV's baseline is mode=plain, so a default .tsv keeps quoting off and is still macro-split.
        List<ExternalSplit> splits = discoverRealDelimitedSplits(Map.of(), "plain.tsv", ".tsv", CsvFormatOptions.TSV, "a\tb\tc\n");

        assertTrue("plain TSV should still be macro-split", splits.size() > 1);
        for (ExternalSplit s : splits) {
            assertEquals("true", ((FileSplit) s).config().get(FileSplitProvider.RECORD_ALIGNED_MACRO_SPLIT_KEY));
        }
    }

    public void testEscapedModeMacroSplits() {
        // mode=escaped keeps quoting off but escaping on: a backslash-escaped raw newline is in-field content,
        // so the file cannot be probed at arbitrary offsets. It is still macro-split via the proven-probe path,
        // which proves a record start at each emitted boundary rather than assuming every newline terminates a
        // record.
        List<ExternalSplit> splits = discoverRealDelimitedSplits(
            Map.of("mode", "escaped"),
            "escaped.csv",
            ".csv",
            CsvFormatOptions.DEFAULT,
            "a,b,c\n"
        );

        assertTrue("escaped CSV should macro-split", splits.size() > 1);
        for (ExternalSplit s : splits) {
            FileSplit fileSplit = (FileSplit) s;
            if (fileSplit.offset() == 0) {
                continue;
            }
            assertEquals("true", fileSplit.config().get(FileSplitProvider.RECORD_ALIGNED_MACRO_SPLIT_KEY));
        }
    }

    public void testQuotedModeOverrideOnTsvMacroSplits() {
        // The proven-probe path keys off the config-resolved reader, not the extension: mode=quoted turns
        // quoting on for a .tsv whose baseline is plain, and the file still macro-splits through proven probing.
        List<ExternalSplit> splits = discoverRealDelimitedSplits(
            Map.of("mode", "quoted"),
            "quoted-mode.tsv",
            ".tsv",
            CsvFormatOptions.TSV,
            "a\tb\tc\n"
        );

        assertTrue("quoted-mode TSV should macro-split", splits.size() > 1);
        for (ExternalSplit s : splits) {
            FileSplit fileSplit = (FileSplit) s;
            if (fileSplit.offset() == 0) {
                continue;
            }
            assertEquals("true", fileSplit.config().get(FileSplitProvider.RECORD_ALIGNED_MACRO_SPLIT_KEY));
        }
    }

    private List<ExternalSplit> discoverRealDelimitedSplits(
        Map<String, Object> config,
        String fileName,
        String extension,
        CsvFormatOptions baselineOptions,
        String lineContent
    ) {
        return discoverRealDelimitedSplits(config, fileName, extension, baselineOptions, lineContent, CSV_MIN_SEGMENT_BYTES);
    }

    private List<ExternalSplit> discoverRealDelimitedSplits(
        Map<String, Object> config,
        String fileName,
        String extension,
        CsvFormatOptions baselineOptions,
        String lineContent,
        long targetStrideBytes
    ) {
        StringBuilder sb = new StringBuilder();
        // ~3.5 MiB: above 2 x CSV_MIN_SEGMENT_BYTES so plain data yields several macro-splits.
        while (sb.length() < 3 * CSV_MIN_SEGMENT_BYTES + CSV_MIN_SEGMENT_BYTES / 2) {
            sb.append(lineContent);
        }
        byte[] payload = sb.toString().getBytes(StandardCharsets.UTF_8);

        String formatName = extension.substring(1);
        FormatReaderRegistry formatRegistry = new FormatReaderRegistry(new DecompressionCodecRegistry());
        formatRegistry.registerLazy(
            formatName,
            (s, bf) -> new CsvFormatReader(bf, baselineOptions, formatName, List.of(extension)),
            Settings.EMPTY,
            null
        );
        formatRegistry.registerExtension(extension, formatName);
        formatRegistry.byName(formatName);

        StorageProviderRegistry storageRegistry = createPayloadStorageRegistry(payload);
        FileSplitProvider splitter = new FileSplitProvider(
            targetStrideBytes,
            new DecompressionCodecRegistry(),
            storageRegistry,
            formatRegistry,
            Settings.EMPTY
        );

        StorageEntry entry = new StorageEntry(StoragePath.of("s3://b/" + fileName), payload.length, Instant.EPOCH);
        FileList fileList = GlobExpander.fileListOf(List.of(entry), "s3://b/*" + extension);
        SplitDiscoveryContext ctx = new SplitDiscoveryContext(
            null,
            fileList,
            Map.of(),
            config,
            PartitionMetadata.EMPTY,
            List.of(),
            ExternalSchema.EMPTY,
            null,
            SegmentableFormatReader.DEFAULT_MAX_RECORD_BYTES,
            () -> false,
            DeclaredReadSpec.NONE
        );
        return splitter.discoverSplits(ctx).splits();
    }

    /**
     * Concurrency budgets are per node, not per file: every deferred file's boundary probes share one budget, so
     * the ceiling never multiplies by the number of files being probed. A configured concurrency of {@code 0}
     * turns permit limiting off rather than meaning "no concurrency", so it must not collapse the budget.
     */
    public void testProbeConcurrencyIsClampedToBlobStoreConcurrency() {
        assertEquals(4, probeConcurrencyFor(Settings.builder().put("esql.external.max_concurrent_requests", 4).build()));
        int ceiling = FileSplitProvider.MAX_PARALLEL_SPLIT_DISCOVERY;
        assertEquals(ceiling, probeConcurrencyFor(Settings.builder().put("esql.external.max_concurrent_requests", ceiling).build()));
        assertEquals(
            FileSplitProvider.MAX_PARALLEL_SPLIT_DISCOVERY,
            probeConcurrencyFor(Settings.builder().put("esql.external.max_concurrent_requests", 200).build())
        );
        assertEquals(
            "permit limiting disabled must not disable concurrency",
            FileSplitProvider.MAX_PARALLEL_SPLIT_DISCOVERY,
            probeConcurrencyFor(Settings.builder().put("esql.external.max_concurrent_requests", 0).build())
        );
    }

    private static int probeConcurrencyFor(Settings settings) {
        return new FileSplitProvider(1024, new DecompressionCodecRegistry(), null, null, settings).splitDiscoveryConcurrency();
    }

    /**
     * With a discovery executor the boundary probes of a multi-file query run concurrently, and they produce
     * exactly the splits the serial path produces. The two must agree: probing fixed stride offsets makes each
     * probe independent of the others, so the order they complete in cannot change the boundary set.
     */
    public void testStridedProbesRunConcurrentlyAndAgreeWithSerialDiscovery() throws Exception {
        Map<String, byte[]> payloads = Map.of("one.csv", delimitedPayload("a,b,c\n"), "two.csv", delimitedPayload("d,e,f\n"));
        // Two probes per file (stride and minimum segment are both 1 MiB against a ~3.5 MiB payload).
        StreamTracking tracking = new StreamTracking(2);

        List<ExternalSplit> serial = discoverPlainCsvSplits(payloads, CSV_MIN_SEGMENT_BYTES, null, null);
        ExecutorService executor = Executors.newFixedThreadPool(4);
        List<ExternalSplit> parallel;
        try {
            parallel = discoverPlainCsvSplits(payloads, CSV_MIN_SEGMENT_BYTES, executor, tracking);
        } finally {
            executor.shutdown();
        }

        assertThat("both files must macro-split", serial.size(), greaterThan(2));
        assertEquals("parallel probing must not change the split set", describe(serial), describe(parallel));
        assertThat("probes must overlap when an executor is available", tracking.peakInFlight.get(), greaterThan(1));
    }

    /**
     * The motivating case is one very large file, not many files, so a single file's own boundary probes must run
     * concurrently. This also checks the boundaries themselves against ground truth on a payload with varying line
     * lengths, where an off-byte in the probe arithmetic would land a split mid-record: every emitted split must
     * begin at a real record start, and the splits together must cover the file exactly once.
     */
    public void testSingleLargeFileProbesConcurrentlyOnTrueRecordStarts() throws Exception {
        StringBuilder csv = new StringBuilder();
        int row = 0;
        while (csv.length() < 4 * CSV_MIN_SEGMENT_BYTES) {
            // Line lengths cycle so stride offsets land at varying depths into a record.
            csv.append(row).append(",value,").append("x".repeat(row % 37)).append('\n');
            row++;
        }
        byte[] payload = csv.toString().getBytes(StandardCharsets.UTF_8);
        Set<Long> trueStarts = trueRecordStarts(stridedSplitter(), payload);
        StreamTracking tracking = new StreamTracking(2);

        ExecutorService executor = Executors.newFixedThreadPool(4);
        List<ExternalSplit> splits;
        try {
            splits = discoverPlainCsvSplits(Map.of("big.csv", payload), 256 * 1024, executor, tracking);
        } finally {
            executor.shutdown();
        }

        assertThat("one large file must still macro-split several ways", splits.size(), greaterThan(2));
        assertThat("a single file's probes must overlap", tracking.peakInFlight.get(), greaterThan(1));
        long expectedOffset = 0;
        for (ExternalSplit split : splits) {
            FileSplit fileSplit = (FileSplit) split;
            assertEquals("splits must be contiguous", expectedOffset, fileSplit.offset());
            assertTrue("split at " + fileSplit.offset() + " must begin on a record start", trueStarts.contains(fileSplit.offset()));
            assertThat("no empty split", fileSplit.length(), greaterThan(0L));
            expectedOffset += fileSplit.length();
        }
        assertEquals("splits must cover the whole file", payload.length, expectedOffset);
    }

    /**
     * A file with no record terminator until its very last byte offers no boundary at any offset, so it is read
     * whole. The price of an offset that finds nothing is one read: it says nothing about the offsets after it,
     * so none of their reads may be skipped on the strength of it.
     */
    public void testAFileWithNoBoundaryInAnyProbeWindowIsReadWhole() throws Exception {
        long stride = 2 * CSV_MIN_SEGMENT_BYTES;
        byte[] payload = oneRecordSpanning(8 * stride);
        StreamTracking tracking = new StreamTracking(1);

        ExecutorService executor = Executors.newFixedThreadPool(4);
        List<ExternalSplit> splits;
        try {
            splits = discoverPlainCsvSplits(Map.of("long-lines.csv", payload), stride, executor, tracking);
        } finally {
            executor.shutdown();
        }
        assertWarnings(true, List.of(containsString("1 file(s) were cut into fewer splits")));

        assertEquals("a file with no usable boundary is read whole", 1, splits.size());
        int probes = RecordBoundaryProbe.stridedPositions(payload.length, stride, CSV_MIN_SEGMENT_BYTES).size();
        assertThat("the payload must offer several offsets to probe", probes, greaterThan(1));
        assertEquals("an offset that finds nothing must not suppress the others", probes, tracking.opens.get());
    }

    /**
     * What leaves a file with no usable boundary is a property of the dataset, not of the file: records wider than
     * the probe window make every file of a scan unsplittable at once. The query is told once, with the count and
     * an example, rather than once per file, which for a scan of many files would bury its response in warnings.
     */
    public void testUnsplittableFilesAreWarnedAboutOncePerQuery() {
        long stride = 2 * CSV_MIN_SEGMENT_BYTES;
        byte[] payload = oneRecordSpanning(8 * stride);
        Map<String, byte[]> payloads = new HashMap<>();
        for (int i = 0; i < 3; i++) {
            payloads.put("long-lines-" + i + ".csv", payload);
        }

        List<ExternalSplit> splits = discoverPlainCsvSplits(payloads, stride, null, null);

        assertEquals("each file with no usable boundary is read whole", payloads.size(), splits.size());
        // assertWarnings fails on any warning left without a matcher, so one matcher is also the assertion that
        // three unsplittable files raised one warning rather than three.
        assertWarnings(true, List.of(containsString("3 file(s) were cut into fewer splits")));
    }

    /**
     * A partial shortfall, which is the case the count in the warning exists for: most offsets resolve and some do
     * not, so the file is cut into fewer pieces than asked for while still being cut. Nothing about the splits
     * says so, which is why it is reported rather than left for the reader of a slow query to infer.
     */
    public void testAPartialShortfallIsWarnedAboutEvenThoughTheFileStillSplits() {
        long stride = 2 * CSV_MIN_SEGMENT_BYTES;
        // One record longer than a probe window, so the offset inside it finds nothing while the offsets over the
        // short rows either side of it resolve normally.
        StringBuilder csv = new StringBuilder();
        while (csv.length() < 2 * stride) {
            csv.append("a,b,c\n");
        }
        csv.append("z".repeat(Math.toIntExact(2 * stride))).append('\n');
        while (csv.length() < 8 * stride) {
            csv.append("a,b,c\n");
        }
        byte[] payload = csv.toString().getBytes(StandardCharsets.UTF_8);

        List<ExternalSplit> splits = discoverPlainCsvSplits(Map.of("one-long-row.csv", payload), stride, null, null);

        assertThat("the file must still be cut at the offsets that did resolve", splits.size(), greaterThan(1));
        assertWarnings(
            true,
            List.of(
                allOf(
                    containsString("1 file(s) were cut into fewer splits"),
                    containsString("0 of them are read as a single whole-file split"),
                    containsString("probe offsets found no record boundary")
                )
            )
        );
    }

    /**
     * A quoted CSV whose sequential walk gives up with file left to cut. The walk cannot skip the record it
     * cannot get past, so everything after it goes uncut, and the splits themselves say no more about that than
     * they do about a strided file that lost offsets. A walked file probes no offsets, so the warning it raises
     * leaves the offset tally out rather than reporting none of none.
     */
    public void testASequentialWalkThatGivesUpIsWarnedAbout() {
        long stride = 2 * CSV_MIN_SEGMENT_BYTES;
        // Quoted rows, an embedded newline in each so the file can only be walked, for the first stride. Then a
        // run with no terminator at all: past the offset that lands in it there is no record start left to prove.
        StringBuilder csv = new StringBuilder();
        while (csv.length() < stride) {
            csv.append("a,\"b\nb\",c\n");
        }
        csv.append("z".repeat(Math.toIntExact(3 * stride)));
        byte[] payload = csv.toString().getBytes(StandardCharsets.UTF_8);

        List<ExternalSplit> splits = discoverCsvSplits(
            Map.of("unterminated-tail.csv", payload),
            stride,
            null,
            null,
            Settings.EMPTY,
            () -> false,
            Map.of()
        );

        assertThat("the file must still be cut where the walk did reach", splits.size(), greaterThan(1));
        assertWarnings(
            true,
            List.of(
                allOf(
                    containsString("1 file(s) were cut into fewer splits"),
                    containsString("0 of them are read as a single whole-file split"),
                    not(containsString("probe offsets"))
                )
            )
        );
    }

    /** A payload of {@code length} bytes that is one record from end to end: no terminator until the final byte. */
    private static byte[] oneRecordSpanning(long length) {
        byte[] payload = new byte[Math.toIntExact(length)];
        Arrays.fill(payload, (byte) 'y');
        payload[payload.length - 1] = '\n';
        return payload;
    }

    /**
     * A file just over one stride whose only probe finds a newline too close to EOF is read whole, which is the
     * intended cut, not a missing boundary. The query is not told to raise {@code target_split_size}: that would
     * point the wrong way.
     */
    public void testAFileWhoseOnlyFoundBoundaryLeavesAShortTailIsReadWholeWithoutWarning() {
        long stride = CSV_MIN_SEGMENT_BYTES;
        byte[] row = "a,b,c\n".getBytes(StandardCharsets.UTF_8);
        byte[] payload = new byte[Math.toIntExact(stride + CSV_MIN_SEGMENT_BYTES)];
        for (int i = 0; i < payload.length; i++) {
            payload[i] = row[i % row.length];
        }
        assertEquals(
            "the file must offer exactly one probe, at the stride",
            1,
            RecordBoundaryProbe.stridedPositions(payload.length, stride, CSV_MIN_SEGMENT_BYTES).size()
        );

        // No assertWarnings call: the test framework fails on any warning left unasserted, so the absence of one
        // here is what pins that a short leftover after a found boundary is not reported as no record boundary.
        List<ExternalSplit> splits = discoverPlainCsvSplits(Map.of("just-over-one-stride.csv", payload), stride, null, null);

        assertEquals("a file barely over one stride is read whole", 1, splits.size());
    }

    /**
     * A small target split size splits at its own offsets. The probe window is capped at the stride, which is
     * what keeps one probe's window from reaching into the next offset, so a caller asking for small splits gets
     * them with correspondingly small probes rather than being rounded up to some fixed read size.
     */
    public void testASmallTargetStrideIsHonoured() {
        long stride = 64 * 1024;
        long widerStride = 256 * 1024;
        Map<String, byte[]> payloads = Map.of("tiny-stride.csv", delimitedPayload("a,b,c\n"));
        byte[] payload = payloads.get("tiny-stride.csv");

        List<ExternalSplit> splits = discoverPlainCsvSplits(payloads, stride, null, null);
        List<ExternalSplit> atWiderStride = discoverPlainCsvSplits(payloads, widerStride, null, null);

        // Every offset resolves, so the split count is the offset count plus the file start.
        int probes = RecordBoundaryProbe.stridedPositions(payload.length, stride, CSV_MIN_SEGMENT_BYTES).size();
        assertEquals("a small stride must split at its own offsets", probes + 1, splits.size());
        assertThat("and into more splits than a wider stride would give", splits.size(), greaterThan(atWiderStride.size()));
        for (int i = 0; i < splits.size() - 1; i++) {
            FileSplit split = (FileSplit) splits.get(i);
            // A boundary sits at most one record past its offset, so every split but the last is one stride long
            // give or take a record.
            assertThat("split " + i + " must be about one stride long", split.length(), lessThan(stride + 64));
        }
    }

    /**
     * A stride small enough to ask for more record-boundary probes than the budget is widened to the stride
     * that spends exactly the budget, and the file is cut at that. Every offset of a strided file is
     * materialized before any read and each becomes a probe task, a queued listener and a blocking ranged read
     * after that, so an extreme target split size would otherwise cost planning latency and planning-time heap
     * for splits too small to pay for either. The widening is what the user did not ask for, so it must be
     * warned about.
     */
    public void testATargetStrideAskingForTooManyProbesIsWidened() {
        byte[] payload = delimitedPayload("a,b,c\n");
        Map<String, byte[]> payloads = Map.of("swarm.csv", payload);
        long stride = payload.length / (4 * FileSplitProvider.MAX_SPLIT_PROBES_PER_QUERY);
        assertThat("the stride under test must be usable at all", stride, greaterThan(0L));
        assertThat(
            "the stride under test must ask for more probes than the budget",
            RecordBoundaryProbe.stridedPositions(payload.length, stride, CSV_MIN_SEGMENT_BYTES).size(),
            greaterThan(FileSplitProvider.MAX_SPLIT_PROBES_PER_QUERY)
        );

        List<ExternalSplit> splits = discoverPlainCsvSplits(payloads, stride, null, null);
        assertWarnings(true, List.of(containsString("would probe more than 1000 record boundaries")));

        long widened = Math.ceilDiv(payload.length, FileSplitProvider.MAX_SPLIT_PROBES_PER_QUERY);
        int probes = RecordBoundaryProbe.stridedPositions(payload.length, widened, CSV_MIN_SEGMENT_BYTES).size();
        assertEquals("the file must be cut at the widened stride, not the requested one", probes + 1, splits.size());
        assertThat(probes, lessThanOrEqualTo(FileSplitProvider.MAX_SPLIT_PROBES_PER_QUERY));
    }

    /**
     * The budget is spent by the query, not by each file on its own: a stride that asks for a modest number of
     * probes per file still asks for more than the budget once enough files want it, and every file's probes go
     * into one batch. A ceiling that each file passed separately would let the count be multiplied by the file
     * count, which is the same unbounded batch it was meant to prevent.
     */
    public void testAStrideAskingForTooManyProbesAcrossFilesIsWidened() {
        byte[] payload = delimitedPayload("a,b,c\n");
        Map<String, byte[]> payloads = new HashMap<>();
        for (int i = 0; i < 4; i++) {
            payloads.put("swarm-" + i + ".csv", payload);
        }
        long stride = payload.length / (FileSplitProvider.MAX_SPLIT_PROBES_PER_QUERY / 2);
        int probesPerFile = RecordBoundaryProbe.stridedPositions(payload.length, stride, CSV_MIN_SEGMENT_BYTES).size();
        assertThat(
            "no single file may reach the budget, or this would pass on a per-file ceiling too",
            probesPerFile,
            lessThan(FileSplitProvider.MAX_SPLIT_PROBES_PER_QUERY)
        );
        assertThat(
            "but the files together must ask for more than it",
            probesPerFile * payloads.size(),
            greaterThan(FileSplitProvider.MAX_SPLIT_PROBES_PER_QUERY)
        );

        // Serial discovery, so the overlap latch of one is satisfied by the first stream and never delays a probe.
        StreamTracking tracking = new StreamTracking(1);
        List<ExternalSplit> splits = discoverPlainCsvSplits(payloads, stride, null, tracking);
        assertWarnings(true, List.of(containsString("would probe more than 1000 record boundaries")));

        long widened = Math.ceilDiv((long) payload.length * payloads.size(), FileSplitProvider.MAX_SPLIT_PROBES_PER_QUERY);
        int probesPerWidenedFile = RecordBoundaryProbe.stridedPositions(payload.length, widened, CSV_MIN_SEGMENT_BYTES).size();
        assertEquals(
            "every file must be cut at the stride the whole query was widened to",
            payloads.size() * (probesPerWidenedFile + 1),
            splits.size()
        );
        // A plain CSV file needs no planning read, so every stream this discovery opened was a probe.
        assertThat(
            "and the probe reads they actually issued must be within the budget",
            tracking.opens.get(),
            lessThanOrEqualTo(FileSplitProvider.MAX_SPLIT_PROBES_PER_QUERY)
        );
        assertEquals("one probe per split past each file's first", splits.size() - payloads.size(), tracking.opens.get());
    }

    /**
     * A query whose probes fit the budget is cut at exactly the size asked for, and says nothing. The widening is
     * a last resort for a scan large enough to plan its way into trouble, so an ordinary one must not pay for it,
     * nor be told about a limit it never came near.
     */
    public void testAQueryWithinTheProbeBudgetIsNotWidened() {
        byte[] payload = delimitedPayload("a,b,c\n");
        Map<String, byte[]> payloads = Map.of("modest.csv", payload);
        long stride = 256 * 1024;
        assertThat(
            "the stride under test must ask for fewer probes than the budget",
            RecordBoundaryProbe.stridedPositions(payload.length, stride, CSV_MIN_SEGMENT_BYTES).size(),
            lessThan(FileSplitProvider.MAX_SPLIT_PROBES_PER_QUERY)
        );

        // No assertWarnings call: the test framework fails on any warning left unasserted, so the absence of one
        // here is what pins that the requested stride was not overridden.
        List<ExternalSplit> splits = discoverPlainCsvSplits(payloads, stride, null, null);

        int probes = RecordBoundaryProbe.stridedPositions(payload.length, stride, CSV_MIN_SEGMENT_BYTES).size();
        assertEquals("the file must be cut at the size asked for", probes + 1, splits.size());
    }

    /**
     * The budget is shared between the files that will be probed, and a file of an extension that cannot be
     * newline macro-split is not one of them however large it is. Counting its bytes would widen the stride of
     * the files that are probed, cutting them coarser to pay for probes nobody issues.
     */
    public void testAnUnsplittableExtensionDoesNotDrawOnTheProbeBudget() {
        byte[] csv = delimitedPayload("a,b,c\n");
        // Large enough that its bytes alone would widen the stride, were they counted.
        byte[] opaque = new byte[csv.length * 4];
        long stride = csv.length / (FileSplitProvider.MAX_SPLIT_PROBES_PER_QUERY / 2);

        Map<String, byte[]> alone = Map.of("data.csv", csv);
        Map<String, byte[]> withOpaqueFile = new HashMap<>(alone);
        withOpaqueFile.put("blob.dat", opaque);

        List<ExternalSplit> aloneSplits = discoverPlainCsvSplits(alone, stride, null, null);
        List<ExternalSplit> withOpaqueSplits = discoverPlainCsvSplits(withOpaqueFile, stride, null, null);

        assertEquals("the unsplittable file must be read whole", aloneSplits.size() + 1, withOpaqueSplits.size());
        assertEquals(
            "and the CSV file must be cut exactly as it was without it",
            describe(aloneSplits),
            describe(withOpaqueSplits).stream().filter(s -> s.contains("data.csv")).toList()
        );
    }

    /**
     * The proven walk a quoted CSV needs is bounded by the same budget as the strided one. It resumes a stride
     * past each boundary it finds, so the stride caps its boundary count the same way, and it costs more per
     * boundary than a strided probe does: its reads are sequential, each one waiting on the one before it.
     */
    public void testTheProvenWalkIsAlsoBoundedByTheProbeBudget() {
        String quotedLine = "1,\"embedded\nnewline\",\"has \"\"quote\"\"\"\n";
        long stride = 1024;

        List<ExternalSplit> splits = discoverRealDelimitedSplits(
            Map.of(),
            "quoted.csv",
            ".csv",
            CsvFormatOptions.DEFAULT,
            quotedLine,
            stride
        );
        assertWarnings(true, List.of(containsString("would probe more than 1000 record boundaries")));

        assertThat("the file must still be macro-split", splits.size(), greaterThan(1));
        // The file start begins a split without being probed for, so the walk spends one probe per split past the
        // first. Comparing the split count itself against the budget would allow one probe over it.
        assertThat(
            "but into no more boundaries than the budget",
            splits.size() - 1,
            lessThanOrEqualTo(FileSplitProvider.MAX_SPLIT_PROBES_PER_QUERY)
        );
    }

    /**
     * A file at or below the stride is cut into nothing and probed not at all, so it neither draws on the probe
     * budget nor is starved by it. The whole-file splits such files yield are bounded by the discovered-file
     * limit instead, which is what keeps a scan of many small files from having to widen anything. A file whose
     * length the listing did not carry reads as zero and takes the same path.
     */
    public void testSmallFilesNeitherProbeNorDrawOnTheBudget() {
        byte[] big = delimitedPayload("a,b,c\n");
        byte[] small = "a,b,c\n".getBytes(StandardCharsets.UTF_8);
        long stride = big.length / (FileSplitProvider.MAX_SPLIT_PROBES_PER_QUERY / 2);

        Map<String, byte[]> alone = Map.of("big.csv", big);
        Map<String, byte[]> withSmallFiles = new HashMap<>(alone);
        for (int i = 0; i < 8; i++) {
            withSmallFiles.put("small-" + i + ".csv", small);
        }
        withSmallFiles.put("unsized.csv", new byte[0]);

        List<ExternalSplit> aloneSplits = discoverPlainCsvSplits(alone, stride, null, null);
        List<ExternalSplit> withSmallSplits = discoverPlainCsvSplits(withSmallFiles, stride, null, null);

        assertEquals(
            "the big file must be cut the same either way",
            aloneSplits.size() + withSmallFiles.size() - alone.size(),
            withSmallSplits.size()
        );
    }

    /**
     * A record too long for the probe window costs the one split its offset would have started, and no more: the
     * offsets past it are probed as usual and their boundaries stand, so the splits either side of the record
     * merge into one that spans it. Serial and concurrent discovery must agree, which they do because an offset's
     * outcome depends on nothing but its own read.
     */
    public void testARecordExceedingTheWindowMidFileCostsOnlyItsOwnSplit() throws Exception {
        // The window is the stride here, the default max record size being far above it.
        long stride = 512 * 1024;
        long longRowBytes = 3 * stride / 2;
        StringBuilder csv = new StringBuilder();
        // Normal rows up to half a stride short of the second stride offset, so the probe there falls inside the
        // long row that follows and finds no boundary within its window.
        while (csv.length() < 2 * stride - stride / 2) {
            csv.append("a,b,c\n");
        }
        long longRowStart = csv.length();
        csv.append("z".repeat(Math.toIntExact(longRowBytes))).append('\n');
        while (csv.length() < 8 * stride) {
            csv.append("a,b,c\n");
        }
        byte[] payload = csv.toString().getBytes(StandardCharsets.UTF_8);
        Map<String, byte[]> payloads = Map.of("mid-file.csv", payload);

        List<ExternalSplit> serial = discoverPlainCsvSplits(payloads, stride, null, null);
        ExecutorService executor = Executors.newFixedThreadPool(4);
        List<ExternalSplit> parallel;
        try {
            parallel = discoverPlainCsvSplits(payloads, stride, executor, null);
        } finally {
            executor.shutdown();
        }

        assertEquals("an offset's outcome must not depend on when it is probed", describe(serial), describe(parallel));
        // One offset lands inside the long row and yields nothing; the file start and the rest of the offsets each
        // start a split, so the long row costs exactly one of them.
        int probes = RecordBoundaryProbe.stridedPositions(payload.length, stride, CSV_MIN_SEGMENT_BYTES).size();
        assertEquals("only the offset inside the long record loses its split", probes, serial.size());
        FileSplit spanning = (FileSplit) serial.get(1);
        assertThat("one split must span the record no probe could split", spanning.offset(), lessThan(longRowStart));
        assertThat(spanning.offset() + spanning.length(), greaterThan(longRowStart + longRowBytes));
        assertWarnings(true, List.of(containsString("1 file(s) were cut into fewer splits")));
    }

    /**
     * Probe concurrency is a per-query budget, so more probes than the budget must not all be in flight at once.
     * The budget follows the node's blob-store concurrency, which is what actually limits open streams.
     */
    public void testProbesInFlightStayWithinTheConcurrencyBudget() throws Exception {
        Settings settings = Settings.builder().put("esql.external.max_concurrent_requests", 2).build();
        byte[] payload = delimitedPayload("a,b,c,d,e\n");
        // A 128 KiB stride over a ~3.5 MiB payload leaves far more probe offsets than the budget of 2.
        StreamTracking tracking = new StreamTracking(2);

        ExecutorService executor = Executors.newFixedThreadPool(8);
        try {
            discoverPlainCsvSplits(Map.of("wide.csv", payload), 128 * 1024, executor, tracking, settings);
        } finally {
            executor.shutdown();
        }

        assertThat("more probes than the budget must have run", tracking.opens.get(), greaterThan(2));
        assertEquals("in-flight probes must not exceed the budget", 2, tracking.peakInFlight.get());
    }

    /**
     * The budget covers the whole query, so probing several files together must not multiply it. Probing one file
     * at a time under its own budget would allow files times budget streams open at once, which is what pooling
     * every deferred file's offsets into one batch prevents.
     */
    public void testProbesOfDifferentFilesShareOneConcurrencyBudget() throws Exception {
        Settings budgetOfTwo = Settings.builder().put("esql.external.max_concurrent_requests", 2).build();
        Map<String, byte[]> payloads = new HashMap<>();
        for (int file = 0; file < 4; file++) {
            payloads.put("f" + file + ".csv", delimitedPayload("a,b,c," + file + "\n"));
        }
        // A 128 KiB stride over ~3.5 MiB gives each file far more probe offsets than the budget of 2.
        StreamTracking tracking = new StreamTracking(2);

        ExecutorService executor = Executors.newFixedThreadPool(8);
        try {
            discoverPlainCsvSplits(payloads, 128 * 1024, executor, tracking, budgetOfTwo);
        } finally {
            executor.shutdown();
        }

        assertThat("every file must contribute probes", tracking.opens.get(), greaterThan(payloads.size() * 2));
        assertEquals("one budget across all files, not one per file", 2, tracking.peakInFlight.get());
    }

    /**
     * Planning a strided file reads nothing, so a cancel arriving while it runs is not seen by any read. It must be
     * seen between the phases instead: a cancelled query must not even dispatch its probes, let alone read.
     */
    public void testCancellationBetweenPlanningAndProbingDispatchesNoProbe() throws Exception {
        byte[] payload = delimitedPayload("a,b,c\n");
        StreamTracking tracking = new StreamTracking(1);
        // Planning hands out the file's storage object without reading it, which is the window under test.
        BooleanSupplier cancelOncePlanned = () -> tracking.objects.get() > 0;

        ExecutorService pool = Executors.newFixedThreadPool(2);
        AtomicInteger dispatched = new AtomicInteger();
        Executor countingExecutor = command -> {
            dispatched.incrementAndGet();
            pool.execute(command);
        };
        try {
            expectThrows(
                TaskCancelledException.class,
                () -> discoverPlainCsvSplits(
                    Map.of("planned.csv", payload),
                    256 * 1024,
                    countingExecutor,
                    tracking,
                    Settings.EMPTY,
                    cancelOncePlanned
                )
            );
        } finally {
            pool.shutdown();
        }
        // The single planned file is planned on the calling thread, so anything dispatched would be a probe.
        assertEquals("a cancelled query must not dispatch probes", 0, dispatched.get());
        assertEquals("a cancel seen before probing must not read", 0, tracking.opens.get());
    }

    /**
     * A cancel landing after the last probe has read is seen by no probe: each one checks before it opens its
     * stream and again after its scan, both of which are behind it. Discovery must therefore check once more when
     * the probe phase joins, rather than going on to build a split set the caller is about to discard.
     */
    public void testCancellationAfterTheLastProbeStillFailsDiscovery() {
        byte[] payload = delimitedPayload("a,b,c\n");
        long stride = 256 * 1024;
        StreamTracking tracking = new StreamTracking(1);
        int probes = RecordBoundaryProbe.stridedPositions(payload.length, stride, CSV_MIN_SEGMENT_BYTES).size();
        assertThat("the payload must offer offsets to probe", probes, greaterThan(0));
        // A probe releases its stream after making its own last cancellation check, so arming the cancel on the
        // final release puts it in the one window no probe can observe.
        BooleanSupplier cancelAfterTheLastProbe = () -> tracking.closes.get() >= probes;

        expectThrows(
            TaskCancelledException.class,
            () -> discoverPlainCsvSplits(Map.of("late.csv", payload), stride, null, tracking, Settings.EMPTY, cancelAfterTheLastProbe)
        );
        assertEquals("every probe must have run before the cancel was seen", probes, tracking.opens.get());
    }

    /**
     * A probe read that fails must fail the query. Probes run on other threads and each one only ever adds a
     * boundary, so an I/O error that did not travel back out of the gather would leave the file quietly split at
     * the boundaries the surviving probes happened to find.
     */
    public void testAFailedProbeReadFailsDiscovery() throws Exception {
        byte[] payload = delimitedPayload("a,b,c\n");
        // The first stream any probe opens fails; planning reads nothing, so no other read can be the first.
        StreamTracking tracking = new StreamTracking(1, 1);

        ExecutorService executor = Executors.newFixedThreadPool(4);
        RuntimeException failure;
        try {
            failure = expectThrows(
                RuntimeException.class,
                () -> discoverPlainCsvSplits(Map.of("broken.csv", payload), 256 * 1024, executor, tracking)
            );
        } finally {
            executor.shutdown();
        }

        assertThat(failure.getCause(), instanceOf(IOException.class));
        assertEquals("connection reset", failure.getCause().getMessage());
    }

    /**
     * A cancel landing between two probes is seen by the next one before it opens a stream, so a query cancelled
     * part-way through a file's offsets fails rather than reading out the rest of them.
     */
    public void testCancellationBetweenProbesIsSeenBeforeTheNextRead() throws Exception {
        byte[] payload = delimitedPayload("a,b,c\n");
        StreamTracking tracking = new StreamTracking(1);
        // Cancelled once the first probe has finished with its stream, so the cancel lands between two probes
        // rather than inside one.
        BooleanSupplier cancelAfterFirstProbe = () -> tracking.closes.get() > 0;

        // A single thread makes the ordering deterministic: no second probe starts before the first one closes.
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            expectThrows(
                TaskCancelledException.class,
                () -> discoverPlainCsvSplits(
                    Map.of("cancelled.csv", payload),
                    256 * 1024,
                    executor,
                    tracking,
                    Settings.EMPTY,
                    cancelAfterFirstProbe
                )
            );
        } finally {
            executor.shutdown();
        }
        assertEquals("the probes after the cancel must not read", 1, tracking.opens.get());
    }

    /**
     * A cancel arriving once probing has started stops the query rather than reading out every remaining offset.
     */
    public void testCancellationDuringProbingAbortsDiscovery() throws Exception {
        byte[] payload = delimitedPayload("a,b,c\n");
        StreamTracking tracking = new StreamTracking(1);
        // Cancelled as soon as the first probe opens its stream, so the cancel lands inside the probe phase
        // rather than during planning.
        BooleanSupplier cancelOnFirstRead = () -> tracking.opens.get() > 0;

        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            expectThrows(
                TaskCancelledException.class,
                () -> discoverPlainCsvSplits(
                    Map.of("cancelled.csv", payload),
                    256 * 1024,
                    executor,
                    tracking,
                    Settings.EMPTY,
                    cancelOnFirstRead
                )
            );
        } finally {
            executor.shutdown();
        }
        assertEquals("discovery must stop at the probe that saw the cancel", 1, tracking.opens.get());
    }

    /**
     * Splits are emitted in file order even though only some files' boundaries are probed in the deferred phase:
     * a small file finishes during planning while a large one comes back later, and the large one's macro-splits
     * must still land in its file's position rather than after every planned file.
     */
    public void testSplitOrderFollowsFileOrderWhenOnlySomeFilesAreProbed() throws Exception {
        byte[] small = "x,y,z\n".repeat(4).getBytes(StandardCharsets.UTF_8);
        Map<String, byte[]> payloads = Map.of("a-small.csv", small, "b-large.csv", delimitedPayload("a,b,c\n"), "c-small.csv", small);

        ExecutorService executor = Executors.newFixedThreadPool(4);
        List<ExternalSplit> splits;
        try {
            splits = discoverPlainCsvSplits(payloads, CSV_MIN_SEGMENT_BYTES, executor, null);
        } finally {
            executor.shutdown();
        }

        List<String> objectOrder = new ArrayList<>();
        for (ExternalSplit split : splits) {
            String objectName = ((FileSplit) split).path().objectName();
            if (objectOrder.isEmpty() || objectOrder.get(objectOrder.size() - 1).equals(objectName) == false) {
                objectOrder.add(objectName);
            }
        }
        assertEquals(List.of("a-small.csv", "b-large.csv", "c-small.csv"), objectOrder);
        assertThat("the large file must contribute several macro-splits", splits.size(), greaterThan(3));
    }

    /** A plain-CSV payload of ~3.5 MiB: above two minimum segments, so it yields several macro-splits. */
    private static byte[] delimitedPayload(String lineContent) {
        StringBuilder sb = new StringBuilder();
        while (sb.length() < 3 * CSV_MIN_SEGMENT_BYTES + CSV_MIN_SEGMENT_BYTES / 2) {
            sb.append(lineContent);
        }
        return sb.toString().getBytes(StandardCharsets.UTF_8);
    }

    /** Offsets, lengths, and position stamps of a split list, in order: enough to tell two split sets apart. */
    private static List<String> describe(List<ExternalSplit> splits) {
        List<String> described = new ArrayList<>(splits.size());
        for (ExternalSplit split : splits) {
            FileSplit fileSplit = (FileSplit) split;
            described.add(
                fileSplit.path()
                    + "["
                    + fileSplit.offset()
                    + ","
                    + fileSplit.length()
                    + ",first="
                    + FileSplitProvider.isFirstInFile(fileSplit)
                    + ",last="
                    + FileSplitProvider.isLastInFile(fileSplit)
                    + "]"
            );
        }
        return described;
    }

    private static List<ExternalSplit> discoverPlainCsvSplits(
        Map<String, byte[]> payloads,
        long targetStrideBytes,
        @Nullable Executor executor,
        @Nullable StreamTracking tracking
    ) {
        return discoverPlainCsvSplits(payloads, targetStrideBytes, executor, tracking, Settings.EMPTY);
    }

    private static List<ExternalSplit> discoverPlainCsvSplits(
        Map<String, byte[]> payloads,
        long targetStrideBytes,
        @Nullable Executor executor,
        @Nullable StreamTracking tracking,
        Settings settings
    ) {
        return discoverPlainCsvSplits(payloads, targetStrideBytes, executor, tracking, settings, () -> false);
    }

    /**
     * Runs full split discovery over one or more plain-mode CSV files, optionally on an executor and against a
     * stream-tracking storage provider.
     */
    private static List<ExternalSplit> discoverPlainCsvSplits(
        Map<String, byte[]> payloads,
        long targetStrideBytes,
        @Nullable Executor executor,
        @Nullable StreamTracking tracking,
        Settings settings,
        BooleanSupplier isCancelled
    ) {
        return discoverCsvSplits(payloads, targetStrideBytes, executor, tracking, settings, isCancelled, Map.of("mode", "plain"));
    }

    /**
     * As {@link #discoverPlainCsvSplits}, but with the CSV mode left to the caller: {@code mode=plain} takes the
     * strided probe path and the default (quoted) mode takes the sequential proven walk.
     */
    private static List<ExternalSplit> discoverCsvSplits(
        Map<String, byte[]> payloads,
        long targetStrideBytes,
        @Nullable Executor executor,
        @Nullable StreamTracking tracking,
        Settings settings,
        BooleanSupplier isCancelled,
        Map<String, Object> csvConfig
    ) {
        FormatReaderRegistry formatRegistry = new FormatReaderRegistry(new DecompressionCodecRegistry());
        formatRegistry.registerLazy(
            "csv",
            (s, bf) -> new CsvFormatReader(bf, CsvFormatOptions.DEFAULT, "csv", List.of(".csv")),
            Settings.EMPTY,
            null
        );
        formatRegistry.registerExtension(".csv", "csv");
        formatRegistry.byName("csv");

        FileSplitProvider provider = new FileSplitProvider(
            targetStrideBytes,
            new DecompressionCodecRegistry(),
            createMultiFileStorageRegistry(payloads, tracking),
            formatRegistry,
            settings,
            executor
        );

        List<StorageEntry> entries = new ArrayList<>();
        for (String objectName : new TreeSet<>(payloads.keySet())) {
            entries.add(new StorageEntry(StoragePath.of("s3://b/" + objectName), payloads.get(objectName).length, Instant.EPOCH));
        }
        FileList fileList = GlobExpander.fileListOf(entries, "s3://b/*.csv");
        SplitDiscoveryContext ctx = new SplitDiscoveryContext(
            null,
            fileList,
            Map.of(),
            csvConfig,
            PartitionMetadata.EMPTY,
            List.of(),
            ExternalSchema.EMPTY,
            null,
            SegmentableFormatReader.DEFAULT_MAX_RECORD_BYTES,
            isCancelled,
            DeclaredReadSpec.NONE
        );
        return provider.discoverSplits(ctx).splits();
    }

    /**
     * Counts storage streams opened and how many were open at once. Each opened stream waits for
     * {@code expectedOverlap} peers to arrive, bounded by a timeout so a serialized run still finishes and simply
     * reports a peak of 1. Without that wait, in-memory reads finish so fast that an assertion about overlapping
     * probes would depend on scheduling luck.
     */
    private static final class StreamTracking {
        private static final long OVERLAP_WAIT_MILLIS = 5_000;
        private static final int NEVER_FAIL = 0;

        final AtomicInteger peakInFlight = new AtomicInteger();
        final AtomicInteger opens = new AtomicInteger();
        final AtomicInteger closes = new AtomicInteger();
        /** Storage objects handed out, which planning does before any stream is opened. */
        final AtomicInteger objects = new AtomicInteger();
        private final AtomicInteger inFlight = new AtomicInteger();
        private final CountDownLatch overlap;
        /** Ordinal of the stream whose reads fail, standing in for a mid-probe I/O error, or {@link #NEVER_FAIL}. */
        private final int failReadsOnOpen;

        StreamTracking(int expectedOverlap) {
            this(expectedOverlap, NEVER_FAIL);
        }

        StreamTracking(int expectedOverlap, int failReadsOnOpen) {
            this.overlap = new CountDownLatch(expectedOverlap);
            this.failReadsOnOpen = failReadsOnOpen;
        }

        /** @return the ordinal of the stream just opened, counting from one */
        int opened() throws InterruptedException {
            int ordinal = opens.incrementAndGet();
            int current = inFlight.incrementAndGet();
            peakInFlight.accumulateAndGet(current, Math::max);
            overlap.countDown();
            overlap.await(OVERLAP_WAIT_MILLIS, TimeUnit.MILLISECONDS);
            return ordinal;
        }

        void closed() {
            closes.incrementAndGet();
            inFlight.decrementAndGet();
        }
    }

    /** Storage provider serving a distinct payload per object name, so a multi-file test can vary file sizes. */
    private static StorageProviderRegistry createMultiFileStorageRegistry(Map<String, byte[]> payloads, @Nullable StreamTracking tracking) {
        StorageProviderRegistry registry = new StorageProviderRegistry(Settings.EMPTY);
        StorageProvider provider = new StorageProvider() {
            @Override
            public StorageObject newObject(StoragePath path) {
                return newObject(path, payloadFor(path).length);
            }

            @Override
            public StorageObject newObject(StoragePath path, long length) {
                return newObject(path, length, Instant.EPOCH);
            }

            @Override
            public StorageObject newObject(StoragePath path, long length, Instant lastModified) {
                byte[] payload = payloadFor(path);
                if (tracking != null) {
                    tracking.objects.incrementAndGet();
                }
                return new StorageObject() {
                    @Override
                    public InputStream newStream() {
                        return trackedStream(new ByteArrayInputStream(payload));
                    }

                    @Override
                    public InputStream newStream(long position, long len) {
                        return trackedStream(new ByteArrayInputStream(payload, Math.toIntExact(position), Math.toIntExact(len)));
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

            private InputStream trackedStream(InputStream delegate) {
                if (tracking == null) {
                    return delegate;
                }
                int ordinal;
                try {
                    ordinal = tracking.opened();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new AssertionError("interrupted while tracking stream", e);
                }
                if (ordinal == tracking.failReadsOnOpen) {
                    return new InputStream() {
                        @Override
                        public int read() throws IOException {
                            throw new IOException("connection reset");
                        }
                    };
                }
                return new FilterInputStream(delegate) {
                    @Override
                    public void close() throws IOException {
                        tracking.closed();
                        super.close();
                    }
                };
            }

            private byte[] payloadFor(StoragePath path) {
                byte[] payload = payloads.get(path.objectName());
                assertNotNull("no payload registered for " + path, payload);
                return payload;
            }

            @Override
            public StorageIterator listObjects(StoragePath prefix, boolean recursive) {
                throw new UnsupportedOperationException();
            }

            @Override
            public boolean exists(StoragePath path) {
                return payloads.containsKey(path.objectName());
            }

            @Override
            public List<String> supportedSchemes() {
                return List.of("s3");
            }

            @Override
            public void close() {}
        };
        registry.registerFactory("s3", StorageProviderFactory.noConfigKeys(() -> provider));
        return registry;
    }

    /**
     * Full {@link FileSplitProvider#discoverSplits} path for a quoted CSV file whose quoted fields carry
     * embedded newlines and {@code ""}-escaped quotes: it emits multiple record-aligned macro-splits (proving
     * the {@code requiresSequentialWholeFileRead} gate lets a proven-capable quoted splitter through). Every
     * emitted macro-split must begin at a proven record start (balanced quotes before it).
     */
    public void testDiscoverSplitsMacroSplitsQuotedCsv() {
        String quotedLine = "1,\"embedded\nnewline\",\"has \"\"quote\"\"\"\n";

        List<ExternalSplit> splits = discoverRealDelimitedSplits(Map.of(), "q.csv", ".csv", CsvFormatOptions.DEFAULT, quotedLine);
        assertThat("quoted CSV must macro-split", splits.size(), greaterThan(1));
        for (ExternalSplit split : splits) {
            FileSplit fileSplit = (FileSplit) split;
            if (fileSplit.offset() == 0) {
                continue;
            }
            assertEquals(
                "macro-split must be record-aligned",
                "true",
                fileSplit.config().get(FileSplitProvider.RECORD_ALIGNED_MACRO_SPLIT_KEY)
            );
        }
    }

    /**
     * Quoted CSV (the default {@code .csv} mode, whose quoted fields may embed newlines) macro-splits via the
     * proven-probe path: {@link RecordBoundaryProbe#provenBoundaries} emits boundaries for
     * a non-strided but proven-capable splitter. Every emitted boundary must be a true record start, checked
     * against the trusted sequential scanner {@link RecordSplitter#findNextRecordBoundary} looped from the file
     * start (its prefix sums are the true record starts), and the boundaries must be strictly increasing. The
     * payload carries {@code ""}-escaped quotes, embedded newlines, and CRLF rows inside quoted fields so a
     * naive strided scan would mis-split; the probe must not.
     */
    public void testRecordAlignedMacroSplitDiscoveryProvesQuotedCsvBoundaries() throws IOException {
        var blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(new NoopCircuitBreaker("test")).build();

        // Build a CSV payload exceeding 3 MiB so macro-splits form (minimumSegmentSize defaults to 1 MiB).
        // Quoted fields carry both ""-escaped quotes and embedded raw newlines.
        StringBuilder csv = new StringBuilder();
        csv.append("id,name,note\n");
        int dataRows = 0;
        while (csv.length() < 3 * 1024 * 1024) {
            if (dataRows % 3 == 0) {
                csv.append(dataRows).append(",\"has \"\"escaped\"\" quotes\",ok\r\n"); // CRLF row guards \r handling
            } else if (dataRows % 3 == 1) {
                csv.append(dataRows).append(",\"embedded\nnewline\",\"another \"\"quoted\"\" value\"\n");
            } else {
                csv.append(dataRows).append(",simple,value\n");
            }
            dataRows++;
        }
        byte[] payload = csv.toString().getBytes(StandardCharsets.UTF_8);
        long fileLength = payload.length;
        assertTrue("payload must exceed 2 MiB so macro-splits form", fileLength > 2 * 1024 * 1024);

        // Default construction => quoting on => CsvRecordSplitter: non-strided but proven-capable.
        var csvReader = new CsvFormatReader(blockFactory);
        StorageObject obj = createInMemoryStorageObject(payload, StoragePath.of("mem://test.csv"));
        Set<Long> trueStarts = trueRecordStarts(csvReader.recordSplitter(SegmentableFormatReader.DEFAULT_MAX_RECORD_BYTES), payload);

        long stride = fileLength / 4;
        RecordBoundaryProbe.ProvenWalk walk = RecordBoundaryProbe.provenBoundaries(
            csvReader.recordSplitter(SegmentableFormatReader.DEFAULT_MAX_RECORD_BYTES),
            obj,
            fileLength,
            stride,
            csvReader.minimumSegmentSize(),
            () -> false
        );

        List<Long> starts = walk.boundaries();
        assertFalse("every record here is one the walk can get past", walk.stoppedBeforeEndOfFile());
        assertThat("expected multiple proven macro-split boundaries", starts.size(), greaterThan(1));
        assertEquals("first boundary is always the file start", 0L, (long) starts.get(0));
        long prev = -1;
        for (long start : starts) {
            assertThat("boundaries must be strictly increasing", start, greaterThan(prev));
            prev = start;
            assertTrue("boundary " + start + " must be a true record start", trueStarts.contains(start));
        }
    }

    /**
     * True record starts: the file start (0) plus every prefix sum of {@link RecordSplitter#findNextRecordBoundary}
     * consumed lengths from the trusted sequential scanner.
     */
    private static Set<Long> trueRecordStarts(RecordSplitter splitter, byte[] payload) throws IOException {
        Set<Long> starts = new TreeSet<>();
        starts.add(0L);
        long acc = 0;
        BufferedInputStream in = new BufferedInputStream(new ByteArrayInputStream(payload));
        long consumed;
        while ((consumed = splitter.findNextRecordBoundary(in)) >= 0) {
            acc += consumed;
            starts.add(acc);
        }
        return starts;
    }

    /**
     * A payload of {@code rows} strides' worth of fixed-width CSV rows, for the drain-versus-abort tests below.
     * Rows divide the stride exactly so probe offsets land on row boundaries and the byte accounting is exact.
     */
    private static byte[] stridesOfRows(long stride, int strides) {
        String row = "0123456789,0123456789,012345678\n";
        assertEquals("rows must tile the stride exactly", 0, stride % row.length());
        return row.repeat(Math.toIntExact(strides * stride / row.length())).getBytes(StandardCharsets.UTF_8);
    }

    /**
     * A probe with little enough of its window left to transfer drains the rest of it and closes, so the HTTP
     * connection returns to the pool for the next probe to reuse. It must not abort (an aborted partial body drops
     * the connection, forcing a fresh handshake per probe), and it must not open a range to end-of-file (which
     * would drain far more than the window).
     */
    public void testSerialStridedProbesDrainBoundedProbeWindows() throws IOException {
        var blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(new NoopCircuitBreaker("test")).build();

        // A stride at the drain threshold caps every window there too, so no probe has more than
        // MAX_DRAIN_BYTES left to transfer and all of them drain.
        long stride = RecordBoundaryProbe.MAX_DRAIN_BYTES;
        byte[] payload = stridesOfRows(stride, 32);
        long fileLength = payload.length;

        DrainSimulatingStorageObject.Tracking tracking = new DrainSimulatingStorageObject.Tracking();
        StorageObject object = DrainSimulatingStorageObject.create(payload, tracking);

        // Plain mode keeps strided probing; macro-split discovery refuses non-strided (default/quoted) CSV,
        // which is read whole-file instead.
        var csvReader = (SegmentableFormatReader) new CsvFormatReader(blockFactory).withConfig(Map.of("mode", "plain"));
        List<Long> starts = serialStridedStarts(csvReader, object, fileLength, stride, SegmentableFormatReader.DEFAULT_MAX_RECORD_BYTES);

        assertThat("expected multiple macro-split boundaries", starts.size(), greaterThan(1));
        assertEquals("strided probes pool the connection by draining, never abort", 0, tracking.abortCalls.get());
        assertTrue("probe streams must be closed", tracking.closed.get());
        // Each probe transfers exactly its own window, which is the stride here: all of it, because it drained
        // (an abort would have transferred only the few bytes up to the first newline), and no more than it,
        // because the range is bounded rather than opened to end-of-file.
        int probes = RecordBoundaryProbe.stridedPositions(fileLength, stride, csvReader.minimumSegmentSize()).size();
        assertEquals(
            "each probe must drain its own bounded window and no more, of a " + fileLength + " byte file",
            probes * stride,
            tracking.bytesConsumed.get()
        );
    }

    /**
     * A probe that finds its boundary in the first row of a full-width window has nearly all of that window left,
     * and above {@link RecordBoundaryProbe#MAX_DRAIN_BYTES} left the next probe's handshake is the cheaper of the
     * two. Such a probe therefore aborts: it pays a connection per probe and transfers only the bytes it scanned.
     */
    public void testStridedProbesAbortRatherThanDrainWhenTooMuchOfTheWindowIsLeft() throws IOException {
        var blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(new NoopCircuitBreaker("test")).build();

        // The window is the stride, so a stride at twice the drain threshold leaves every probe above it.
        long stride = FULL_WIDTH_WINDOW_BYTES;
        byte[] payload = stridesOfRows(stride, 8);
        long fileLength = payload.length;

        DrainSimulatingStorageObject.Tracking tracking = new DrainSimulatingStorageObject.Tracking();
        StorageObject object = DrainSimulatingStorageObject.create(payload, tracking);

        var csvReader = (SegmentableFormatReader) new CsvFormatReader(blockFactory).withConfig(Map.of("mode", "plain"));
        List<Long> starts = serialStridedStarts(csvReader, object, fileLength, stride, SegmentableFormatReader.DEFAULT_MAX_RECORD_BYTES);

        int probes = RecordBoundaryProbe.stridedPositions(fileLength, stride, csvReader.minimumSegmentSize()).size();
        assertThat("the fixture must still split the file", starts.size(), greaterThan(1));
        assertEquals("every probe with more than the drain threshold left aborts", probes, tracking.abortCalls.get());
        // Each row falls on a stride boundary, so a probe finds its terminator within the first row and reads
        // essentially nothing. Draining would instead have transferred all of the file.
        assertThat(
            "an aborting probe transfers only what it scanned; consumed " + tracking.bytesConsumed.get() + " of " + fileLength,
            tracking.bytesConsumed.get(),
            lessThan(fileLength / 8)
        );
    }

    /**
     * The release rule turns on how much of the window is left to transfer, and on nothing else. Two probes open
     * the same full-width window at the same offset of the same file, and differ only in how far the record there
     * runs: the one that scans down to {@link RecordBoundaryProbe#MAX_DRAIN_BYTES} left drains, the one that finds
     * its boundary immediately aborts.
     */
    public void testTheReleaseRuleTurnsOnTheBytesLeftToTransfer() throws IOException {
        long window = FULL_WIDTH_WINDOW_BYTES;

        DrainSimulatingStorageObject.Tracking drainedIt = probeWindowWhoseRecordRunsFor(window - RecordBoundaryProbe.MAX_DRAIN_BYTES);
        assertEquals("a probe with only the threshold left drains its window", 0, drainedIt.abortCalls.get());
        assertEquals("which transfers all of it", window, drainedIt.bytesConsumed.get());

        DrainSimulatingStorageObject.Tracking abortedIt = probeWindowWhoseRecordRunsFor(64);
        assertEquals("a probe with more than the threshold left aborts instead", 1, abortedIt.abortCalls.get());
        assertThat("transferring only what it scanned", abortedIt.bytesConsumed.get(), lessThan(window));
    }

    /**
     * Probes one full-width window whose record at the probe offset ends {@code recordBytes} in, and reports what
     * the storage object saw. The offset, the stride and so the window are the same for every call, so the bytes
     * the probe is left to transfer are the only thing that varies.
     */
    private static DrainSimulatingStorageObject.Tracking probeWindowWhoseRecordRunsFor(long recordBytes) throws IOException {
        int window = Math.toIntExact(FULL_WIDTH_WINDOW_BYTES);
        byte[] payload = new byte[3 * window];
        Arrays.fill(payload, (byte) 'x');
        // A newline just before the probe offset, so the offset itself starts a record, and the one that ends it.
        payload[window - 1] = '\n';
        payload[Math.toIntExact(window + recordBytes) - 1] = '\n';
        payload[payload.length - 1] = '\n';

        DrainSimulatingStorageObject.Tracking tracking = new DrainSimulatingStorageObject.Tracking();
        StorageObject object = DrainSimulatingStorageObject.create(payload, tracking);
        RecordBoundaryProbe.probeAt(
            stridedSplitter(),
            object,
            window,
            payload.length,
            1,
            window,
            SegmentableFormatReader.DEFAULT_MAX_RECORD_BYTES,
            () -> false
        );
        return tracking;
    }

    // strided macro-split discovery: fixed probe offsets, independent probes

    /**
     * A strided (newline-terminated) record splitter. Plain-mode CSV turns quoting off, so every newline
     * terminates a record and the splitter can be probed at any offset, exactly like NDJSON's.
     */
    private static RecordSplitter stridedSplitter() {
        var blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(new NoopCircuitBreaker("test")).build();
        var reader = (SegmentableFormatReader) new CsvFormatReader(blockFactory).withConfig(Map.of("mode", "plain"));
        return reader.recordSplitter(SegmentableFormatReader.DEFAULT_MAX_RECORD_BYTES);
    }

    /**
     * A strided walk probes fixed multiples of the stride rather than re-anchoring on each boundary it finds.
     * That independence is what lets the probes run concurrently: every position is known before any read.
     */
    public void testMacroSplitProbePositionsAreFixedStrideMultiples() {
        assertEquals(List.of(100L, 200L, 300L, 400L), RecordBoundaryProbe.stridedPositions(500, 100, 1));
    }

    /** A stride offset with less than a minimum segment behind it would produce a runt split, so it is not probed. */
    public void testMacroSplitProbePositionsStopBeforeAShortTail() {
        assertEquals(List.of(100L, 200L), RecordBoundaryProbe.stridedPositions(500, 100, 250));
        assertEquals(List.of(), RecordBoundaryProbe.stridedPositions(500, 100, 500));
    }

    /** No offset is inside a file at or below one stride, which is the caller's "too small to split" case. */
    public void testMacroSplitProbePositionsEmptyForFileWithinOneStride() {
        assertEquals(List.of(), RecordBoundaryProbe.stridedPositions(100, 100, 1));
        assertEquals(List.of(), RecordBoundaryProbe.stridedPositions(60, 100, 1));
    }

    /** The file start is always a split start, and each probed boundary follows in probe order. */
    public void testReduceProbeOutcomesSeedsTheFileStart() {
        List<Long> boundaries = RecordBoundaryProbe.reduce(
            List.of(RecordBoundaryProbe.Outcome.at(120), RecordBoundaryProbe.Outcome.at(240))
        );
        assertEquals(List.of(0L, 120L, 240L), boundaries);
    }

    /**
     * An offset that yielded no boundary costs the one split it would have started: the boundaries after it still
     * stand, and the splits either side of it merge into the one that spans it.
     */
    public void testReduceProbeOutcomesSkipsAnOffsetThatFoundNoBoundary() {
        List<Long> boundaries = RecordBoundaryProbe.reduce(
            List.of(RecordBoundaryProbe.Outcome.at(120), RecordBoundaryProbe.Outcome.NONE, RecordBoundaryProbe.Outcome.at(360))
        );
        assertEquals(List.of(0L, 120L, 360L), boundaries);
    }

    /**
     * A found boundary that would leave a short tail is dropped the same way as an offset that found nothing:
     * it does not start a split, and the spans either side of it merge.
     */
    public void testReduceProbeOutcomesSkipsATailTooShortOffset() {
        List<Long> boundaries = RecordBoundaryProbe.reduce(
            List.of(RecordBoundaryProbe.Outcome.at(120), RecordBoundaryProbe.Outcome.TAIL_TOO_SHORT, RecordBoundaryProbe.Outcome.at(360))
        );
        assertEquals(List.of(0L, 120L, 360L), boundaries);
    }

    /** Two stride offsets landing inside one record resolve to the same boundary; only one split starts there. */
    public void testReduceProbeOutcomesDropsBoundariesThatDoNotAdvance() {
        List<Long> boundaries = RecordBoundaryProbe.reduce(
            List.of(RecordBoundaryProbe.Outcome.at(120), RecordBoundaryProbe.Outcome.at(120), RecordBoundaryProbe.Outcome.at(240))
        );
        assertEquals(List.of(0L, 120L, 240L), boundaries);
    }

    /**
     * A record longer than the probe window leaves no boundary to split at, so the probe yields none rather than
     * reading on to end-of-file. Bounding the read is what keeps a probe a small, predictable ranged GET.
     */
    public void testProbeStridedBoundaryYieldsNoBoundaryWhenNoneInWindow() throws IOException {
        int window = 64 * 1024;
        byte[] payload = new byte[4 * window];
        Arrays.fill(payload, (byte) 'x');
        payload[payload.length - 1] = '\n';
        StorageObject object = createInMemoryStorageObject(payload, StoragePath.of("mem://long-line.ndjson"));

        RecordBoundaryProbe.Outcome outcome = RecordBoundaryProbe.probeAt(
            stridedSplitter(),
            object,
            window,
            payload.length,
            1,
            window,
            SegmentableFormatReader.DEFAULT_MAX_RECORD_BYTES,
            () -> false
        );

        assertEquals("a record spanning the whole window leaves nothing to split at", RecordBoundaryProbe.Outcome.NONE, outcome);
    }

    /** A boundary too close to end-of-file would leave a short final split, so the probe yields tail-too-short instead. */
    public void testProbeStridedBoundaryYieldsTailTooShortWhenTailIsBelowMinimumSegment() throws IOException {
        byte[] payload = "aaaa\nbbbb\ncccc\n".getBytes(StandardCharsets.UTF_8);
        StorageObject object = createInMemoryStorageObject(payload, StoragePath.of("mem://short.ndjson"));
        RecordSplitter splitter = stridedSplitter();

        int maxRecordBytes = SegmentableFormatReader.DEFAULT_MAX_RECORD_BYTES;
        assertEquals(
            RecordBoundaryProbe.Outcome.at(10),
            RecordBoundaryProbe.probeAt(splitter, object, 5, payload.length, 5, payload.length, maxRecordBytes, () -> false)
        );
        // With a 6-byte minimum segment, the same boundary leaves only 5 of the 15 bytes behind it.
        assertEquals(
            RecordBoundaryProbe.Outcome.TAIL_TOO_SHORT,
            RecordBoundaryProbe.probeAt(splitter, object, 5, payload.length, 6, payload.length, maxRecordBytes, () -> false)
        );
    }

    /**
     * A probe that fails mid-read aborts its stream rather than draining it: the read is not going to be reused,
     * so the connection (and the storage permit it holds) should be released at once instead of after pointlessly
     * transferring the rest of the window.
     */
    public void testProbeStridedBoundaryAbortsAFailedRead() {
        AtomicInteger abortCalls = new AtomicInteger();
        StorageObject failing = new StorageObject() {
            @Override
            public InputStream newStream() {
                return newStream(0, 1);
            }

            @Override
            public InputStream newStream(long position, long length) {
                return new InputStream() {
                    @Override
                    public int read() throws IOException {
                        throw new IOException("connection reset");
                    }
                };
            }

            @Override
            public void abortStream(InputStream stream) throws IOException {
                abortCalls.incrementAndGet();
                stream.close();
            }

            @Override
            public long length() {
                return 4096;
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
                return StoragePath.of("mem://failing.ndjson");
            }
        };

        expectThrows(
            IOException.class,
            () -> RecordBoundaryProbe.probeAt(
                stridedSplitter(),
                failing,
                1024,
                4096,
                1,
                1024,
                SegmentableFormatReader.DEFAULT_MAX_RECORD_BYTES,
                () -> false
            )
        );
        assertEquals("a failed probe must abort, not drain", 1, abortCalls.get());
    }

    /**
     * A CRLF straddling the probe window's edge. The CR is the window's last byte, so the scan looking for the LF
     * behind it sees the window end and reports a clean terminator at the CR. Cutting there would start the next
     * split on the orphaned LF, which is not a record start, so the offset yields nothing instead and the span
     * open across it runs on to the next offset that does resolve.
     */
    public void testABoundaryAtTheEdgeOfATruncatedWindowIsRejected() throws IOException {
        int window = 4096;
        // The probe at `window` reads [window, 2 * window). Fill that range with non-terminator bytes up to a CR on
        // its very last byte, whose LF is the first byte the probe cannot see.
        byte[] payload = new byte[4 * window];
        Arrays.fill(payload, (byte) 'x');
        payload[window - 1] = '\n';
        payload[2 * window - 1] = '\r';
        payload[2 * window] = '\n';
        payload[payload.length - 1] = '\n';
        StorageObject object = createInMemoryStorageObject(payload, StoragePath.of("mem://crlf-edge.csv"));

        RecordBoundaryProbe.Outcome outcome = RecordBoundaryProbe.probeAt(
            stridedSplitter(),
            object,
            window,
            payload.length,
            1,
            window,
            SegmentableFormatReader.DEFAULT_MAX_RECORD_BYTES,
            () -> false
        );

        assertEquals(
            "a terminator found against the window's edge is not one the file agrees to",
            RecordBoundaryProbe.Outcome.NONE,
            outcome
        );
        // A byte earlier and the LF is inside the window, which is the same file cut the way it should be.
        byte[] insideTheWindow = payload.clone();
        insideTheWindow[2 * window - 1] = 'x';
        insideTheWindow[2 * window - 3] = '\r';
        insideTheWindow[2 * window - 2] = '\n';
        StorageObject shifted = createInMemoryStorageObject(insideTheWindow, StoragePath.of("mem://crlf-inside.csv"));

        assertEquals(
            "the same terminator one byte further in is complete, and is cut at",
            RecordBoundaryProbe.Outcome.at(2L * window - 1),
            RecordBoundaryProbe.probeAt(
                stridedSplitter(),
                shifted,
                window,
                insideTheWindow.length,
                1,
                window,
                SegmentableFormatReader.DEFAULT_MAX_RECORD_BYTES,
                () -> false
            )
        );
    }

    /**
     * A probe cancelled after its scan aborts its stream rather than draining it. The drain exists only to hand a
     * pooled connection to the next probe, and a cancelled query has no next probe, so the transfer would buy
     * nothing.
     */
    public void testAProbeCancelledMidScanAbortsInsteadOfDraining() {
        // A stride at the drain threshold caps the window there, so an uncancelled probe here would drain.
        long stride = RecordBoundaryProbe.MAX_DRAIN_BYTES;
        byte[] payload = stridesOfRows(stride, 3);
        DrainSimulatingStorageObject.Tracking tracking = new DrainSimulatingStorageObject.Tracking();
        StorageObject object = DrainSimulatingStorageObject.create(payload, tracking);

        // False on the probe's entry check, true on the check it makes after finding its boundary.
        AtomicInteger checks = new AtomicInteger();
        BooleanSupplier cancelAfterTheScan = () -> checks.getAndIncrement() > 0;

        expectThrows(
            TaskCancelledException.class,
            () -> RecordBoundaryProbe.probeAt(
                stridedSplitter(),
                object,
                stride,
                payload.length,
                1,
                stride,
                SegmentableFormatReader.DEFAULT_MAX_RECORD_BYTES,
                cancelAfterTheScan
            )
        );

        assertEquals("a cancelled probe aborts its stream", 1, tracking.abortCalls.get());
        assertThat("and does not drain its window first", tracking.bytesConsumed.get(), lessThan(RecordBoundaryProbe.MAX_DRAIN_BYTES));
    }

    /** A probe checks for cancellation before opening its stream, so a cancelled query issues no further reads. */
    public void testProbeStridedBoundaryFailsFastOnCancellation() {
        AtomicInteger streamsOpened = new AtomicInteger();
        byte[] payload = "aaaa\nbbbb\ncccc\n".getBytes(StandardCharsets.UTF_8);
        StorageObject counting = new StorageObject() {
            @Override
            public InputStream newStream() {
                streamsOpened.incrementAndGet();
                return new ByteArrayInputStream(payload);
            }

            @Override
            public InputStream newStream(long position, long length) {
                streamsOpened.incrementAndGet();
                return new ByteArrayInputStream(payload, (int) position, (int) length);
            }

            @Override
            public long length() {
                return payload.length;
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
                return StoragePath.of("mem://cancelled.ndjson");
            }
        };

        expectThrows(
            TaskCancelledException.class,
            () -> RecordBoundaryProbe.probeAt(
                stridedSplitter(),
                counting,
                5,
                payload.length,
                1,
                payload.length,
                SegmentableFormatReader.DEFAULT_MAX_RECORD_BYTES,
                () -> true
            )
        );
        assertEquals("a cancelled probe must not read", 0, streamsOpened.get());
    }

    /**
     * A record longer than the splitter's maximum cannot be measured by a probe, so the offset inside it yields no
     * boundary and the split that began before it runs on past the record. The offsets beyond the record are
     * probed as usual, so a record the splitter refuses to span costs the one split its offset would have started.
     */
    public void testMacroSplitDiscoverySkipsAnOffsetInsideAnOversizedRecord() throws IOException {
        var blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(new NoopCircuitBreaker("test")).build();
        int maxRecordBytes = 16;
        long stride = 256 * 1024;
        // A record straddling the first stride offset, long enough that the probe there runs past maxRecordBytes
        // before reaching its terminator. The filler rows divide into the offset exactly, so it starts where meant.
        long oversizedStart = stride - 64;
        StringBuilder csv = new StringBuilder();
        while (csv.length() < oversizedStart) {
            csv.append("tail\n");
        }
        assertEquals("the filler must end exactly where the oversized record starts", oversizedStart, csv.length());
        csv.append("x".repeat(127)).append('\n');
        long oversizedEnd = csv.length();
        while (csv.length() < 2 * CSV_MIN_SEGMENT_BYTES) {
            csv.append("tail\n");
        }
        byte[] payload = csv.toString().getBytes(StandardCharsets.UTF_8);
        StorageObject object = createInMemoryStorageObject(payload, StoragePath.of("mem://test.csv"));
        // Plain mode: the max-record-size verdict is format-agnostic, but macro-split discovery refuses non-strided
        // (default/quoted) CSV. Plain CSV keeps strided probing.
        var csvReader = (SegmentableFormatReader) new CsvFormatReader(blockFactory).withConfig(Map.of("mode", "plain"));

        List<Long> starts = serialStridedStarts(csvReader, object, payload.length, stride, maxRecordBytes);

        // A maximum the record does fit in leaves every offset able to start a split, which is the count to
        // measure the oversized record's cost against.
        List<Long> unrestricted = serialStridedStarts(
            csvReader,
            object,
            payload.length,
            stride,
            SegmentableFormatReader.DEFAULT_MAX_RECORD_BYTES
        );
        assertThat("the payload must offer several offsets to probe", unrestricted.size(), greaterThan(2));
        assertEquals("only the offset inside the oversized record loses its split", unrestricted.size() - 1, starts.size());
        for (long start : starts) {
            assertFalse(
                "no split may start inside a record the splitter cannot span: " + start,
                start > oversizedStart && start < oversizedEnd
            );
        }
    }

    /**
     * The serial form of the strided walk a node without a discovery executor falls back to: the fixed stride
     * offsets of {@code reader}'s splitter, probed on the calling thread and reduced the same way the concurrent
     * gather reduces them.
     */
    private static List<Long> serialStridedStarts(
        SegmentableFormatReader reader,
        StorageObject object,
        long fileLength,
        long targetStrideBytes,
        int maxRecordBytes
    ) throws IOException {
        long minSegment = reader.minimumSegmentSize();
        return RecordBoundaryProbe.reduce(
            RecordBoundaryProbe.stridedOutcomes(
                reader.recordSplitter(maxRecordBytes),
                object,
                fileLength,
                RecordBoundaryProbe.stridedPositions(fileLength, targetStrideBytes, minSegment),
                minSegment,
                targetStrideBytes,
                maxRecordBytes,
                () -> false
            )
        );
    }

    private static StorageObject createInMemoryStorageObject(byte[] data, StoragePath path) {
        return new StorageObject() {
            @Override
            public InputStream newStream() {
                return new ByteArrayInputStream(data);
            }

            @Override
            public InputStream newStream(long position, long length) {
                return new ByteArrayInputStream(data, (int) position, (int) length);
            }

            @Override
            public long length() {
                return data.length;
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
        };
    }

    public void testTargetSplitSizeConfigOverride() {
        StorageEntry entry = new StorageEntry(StoragePath.of("s3://b/data.ndjson"), 3000, Instant.EPOCH);
        FileList fileList = GlobExpander.fileListOf(List.of(entry), "s3://b/*.ndjson");

        Map<String, Object> config = Map.of(FileSplitProvider.CONFIG_TARGET_SPLIT_SIZE, "1kb");
        SplitDiscoveryContext ctx = new SplitDiscoveryContext(null, fileList, config, PartitionMetadata.EMPTY, List.of());
        List<ExternalSplit> splits = provider.discoverSplits(ctx).splits();

        assertEquals(1, splits.size());
        assertEquals(3000, ((FileSplit) splits.get(0)).length());
    }

    public void testTargetSplitSizeConfigOverrideMb() {
        long fileSize = 300 * 1024 * 1024L; // 300 MB
        StorageEntry entry = new StorageEntry(StoragePath.of("s3://b/data.ndjson"), fileSize, Instant.EPOCH);
        FileList fileList = GlobExpander.fileListOf(List.of(entry), "s3://b/*.ndjson");

        Map<String, Object> config = Map.of(FileSplitProvider.CONFIG_TARGET_SPLIT_SIZE, "32mb");
        SplitDiscoveryContext ctx = new SplitDiscoveryContext(null, fileList, config, PartitionMetadata.EMPTY, List.of());
        List<ExternalSplit> splits = provider.discoverSplits(ctx).splits();

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
        // Assert the message, not just the type: IllegalArgumentException is far broader than the
        // QlIllegalArgumentException this replaced, so a bare type check would pass on an unrelated failure.
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> provider.discoverSplits(ctx));
        assertThat(e.getMessage(), containsString("target_split_size"));
        assertEquals(RestStatus.BAD_REQUEST, ExceptionsHelper.status(e));
    }

    public void testFileSizeExactlyEqualsSplitSizeProducesSingleSplit() {
        long targetSize = 1000;
        FileSplitProvider splitter = new FileSplitProvider(targetSize);

        StorageEntry entry = new StorageEntry(StoragePath.of("s3://b/exact.ndjson"), targetSize, Instant.EPOCH);
        FileList fileList = GlobExpander.fileListOf(List.of(entry), "s3://b/*.ndjson");

        SplitDiscoveryContext ctx = new SplitDiscoveryContext(null, fileList, Map.of(), PartitionMetadata.EMPTY, List.of());
        List<ExternalSplit> splits = splitter.discoverSplits(ctx).splits();

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
        List<ExternalSplit> splits = provider.discoverSplits(ctx).splits();

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
        List<ExternalSplit> splits = splitter.discoverSplits(ctx).splits();

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
        List<ExternalSplit> splits = splitter.discoverSplits(ctx).splits();

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

    public void testRangeAwareSplitsRekeyRenamesAndPoisonRetypesDeclaredStats() {
        // Footer range stats are keyed by PHYSICAL names (id, amount) and hold inferred-type values. A declaration
        // renames id->emp_id (same type: LONG) and re-types amount->price (LONG->KEYWORD). The split boundary must
        // rekey the rename (values unchanged, so emp_id keeps correct min/max/count) and poison the re-type (price's
        // extrema dropped + markers written, counts stripped), while row_count survives so COUNT(*) stays warm.
        Map<String, Object> rawStats = new HashMap<>();
        rawStats.put(SourceStatisticsSerializer.STATS_ROW_COUNT, 100L);
        rawStats.put(SourceStatisticsSerializer.columnMinKey("id"), 0L);
        rawStats.put(SourceStatisticsSerializer.columnMaxKey("id"), 99L);
        rawStats.put(SourceStatisticsSerializer.columnValueCountKey("id"), 100L);
        rawStats.put(SourceStatisticsSerializer.columnMinKey("amount"), 5L);
        rawStats.put(SourceStatisticsSerializer.columnValueCountKey("amount"), 100L);

        RangeAwareFormatReader mockReader = createMockRangeReader(List.of(new SplitRange(100, 500, rawStats)));
        FormatReaderRegistry formatRegistry = new FormatReaderRegistry(new DecompressionCodecRegistry());
        formatRegistry.registerLazy("parquet", (s, bf) -> mockReader, Settings.EMPTY, null);
        formatRegistry.byName("parquet");
        FileSplitProvider splitter = new FileSplitProvider(
            FileSplitProvider.DEFAULT_TARGET_SPLIT_SIZE,
            new DecompressionCodecRegistry(),
            createMockStorageRegistry(),
            formatRegistry,
            Settings.EMPTY
        );

        StorageEntry entry = new StorageEntry(StoragePath.of("s3://b/data.parquet"), 2000, Instant.EPOCH);
        FileList fileList = GlobExpander.fileListOf(List.of(entry), "s3://b/*.parquet");

        // Overlaid (logical) read schema + unified schema: emp_id:long, price:keyword. Pre-overlay inferred file types
        // (physical): id:long, amount:long.
        List<Attribute> overlaid = List.of(
            new ReferenceAttribute(Source.EMPTY, "emp_id", DataType.LONG),
            new ReferenceAttribute(Source.EMPTY, "price", DataType.KEYWORD)
        );
        Map<String, DataType> inferredTypes = Map.of("id", DataType.LONG, "amount", DataType.LONG);
        Map<StoragePath, SchemaReconciliation.FileSchemaInfo> schemaMap = Map.of(
            entry.path(),
            new SchemaReconciliation.FileSchemaInfo(new ExternalSchema(overlaid), null, null, inferredTypes)
        );
        DeclaredReadSpec spec = DeclaredReadSpec.of(
            Map.of("emp_id", "id", "price", "amount"), // logical -> physical
            null,
            Map.of(),
            Set.of("emp_id", "price")
        );
        ExternalSchema schema = new ExternalSchema(overlaid);
        SplitDiscoveryContext ctx = new SplitDiscoveryContext(
            null,
            fileList,
            schemaMap,
            Map.of(),
            PartitionMetadata.EMPTY,
            List.of(),
            schema,
            schema,
            SegmentableFormatReader.DEFAULT_MAX_RECORD_BYTES,
            () -> false,
            spec
        );

        List<ExternalSplit> splits = splitter.discoverSplits(ctx).splits();
        assertEquals(1, splits.size());
        Map<String, Object> stats = ((FileSplit) splits.get(0)).statistics();

        // rename: id's family moved to emp_id, physical key gone
        assertEquals(0L, stats.get(SourceStatisticsSerializer.columnMinKey("emp_id")));
        assertEquals(99L, stats.get(SourceStatisticsSerializer.columnMaxKey("emp_id")));
        assertEquals(100L, stats.get(SourceStatisticsSerializer.columnValueCountKey("emp_id")));
        assertNull(stats.get(SourceStatisticsSerializer.columnMinKey("id")));
        // re-type: price poisoned — extremum dropped + marker written, count stripped
        assertNull(stats.get(SourceStatisticsSerializer.columnMinKey("price")));
        assertEquals(Boolean.TRUE, stats.get(SourceStatisticsSerializer.columnMinUnservableKey("price")));
        assertNull(stats.get(SourceStatisticsSerializer.columnValueCountKey("price")));
        // COUNT(*) stays warm
        assertEquals(100L, stats.get(SourceStatisticsSerializer.STATS_ROW_COUNT));
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
        List<ExternalSplit> splits = splitter.discoverSplits(ctx).splits();

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
        List<ExternalSplit> splits = splitter.discoverSplits(ctx).splits();

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
            public Configured<FormatReader> withConfigTrackingConsumedKeys(Map<String, Object> config) {
                return Configured.empty(this);
            }

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
            public RowPositionStrategy rowPositionStrategy() {
                return PassThroughRowPositionStrategy.INSTANCE;
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
        List<ExternalSplit> splits = splitter.discoverSplits(ctx).splits();

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
            public Configured<FormatReader> withConfigTrackingConsumedKeys(Map<String, Object> config) {
                return Configured.empty(this);
            }

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
            public RowPositionStrategy rowPositionStrategy() {
                return PassThroughRowPositionStrategy.INSTANCE;
            }

            @Override
            public void close() {}
        };
    }

    private static StorageProviderRegistry createMockStorageRegistry() {
        StorageProviderRegistry registry = new StorageProviderRegistry(Settings.EMPTY);
        StorageProvider mockProvider = new StorageProvider() {
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
        };
        registry.registerFactory("s3", StorageProviderFactory.noConfigKeys(() -> mockProvider));
        return registry;
    }

    /** S3 mock that serves {@code payload} for range reads (newline boundary scanning during split discovery). */
    private static StorageProviderRegistry createPayloadStorageRegistry(byte[] payload) {
        StorageProviderRegistry registry = new StorageProviderRegistry(Settings.EMPTY);
        StorageProvider payloadProvider = new StorageProvider() {
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
        };
        registry.registerFactory("s3", StorageProviderFactory.noConfigKeys(() -> payloadProvider));
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
        schemaInfo.put(
            pathA,
            new SchemaReconciliation.FileSchemaInfo(new ExternalSchema(List.of(refAttr("id"), refAttr("name"))), null, null)
        );
        schemaInfo.put(
            pathB,
            new SchemaReconciliation.FileSchemaInfo(
                new ExternalSchema(List.of(refAttr("id"), refAttr("name"), refAttr("bonus"))),
                null,
                null
            )
        );
        schemaInfo.put(pathC, new SchemaReconciliation.FileSchemaInfo(new ExternalSchema(List.of(refAttr("bonus"))), null, null));
        SplitDiscoveryContext ctx = new SplitDiscoveryContext(
            null,
            fileList,
            schemaInfo,
            Map.of(),
            PartitionMetadata.EMPTY,
            List.of(),
            new ExternalSchema(List.of(refAttr("id"), refAttr("name")))
        );
        List<ExternalSplit> splits = provider.discoverSplits(ctx).splits();

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
        schemaInfo.put(
            pathA,
            new SchemaReconciliation.FileSchemaInfo(new ExternalSchema(List.of(refAttr("id"), refAttr("name"))), null, null)
        );
        schemaInfo.put(
            pathB,
            new SchemaReconciliation.FileSchemaInfo(new ExternalSchema(List.of(refAttr("bonus"), refAttr("name"))), null, null)
        );
        SplitDiscoveryContext ctx = new SplitDiscoveryContext(
            null,
            fileList,
            schemaInfo,
            Map.of(),
            PartitionMetadata.EMPTY,
            List.of(),
            new ExternalSchema(List.of(refAttr("id"), refAttr("name")))
        );
        List<ExternalSplit> splits = provider.discoverSplits(ctx).splits();

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
        schemaInfo.put(pathA, new SchemaReconciliation.FileSchemaInfo(new ExternalSchema(List.of(refAttr("id"))), null, null));
        schemaInfo.put(pathB, new SchemaReconciliation.FileSchemaInfo(new ExternalSchema(List.of(refAttr("bonus"))), null, null));
        SplitDiscoveryContext ctx = new SplitDiscoveryContext(
            null,
            fileList,
            schemaInfo,
            Map.of(),
            PartitionMetadata.EMPTY,
            List.of(),
            ExternalSchema.EMPTY
        );
        List<ExternalSplit> splits = provider.discoverSplits(ctx).splits();

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
            new ExternalSchema(List.of(refAttr("id"), refAttr("name")))
        );
        List<ExternalSplit> splits = provider.discoverSplits(ctx).splits();

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
        schemaInfo.put(pathA, new SchemaReconciliation.FileSchemaInfo(new ExternalSchema(List.of(refAttr("id"))), null, null));
        schemaInfo.put(pathB, new SchemaReconciliation.FileSchemaInfo(new ExternalSchema(List.of(refAttr("bonus"))), null, null));
        SplitDiscoveryContext ctx = new SplitDiscoveryContext(
            null,
            fileList,
            schemaInfo,
            Map.of(),
            partitions,
            List.of(),
            new ExternalSchema(List.of(refAttr("year")))
        );
        List<ExternalSplit> splits = provider.discoverSplits(ctx).splits();

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
        schemaInfo.put(
            pathA,
            new SchemaReconciliation.FileSchemaInfo(new ExternalSchema(List.of(refAttr("id"), refAttr("name"))), null, null)
        );
        // pathB intentionally has no entry in schemaInfo
        SplitDiscoveryContext ctx = new SplitDiscoveryContext(
            null,
            fileList,
            schemaInfo,
            Map.of(),
            PartitionMetadata.EMPTY,
            List.of(),
            new ExternalSchema(List.of(refAttr("id"), refAttr("name")))
        );
        List<ExternalSplit> splits = provider.discoverSplits(ctx).splits();

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
        schemaInfo.put(
            pathA,
            new SchemaReconciliation.FileSchemaInfo(new ExternalSchema(List.of(refAttr("id"), refAttr("name"))), null, null)
        );
        schemaInfo.put(pathB, new SchemaReconciliation.FileSchemaInfo(new ExternalSchema(List.of(refAttr("bonus"))), null, null));
        schemaInfo.put(
            pathC,
            new SchemaReconciliation.FileSchemaInfo(new ExternalSchema(List.of(refAttr("id"), refAttr("name"))), null, null)
        );
        Expression yearFilter = new Equals(SRC, fieldAttr("year"), intLiteral(2024));
        SplitDiscoveryContext ctx = new SplitDiscoveryContext(
            null,
            fileList,
            schemaInfo,
            Map.of(),
            partitions,
            List.of(yearFilter),
            new ExternalSchema(List.of(refAttr("id"), refAttr("name")))
        );
        List<ExternalSplit> splits = provider.discoverSplits(ctx).splits();

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
        List<ExternalSplit> splits = provider.discoverSplits(ctx).splits();

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
        schemaInfo.put(
            pathA,
            new SchemaReconciliation.FileSchemaInfo(new ExternalSchema(List.of(refAttr("id"), refAttr("name"))), null, null)
        );
        Expression yearFilter = new Equals(SRC, fieldAttr("year"), intLiteral(2024));
        SplitDiscoveryContext ctx = new SplitDiscoveryContext(
            null,
            fileList,
            schemaInfo,
            Map.of(),
            partitions,
            List.of(yearFilter),
            new ExternalSchema(List.of(refAttr("id")))
        );
        List<ExternalSplit> splits = provider.discoverSplits(ctx).splits();

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
        schemaInfo.put(
            pathA,
            new SchemaReconciliation.FileSchemaInfo(new ExternalSchema(List.of(refAttr("id"), refAttr("price"))), null, null)
        );
        schemaInfo.put(pathB, new SchemaReconciliation.FileSchemaInfo(new ExternalSchema(List.of(refAttr("id"))), null, null));
        schemaInfo.put(
            pathC,
            new SchemaReconciliation.FileSchemaInfo(new ExternalSchema(List.of(refAttr("id"), refAttr("price"))), null, null)
        );
        // year=2024 filter prunes pathC; price > 100 filter prunes pathB (missing 'price')
        List<Expression> filters = List.of(
            new Equals(SRC, fieldAttr("year"), intLiteral(2024)),
            new GreaterThan(SRC, fieldAttr("price"), intLiteral(100), null)
        );
        SplitDiscoveryContext ctx = new SplitDiscoveryContext(
            null,
            fileList,
            schemaInfo,
            Map.of(),
            partitions,
            filters,
            new ExternalSchema(List.of(refAttr("id"), refAttr("price")))
        );
        List<ExternalSplit> splits = provider.discoverSplits(ctx).splits();

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
        List<ExternalSplit> splits = splitter.discoverSplits(ctx).splits();

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
        List<ExternalSplit> splits = splitter.discoverSplits(ctx).splits();

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

    public void testCancellationAbortsDiscoveryMidLoop() {
        // Five files processed sequentially (no executor). The cancellation signal flips true partway
        // through, so discovery must throw and stop polling rather than processing every file.
        StorageEntry e1 = new StorageEntry(StoragePath.of("s3://b/a.parquet"), 100, Instant.EPOCH);
        StorageEntry e2 = new StorageEntry(StoragePath.of("s3://b/b.parquet"), 100, Instant.EPOCH);
        StorageEntry e3 = new StorageEntry(StoragePath.of("s3://b/c.parquet"), 100, Instant.EPOCH);
        StorageEntry e4 = new StorageEntry(StoragePath.of("s3://b/d.parquet"), 100, Instant.EPOCH);
        StorageEntry e5 = new StorageEntry(StoragePath.of("s3://b/e.parquet"), 100, Instant.EPOCH);
        FileList fileList = GlobExpander.fileListOf(List.of(e1, e2, e3, e4, e5), "s3://b/*.parquet");

        // The supplier is polled once before the loop and once per file at the top of processFileForSplits.
        // It reports cancelled starting at the 4th poll, so only the first two files are processed.
        AtomicInteger polls = new AtomicInteger();
        BooleanSupplier cancel = () -> polls.incrementAndGet() > 3;

        SplitDiscoveryContext ctx = new SplitDiscoveryContext(
            null,
            fileList,
            Map.of(),
            Map.of(),
            PartitionMetadata.EMPTY,
            List.of(),
            ExternalSchema.EMPTY,
            null,
            SegmentableFormatReader.DEFAULT_MAX_RECORD_BYTES,
            cancel,
            DeclaredReadSpec.NONE
        );

        expectThrows(TaskCancelledException.class, () -> provider.discoverSplits(ctx));
        // Polling stopped as soon as cancellation was observed (4 polls), well short of the 6 it would take
        // to process all five files, proving discovery short-circuited.
        assertEquals(4, polls.get());
    }

    public void testNotCancelledProcessesAllFiles() {
        StorageEntry e1 = new StorageEntry(StoragePath.of("s3://b/a.parquet"), 100, Instant.EPOCH);
        StorageEntry e2 = new StorageEntry(StoragePath.of("s3://b/b.parquet"), 100, Instant.EPOCH);
        StorageEntry e3 = new StorageEntry(StoragePath.of("s3://b/c.parquet"), 100, Instant.EPOCH);
        FileList fileList = GlobExpander.fileListOf(List.of(e1, e2, e3), "s3://b/*.parquet");

        SplitDiscoveryContext ctx = new SplitDiscoveryContext(
            null,
            fileList,
            Map.of(),
            Map.of(),
            PartitionMetadata.EMPTY,
            List.of(),
            ExternalSchema.EMPTY,
            null,
            SegmentableFormatReader.DEFAULT_MAX_RECORD_BYTES,
            () -> false,
            DeclaredReadSpec.NONE
        );

        assertEquals(3, provider.discoverSplits(ctx).splits().size());
    }

    // -- the matcher may only ever fail to prune; these pin the cases where it used to prune a matching file --

    /**
     * Integral partition values must be compared as longs, not doubles. Above 2^53 a double cannot separate adjacent
     * longs, so {@code 9007199254740992 == 9007199254740993} came out TRUE, {@code !=} came out FALSE, and a file
     * whose every row matches the filter was pruned away. A LONG partition column holding epoch-micros or snowflake
     * ids reaches this range routinely.
     */
    public void testLargeLongPartitionValuesAreNotCollapsedByDoublePrecision() {
        Map<String, Object> values = Map.of("ts", 9007199254740992L);
        Literal adjacent = new Literal(SRC, 9007199254740993L, DataType.LONG);

        assertEquals(Boolean.FALSE, FileSplitProvider.evaluateFilter(new Equals(SRC, fieldAttr("ts"), adjacent), values));
        assertTrue(
            "ts != <adjacent long> is TRUE, so the file must be kept — pruning it drops every matching row",
            FileSplitProvider.matchesPartitionFilters(values, List.of(new NotEquals(SRC, fieldAttr("ts"), adjacent)))
        );
        assertTrue(
            "ts < <adjacent long> is TRUE, so the file must be kept",
            FileSplitProvider.matchesPartitionFilters(values, List.of(new LessThan(SRC, fieldAttr("ts"), adjacent)))
        );
    }

    /**
     * Keyword ranges must order by UTF-8 bytes, the way ES|QL orders keywords. {@link String#compareTo} orders by
     * UTF-16 code units, which puts a supplementary-plane character (its leading surrogate, 0xD83D) <em>below</em>
     * a private-use char like U+E000 — the opposite of the engine's answer. The file would be pruned while its rows
     * satisfy the predicate.
     */
    public void testKeywordRangeUsesUtf8ByteOrderNotUtf16() {
        Map<String, Object> values = Map.of("region", "\uD83D\uDE00"); // U+1F600 GRINNING FACE, above the BMP
        Literal privateUse = new Literal(SRC, new BytesRef("\uE000"), DataType.KEYWORD); // U+E000, top of the BMP

        assertEquals(
            "U+1F600 > U+E000 by code point, so the row matches and the file must be kept",
            Boolean.TRUE,
            FileSplitProvider.evaluateFilter(new GreaterThan(SRC, fieldAttr("region"), privateUse), values)
        );
    }

    /**
     * A literal on the left of an asymmetric operator keeps its side. Applying the comparator with the column first
     * would evaluate {@code year > 2024} for {@code 2024 > year} — the exact inverse, pruning precisely the files
     * that match. {@code LiteralsOnTheRight} normalizes this away upstream, so the matcher is never handed this shape
     * today; it must still not be wrong if it is.
     */
    public void testLiteralOnTheLeftKeepsOperandOrder() {
        Map<String, Object> values = Map.of("year", 2020);

        // 2024 > year -> 2024 > 2020 -> TRUE (the file matches, and must be kept)
        assertEquals(Boolean.TRUE, FileSplitProvider.evaluateFilter(new GreaterThan(SRC, intLiteral(2024), fieldAttr("year")), values));
        // 2024 < year -> 2024 < 2020 -> FALSE (the file cannot match, and may be pruned)
        assertEquals(Boolean.FALSE, FileSplitProvider.evaluateFilter(new LessThan(SRC, intLiteral(2024), fieldAttr("year")), values));
    }

    /**
     * A compressed file whose codec finds no block boundaries falls back to one whole-file split, and that
     * split still states its position.
     * <p>
     * Pinned at the producer rather than through the position helpers: a whole-file split with no keys is
     * currently rescued by the compatibility path for splits from older coordinators, so an unstamped
     * producer looks correct until that path is deleted — at which point the file's final record would go
     * missing again with every other test still green.
     */
    public void testCompressedFallbackWithNoBoundariesStampsPosition() {
        assertWholeFileSplitStamped(splitsFromCompressedFallback());
    }

    private static List<ExternalSplit> splitsFromCompressedFallback() {
        // A codec that reports no block boundaries at all sends the provider down its whole-file fallback.
        DecompressionCodecRegistry codecRegistry = new DecompressionCodecRegistry();
        codecRegistry.register(new FakeSplittableCodec(new long[0]));
        FileSplitProvider splitter = new FileSplitProvider(
            FileSplitProvider.DEFAULT_TARGET_SPLIT_SIZE,
            codecRegistry,
            createMockStorageRegistry(),
            new FormatReaderRegistry(codecRegistry),
            Settings.EMPTY
        );
        StorageEntry entry = new StorageEntry(StoragePath.of("s3://b/noboundaries.ndjson.bz2"), 1_000_000L, Instant.EPOCH);
        return splitter.discoverSplits(
            new SplitDiscoveryContext(
                null,
                GlobExpander.fileListOf(List.of(entry), "s3://b/*.ndjson.bz2"),
                Map.of(),
                PartitionMetadata.EMPTY,
                List.of()
            )
        ).splits();
    }

    private static void assertWholeFileSplitStamped(List<ExternalSplit> splits) {
        assertEquals("the fallback emits exactly one whole-file split", 1, splits.size());
        FileSplit whole = (FileSplit) splits.get(0);
        assertEquals(0, whole.offset());
        assertEquals("true", whole.config().get(FileSplitProvider.FIRST_SPLIT_KEY));
        assertEquals("true", whole.config().get(FileSplitProvider.LAST_SPLIT_KEY));
    }

    /**
     * Every shape of split this class can produce, with the position it must report.
     * <p>
     * Split position used to be re-derived at each consumer under differing rules, so a whole-file read
     * answered "first" in one place and "not last" in another — and readers discarded its final record.
     * This table is the single statement of the contract. <b>A new split shape must add a row here.</b>
     */
    public void testSplitPositionAcrossEveryProducibleShape() {
        StoragePath p = StoragePath.of("s3://b/f.ndjson");

        // Whole file: owns both ends.
        assertPosition("stamped whole-file", split(p, 0, 100, Map.of("_first_split", "true", "_last_split", "true")), true, true);

        // Newline-aligned macro-splits: the edges are stamped, the middle is stamped as neither.
        Map<String, Object> ram = Map.of("_record_aligned_macro_split", "true");
        assertPosition("newline macro first", split(p, 0, 40, withKeys(ram, "_first_split")), true, false);
        assertPosition("newline macro middle", split(p, 40, 40, ram), false, false);
        assertPosition("newline macro last", split(p, 80, 20, withKeys(ram, "_last_split")), false, true);

        // Compressed block-aligned macro-splits. The first starts at offset 0, so an offset-based rule
        // would call it whole-file; its protocol key is what rules it out.
        Map<String, Object> cos = Map.of("_compressed_offset_split", "true");
        assertPosition("compressed macro first", split(p, 0, 40, withKeys(cos, "_first_split")), true, false);
        assertPosition("compressed macro middle", split(p, 40, 40, cos), false, false);
        assertPosition("compressed macro last", split(p, 80, 20, withKeys(cos, "_last_split")), false, true);

        // Range splits carry no position keys by design — byte ranges are not a record-boundary protocol.
        Map<String, Object> range = Map.of("_range_split", "true");
        assertPosition("range split at offset 0", split(p, 0, 40, range), true, false);
        assertPosition("range split mid-file", split(p, 40, 40, range), false, false);

        // A split from a coordinator that predates position stamping: recognised by the BWC belt.
        assertPosition("legacy unstamped whole-file", split(p, 0, 100, Map.of()), true, true);
    }

    /** The belt must recognise only genuine whole-file splits — anything covering part of a file is excluded. */
    public void testLegacyBeltExcludesEveryPartialFileShape() {
        StoragePath p = StoragePath.of("s3://b/f.ndjson");
        assertFalse("mid-file offset is never whole-file", FileSplitProvider.isLastInFile(split(p, 500, 40, Map.of())));
        assertFalse(
            "record-aligned macro at offset 0 is not whole-file",
            FileSplitProvider.isLastInFile(split(p, 0, 40, Map.of("_record_aligned_macro_split", "true")))
        );
        assertFalse(
            "compressed macro at offset 0 is not whole-file",
            FileSplitProvider.isLastInFile(split(p, 0, 40, Map.of("_compressed_offset_split", "true")))
        );
        assertFalse(
            "range split at offset 0 is not whole-file",
            FileSplitProvider.isLastInFile(split(p, 0, 40, Map.of("_range_split", "true")))
        );
        // An explicit stamp always wins over the belt's inference.
        assertTrue(FileSplitProvider.isLastInFile(split(p, 500, 40, Map.of("_last_split", "true"))));
    }

    private static void assertPosition(String shape, FileSplit split, boolean first, boolean last) {
        assertEquals(shape + ": isFirstInFile", first, FileSplitProvider.isFirstInFile(split));
        assertEquals(shape + ": isLastInFile", last, FileSplitProvider.isLastInFile(split));
    }

    private static FileSplit split(StoragePath path, long offset, long length, Map<String, Object> config) {
        return new FileSplit("file", path, offset, length, ".ndjson", config, Map.of());
    }

    private static Map<String, Object> withKeys(Map<String, Object> base, String... extra) {
        Map<String, Object> out = new HashMap<>(base);
        for (String k : extra) {
            out.put(k, "true");
        }
        return out;
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
