/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalSplit;
import org.elasticsearch.xpack.esql.datasources.spi.SplitDiscoveryContext;
import org.elasticsearch.xpack.esql.datasources.spi.SplitProvider;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

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
}
