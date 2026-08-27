/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.glob;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.PartitionMetadata;
import org.elasticsearch.xpack.esql.datasources.StorageEntry;
import org.elasticsearch.xpack.esql.datasources.spi.FileList;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.time.Instant;
import java.util.List;
import java.util.Map;

public class GenericFileListTests extends ESTestCase {

    public void testUnresolvedIdentity() {
        assertSame(FileList.UNRESOLVED, FileList.UNRESOLVED);
    }

    public void testEmptyIdentity() {
        assertSame(FileList.EMPTY, FileList.EMPTY);
    }

    public void testUnresolvedIsNotEmpty() {
        assertNotSame(FileList.UNRESOLVED, FileList.EMPTY);
    }

    public void testIsResolvedForRegularGenericFileList() {
        StorageEntry entry = new StorageEntry(StoragePath.of("s3://bucket/file.parquet"), 100, Instant.EPOCH);
        FileList fileList = GlobExpander.fileListOf(List.of(entry), "s3://bucket/*.parquet");
        assertTrue(fileList.isResolved());
        assertFalse(fileList.isEmpty());
    }

    public void testUnresolvedSentinelSemantics() {
        assertFalse(FileList.UNRESOLVED.isResolved());
        assertFalse(FileList.UNRESOLVED.isEmpty());
    }

    public void testEmptySentinelSemantics() {
        assertTrue(FileList.EMPTY.isResolved());
        assertTrue(FileList.EMPTY.isEmpty());
    }

    public void testSizeMatchesFiles() {
        StorageEntry e1 = new StorageEntry(StoragePath.of("s3://b/a.parquet"), 10, Instant.EPOCH);
        StorageEntry e2 = new StorageEntry(StoragePath.of("s3://b/b.parquet"), 20, Instant.EPOCH);
        GenericFileList fileList = (GenericFileList) GlobExpander.fileListOf(List.of(e1, e2), "s3://b/*.parquet");
        assertEquals(fileList.files().size(), fileList.size());
        assertEquals(2, fileList.size());
    }

    public void testOriginalPatternPreserved() {
        String pattern = "s3://bucket/data/**" + "/*.parquet";
        StorageEntry entry = new StorageEntry(StoragePath.of("s3://bucket/data/sub/file.parquet"), 50, Instant.EPOCH);
        GenericFileList fileList = (GenericFileList) GlobExpander.fileListOf(List.of(entry), pattern);
        assertEquals(pattern, fileList.originalPattern());
    }

    public void testSentinelsNotEqual() {
        assertNotEquals(FileList.UNRESOLVED, FileList.EMPTY);
        assertNotEquals(FileList.EMPTY, FileList.UNRESOLVED);
        assertNotEquals(FileList.UNRESOLVED.hashCode(), FileList.EMPTY.hashCode());
    }

    public void testPartitionMetadataNullByDefault() {
        StorageEntry entry = new StorageEntry(StoragePath.of("s3://bucket/file.parquet"), 100, Instant.EPOCH);
        FileList fileList = GlobExpander.fileListOf(List.of(entry), "s3://bucket/*.parquet");
        assertNull(fileList.partitionMetadata());
    }

    public void testPartitionMetadataAttached() {
        StoragePath path = StoragePath.of("s3://bucket/year=2024/file.parquet");
        StorageEntry entry = new StorageEntry(path, 100, Instant.EPOCH);
        PartitionMetadata pm = new PartitionMetadata(Map.of("year", DataType.INTEGER), Map.of(path, Map.of("year", 2024)));
        FileList fileList = GlobExpander.fileListOf(List.of(entry), "s3://bucket/year=*/*.parquet", pm);
        assertNotNull(fileList.partitionMetadata());
        assertFalse(fileList.partitionMetadata().isEmpty());
        assertEquals(DataType.INTEGER, fileList.partitionMetadata().partitionColumns().get("year"));
    }

    public void testSentinelsHaveNullPartitionMetadata() {
        assertNull(FileList.UNRESOLVED.partitionMetadata());
        assertNull(FileList.EMPTY.partitionMetadata());
    }

    public void testEqualityWithPartitionMetadata() {
        StoragePath path = StoragePath.of("s3://bucket/year=2024/file.parquet");
        StorageEntry entry = new StorageEntry(path, 100, Instant.EPOCH);
        PartitionMetadata pm = new PartitionMetadata(Map.of("year", DataType.INTEGER), Map.of(path, Map.of("year", 2024)));
        GenericFileList a = (GenericFileList) GlobExpander.fileListOf(List.of(entry), "s3://bucket/year=*/*.parquet", pm);
        GenericFileList b = (GenericFileList) GlobExpander.fileListOf(List.of(entry), "s3://bucket/year=*/*.parquet", pm);
        GenericFileList c = (GenericFileList) GlobExpander.fileListOf(List.of(entry), "s3://bucket/year=*/*.parquet", null);

        assertEquals(a, b);
        assertNotEquals(a, c);
    }
}
