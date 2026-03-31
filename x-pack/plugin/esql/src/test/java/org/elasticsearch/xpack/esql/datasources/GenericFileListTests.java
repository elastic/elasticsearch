/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.time.Instant;
import java.util.List;
import java.util.Map;

public class GenericFileListTests extends ESTestCase {

    public void testUnresolvedIdentity() {
        assertSame(GenericFileList.UNRESOLVED, GenericFileList.UNRESOLVED);
    }

    public void testEmptyIdentity() {
        assertSame(GenericFileList.EMPTY, GenericFileList.EMPTY);
    }

    public void testUnresolvedIsNotEmpty() {
        assertNotSame(GenericFileList.UNRESOLVED, GenericFileList.EMPTY);
    }

    public void testIsResolvedForRegularGenericFileList() {
        StorageEntry entry = new StorageEntry(StoragePath.of("s3://bucket/file.parquet"), 100, Instant.EPOCH);
        GenericFileList fileList = new GenericFileList(List.of(entry), "s3://bucket/*.parquet");
        assertTrue(fileList.isResolved());
        assertFalse(fileList.isUnresolved());
        assertFalse(fileList.isEmpty());
    }

    public void testIsUnresolved() {
        assertTrue(GenericFileList.UNRESOLVED.isUnresolved());
        assertFalse(GenericFileList.EMPTY.isUnresolved());
    }

    public void testIsEmpty() {
        assertTrue(GenericFileList.EMPTY.isEmpty());
        assertFalse(GenericFileList.UNRESOLVED.isEmpty());
    }

    public void testSizeMatchesFiles() {
        StorageEntry e1 = new StorageEntry(StoragePath.of("s3://b/a.parquet"), 10, Instant.EPOCH);
        StorageEntry e2 = new StorageEntry(StoragePath.of("s3://b/b.parquet"), 20, Instant.EPOCH);
        GenericFileList fileList = new GenericFileList(List.of(e1, e2), "s3://b/*.parquet");
        assertEquals(fileList.files().size(), fileList.size());
        assertEquals(2, fileList.size());
    }

    public void testOriginalPatternPreserved() {
        String pattern = "s3://bucket/data/**" + "/*.parquet";
        StorageEntry entry = new StorageEntry(StoragePath.of("s3://bucket/data/sub/file.parquet"), 50, Instant.EPOCH);
        GenericFileList fileList = new GenericFileList(List.of(entry), pattern);
        assertEquals(pattern, fileList.originalPattern());
    }

    public void testSentinelsNotEqual() {
        assertNotEquals(GenericFileList.UNRESOLVED, GenericFileList.EMPTY);
        assertNotEquals(GenericFileList.EMPTY, GenericFileList.UNRESOLVED);
        assertNotEquals(GenericFileList.UNRESOLVED.hashCode(), GenericFileList.EMPTY.hashCode());
    }

    public void testPartitionMetadataNullByDefault() {
        StorageEntry entry = new StorageEntry(StoragePath.of("s3://bucket/file.parquet"), 100, Instant.EPOCH);
        GenericFileList fileList = new GenericFileList(List.of(entry), "s3://bucket/*.parquet");
        assertNull(fileList.partitionMetadata());
    }

    public void testPartitionMetadataAttached() {
        StoragePath path = StoragePath.of("s3://bucket/year=2024/file.parquet");
        StorageEntry entry = new StorageEntry(path, 100, Instant.EPOCH);
        PartitionMetadata pm = new PartitionMetadata(Map.of("year", DataType.INTEGER), Map.of(path, Map.of("year", 2024)));
        GenericFileList fileList = new GenericFileList(List.of(entry), "s3://bucket/year=*/*.parquet", pm);
        assertNotNull(fileList.partitionMetadata());
        assertFalse(fileList.partitionMetadata().isEmpty());
        assertEquals(DataType.INTEGER, fileList.partitionMetadata().partitionColumns().get("year"));
    }

    public void testSentinelsHaveNullPartitionMetadata() {
        assertNull(GenericFileList.UNRESOLVED.partitionMetadata());
        assertNull(GenericFileList.EMPTY.partitionMetadata());
    }

    public void testEqualityWithPartitionMetadata() {
        StoragePath path = StoragePath.of("s3://bucket/year=2024/file.parquet");
        StorageEntry entry = new StorageEntry(path, 100, Instant.EPOCH);
        PartitionMetadata pm = new PartitionMetadata(Map.of("year", DataType.INTEGER), Map.of(path, Map.of("year", 2024)));
        GenericFileList a = new GenericFileList(List.of(entry), "s3://bucket/year=*/*.parquet", pm);
        GenericFileList b = new GenericFileList(List.of(entry), "s3://bucket/year=*/*.parquet", pm);
        GenericFileList c = new GenericFileList(List.of(entry), "s3://bucket/year=*/*.parquet", null);

        assertEquals(a, b);
        assertNotEquals(a, c);
    }
}
