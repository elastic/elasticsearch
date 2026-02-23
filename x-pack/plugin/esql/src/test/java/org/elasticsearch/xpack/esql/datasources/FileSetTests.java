/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.time.Instant;
import java.util.List;

public class FileSetTests extends ESTestCase {

    public void testUnresolvedIdentity() {
        assertSame(FileSet.UNRESOLVED, FileSet.UNRESOLVED);
    }

    public void testEmptyIdentity() {
        assertSame(FileSet.EMPTY, FileSet.EMPTY);
    }

    public void testUnresolvedIsNotEmpty() {
        assertNotSame(FileSet.UNRESOLVED, FileSet.EMPTY);
    }

    public void testIsResolvedForRegularFileSet() {
        StorageEntry entry = new StorageEntry(StoragePath.of("s3://bucket/file.parquet"), 100, Instant.EPOCH);
        FileSet fileSet = new FileSet(List.of(entry), "s3://bucket/*.parquet");
        assertTrue(fileSet.isResolved());
        assertFalse(fileSet.isUnresolved());
        assertFalse(fileSet.isEmpty());
    }

    public void testIsUnresolved() {
        assertTrue(FileSet.UNRESOLVED.isUnresolved());
        assertFalse(FileSet.EMPTY.isUnresolved());
    }

    public void testIsEmpty() {
        assertTrue(FileSet.EMPTY.isEmpty());
        assertFalse(FileSet.UNRESOLVED.isEmpty());
    }

    public void testSizeMatchesFiles() {
        StorageEntry e1 = new StorageEntry(StoragePath.of("s3://b/a.parquet"), 10, Instant.EPOCH);
        StorageEntry e2 = new StorageEntry(StoragePath.of("s3://b/b.parquet"), 20, Instant.EPOCH);
        FileSet fileSet = new FileSet(List.of(e1, e2), "s3://b/*.parquet");
        assertEquals(fileSet.files().size(), fileSet.size());
        assertEquals(2, fileSet.size());
    }

    public void testOriginalPatternPreserved() {
        String pattern = "s3://bucket/data/**" + "/*.parquet";
        StorageEntry entry = new StorageEntry(StoragePath.of("s3://bucket/data/sub/file.parquet"), 50, Instant.EPOCH);
        FileSet fileSet = new FileSet(List.of(entry), pattern);
        assertEquals(pattern, fileSet.originalPattern());
    }

    public void testSentinelsNotEqual() {
        assertNotEquals(FileSet.UNRESOLVED, FileSet.EMPTY);
        assertNotEquals(FileSet.EMPTY, FileSet.UNRESOLVED);
        assertNotEquals(FileSet.UNRESOLVED.hashCode(), FileSet.EMPTY.hashCode());
    }
}
