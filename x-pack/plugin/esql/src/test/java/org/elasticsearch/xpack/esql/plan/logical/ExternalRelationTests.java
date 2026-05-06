/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.datasources.StorageEntry;
import org.elasticsearch.xpack.esql.datasources.glob.GlobExpander;
import org.elasticsearch.xpack.esql.datasources.spi.FileList;
import org.elasticsearch.xpack.esql.datasources.spi.SourceMetadata;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.plan.physical.ExternalSourceExec;

import java.time.Instant;
import java.util.List;
import java.util.Map;

/**
 * Tests for ExternalRelation and ExternalSourceExec FileList threading.
 * Verifies that FileList is correctly threaded through constructors, toPhysicalExec(),
 * withPushedFilter(), withEstimatedRowSize(), equals/hashCode, and info().
 */
public class ExternalRelationTests extends ESTestCase {

    // ===== ExternalRelation tests =====

    public void testConstructorWithFileList() {
        FileList fileList = createFileList();
        ExternalRelation relation = createRelation(fileList);

        assertSame(fileList, relation.fileList());
        assertTrue(relation.fileList().isResolved());
        assertEquals(2, relation.fileList().fileCount());
    }

    public void testConstructorWithoutFileListDefaultsToUnresolved() {
        SourceMetadata metadata = createMetadata();
        List<Attribute> output = createAttributes();

        ExternalRelation relation = new ExternalRelation(Source.EMPTY, "s3://bucket/data.parquet", metadata, output);

        assertSame(FileList.UNRESOLVED, relation.fileList());
        assertFalse(relation.fileList().isResolved());
    }

    public void testToPhysicalExecThreadsFileList() {
        FileList fileList = createFileList();
        ExternalRelation relation = createRelation(fileList);

        ExternalSourceExec exec = relation.toPhysicalExec();

        assertNotNull(exec);
        assertSame(fileList, exec.fileList());
        assertTrue(exec.fileList().isResolved());
        assertEquals(2, exec.fileList().fileCount());
    }

    public void testWithAttributesPreservesFileList() {
        FileList fileList = createFileList();
        ExternalRelation relation = createRelation(fileList);

        List<Attribute> newAttrs = List.of(attr("new_col", DataType.LONG));
        ExternalRelation updated = relation.withAttributes(newAttrs);

        assertSame(fileList, updated.fileList());
        assertEquals(1, updated.output().size());
        assertEquals("new_col", updated.output().get(0).name());
    }

    public void testEqualsAndHashCodeIncludeFileList() {
        FileList fileList1 = createFileList();
        FileList fileList2 = GlobExpander.fileListOf(
            List.of(new StorageEntry(StoragePath.of("s3://bucket/data/other.parquet"), 999, Instant.EPOCH)),
            "s3://bucket/data/other*.parquet"
        );

        SourceMetadata metadata = createMetadata();
        List<Attribute> output = createAttributes();

        ExternalRelation relation1 = new ExternalRelation(Source.EMPTY, "s3://bucket/data.parquet", metadata, output, fileList1);
        ExternalRelation relation2 = new ExternalRelation(Source.EMPTY, "s3://bucket/data.parquet", metadata, output, fileList1);
        ExternalRelation relation3 = new ExternalRelation(Source.EMPTY, "s3://bucket/data.parquet", metadata, output, fileList2);

        assertEquals(relation1, relation2);
        assertEquals(relation1.hashCode(), relation2.hashCode());
        assertNotEquals(relation1, relation3);
    }

    public void testInfoRoundTripsFileList() {
        FileList fileList = createFileList();
        ExternalRelation relation = createRelation(fileList);

        var info = relation.info();
        assertNotNull(info);
        assertSame(fileList, relation.fileList());
    }

    // ===== ExternalSourceExec FileList threading tests =====

    public void testExecWithPushedFilterPreservesFileList() {
        FileList fileList = createFileList();
        ExternalSourceExec exec = createExec(fileList);

        ExternalSourceExec filtered = exec.withPushedFilter("some_filter_object");

        assertSame(fileList, filtered.fileList());
        assertEquals("some_filter_object", filtered.pushedFilter());
    }

    public void testExecConstructorWithEstimatedRowSizePreservesFileList() {
        FileList fileList = createFileList();
        List<Attribute> attrs = createAttributes();

        ExternalSourceExec exec = new ExternalSourceExec(
            Source.EMPTY,
            "s3://bucket/data.parquet",
            "parquet",
            attrs,
            Map.of(),
            Map.of(),
            null,
            256,
            fileList
        );

        assertSame(fileList, exec.fileList());
        assertEquals(Integer.valueOf(256), exec.estimatedRowSize());
    }

    public void testExecWithoutFileListHasNullFileList() {
        List<Attribute> attrs = createAttributes();
        ExternalSourceExec exec = new ExternalSourceExec(
            Source.EMPTY,
            "s3://bucket/data.parquet",
            "parquet",
            attrs,
            Map.of(),
            Map.of(),
            null,
            null
        );

        assertNull(exec.fileList());

        FileList fileList = createFileList();
        ExternalSourceExec execWithFileList = new ExternalSourceExec(
            Source.EMPTY,
            "s3://bucket/data.parquet",
            "parquet",
            attrs,
            Map.of(),
            Map.of(),
            null,
            null,
            fileList
        );

        assertSame(fileList, execWithFileList.fileList());
    }

    public void testExecEqualsAndHashCodeIncludeFileList() {
        FileList fileList1 = createFileList();
        FileList fileList2 = GlobExpander.fileListOf(
            List.of(new StorageEntry(StoragePath.of("s3://bucket/data/other.parquet"), 999, Instant.EPOCH)),
            "s3://bucket/data/other*.parquet"
        );

        List<Attribute> attrs = createAttributes();

        ExternalSourceExec exec1 = new ExternalSourceExec(
            Source.EMPTY,
            "s3://bucket/data.parquet",
            "parquet",
            attrs,
            Map.of(),
            Map.of(),
            null,
            null,
            fileList1
        );
        ExternalSourceExec exec2 = new ExternalSourceExec(
            Source.EMPTY,
            "s3://bucket/data.parquet",
            "parquet",
            attrs,
            Map.of(),
            Map.of(),
            null,
            null,
            fileList1
        );
        ExternalSourceExec exec3 = new ExternalSourceExec(
            Source.EMPTY,
            "s3://bucket/data.parquet",
            "parquet",
            attrs,
            Map.of(),
            Map.of(),
            null,
            null,
            fileList2
        );

        assertEquals(exec1, exec2);
        assertEquals(exec1.hashCode(), exec2.hashCode());
        assertNotEquals(exec1, exec3);
    }

    // ===== Helpers =====

    private static Attribute attr(String name, DataType type) {
        return new FieldAttribute(Source.EMPTY, name, new EsField(name, type, Map.of(), false, EsField.TimeSeriesFieldType.NONE));
    }

    private static List<Attribute> createAttributes() {
        return List.of(attr("id", DataType.LONG), attr("name", DataType.KEYWORD));
    }

    private static FileList createFileList() {
        return GlobExpander.fileListOf(
            List.of(
                new StorageEntry(StoragePath.of("s3://bucket/data/f1.parquet"), 100, Instant.EPOCH),
                new StorageEntry(StoragePath.of("s3://bucket/data/f2.parquet"), 200, Instant.EPOCH)
            ),
            "s3://bucket/data/*.parquet"
        );
    }

    private static SourceMetadata createMetadata() {
        return new SourceMetadata() {
            @Override
            public List<Attribute> schema() {
                return createAttributes();
            }

            @Override
            public String sourceType() {
                return "parquet";
            }

            @Override
            public String location() {
                return "s3://bucket/data.parquet";
            }

            @Override
            public boolean equals(Object o) {
                return o instanceof SourceMetadata;
            }

            @Override
            public int hashCode() {
                return 1;
            }
        };
    }

    private static ExternalRelation createRelation(FileList fileList) {
        return new ExternalRelation(Source.EMPTY, "s3://bucket/data.parquet", createMetadata(), createAttributes(), fileList);
    }

    private static ExternalSourceExec createExec(FileList fileList) {
        return new ExternalSourceExec(
            Source.EMPTY,
            "s3://bucket/data.parquet",
            "parquet",
            createAttributes(),
            Map.of(),
            Map.of(),
            null,
            null,
            fileList
        );
    }
}
