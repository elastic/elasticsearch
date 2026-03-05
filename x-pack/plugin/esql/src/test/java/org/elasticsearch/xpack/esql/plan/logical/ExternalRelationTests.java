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
import org.elasticsearch.xpack.esql.datasources.FileSet;
import org.elasticsearch.xpack.esql.datasources.StorageEntry;
import org.elasticsearch.xpack.esql.datasources.spi.SourceMetadata;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.plan.physical.ExternalSourceExec;

import java.time.Instant;
import java.util.List;
import java.util.Map;

/**
 * Tests for ExternalRelation and ExternalSourceExec FileSet threading.
 * Verifies that FileSet is correctly threaded through constructors, toPhysicalExec(),
 * withPushedFilter(), withEstimatedRowSize(), equals/hashCode, and info().
 */
public class ExternalRelationTests extends ESTestCase {

    // ===== ExternalRelation tests =====

    public void testConstructorWithFileSet() {
        FileSet fileSet = createFileSet();
        ExternalRelation relation = createRelation(fileSet);

        assertSame(fileSet, relation.fileSet());
        assertTrue(relation.fileSet().isResolved());
        assertEquals(2, relation.fileSet().size());
    }

    public void testConstructorWithoutFileSetDefaultsToUnresolved() {
        SourceMetadata metadata = createMetadata();
        List<Attribute> output = createAttributes();

        ExternalRelation relation = new ExternalRelation(Source.EMPTY, "s3://bucket/data.parquet", metadata, output);

        assertSame(FileSet.UNRESOLVED, relation.fileSet());
        assertTrue(relation.fileSet().isUnresolved());
    }

    public void testToPhysicalExecThreadsFileSet() {
        FileSet fileSet = createFileSet();
        ExternalRelation relation = createRelation(fileSet);

        ExternalSourceExec exec = relation.toPhysicalExec();

        assertNotNull(exec);
        assertSame(fileSet, exec.fileSet());
        assertTrue(exec.fileSet().isResolved());
        assertEquals(2, exec.fileSet().size());
    }

    public void testWithAttributesPreservesFileSet() {
        FileSet fileSet = createFileSet();
        ExternalRelation relation = createRelation(fileSet);

        List<Attribute> newAttrs = List.of(attr("new_col", DataType.LONG));
        ExternalRelation updated = relation.withAttributes(newAttrs);

        assertSame(fileSet, updated.fileSet());
        assertEquals(1, updated.output().size());
        assertEquals("new_col", updated.output().get(0).name());
    }

    public void testEqualsAndHashCodeIncludeFileSet() {
        FileSet fileSet1 = createFileSet();
        FileSet fileSet2 = new FileSet(
            List.of(new StorageEntry(StoragePath.of("s3://bucket/data/other.parquet"), 999, Instant.EPOCH)),
            "s3://bucket/data/other*.parquet"
        );

        SourceMetadata metadata = createMetadata();
        List<Attribute> output = createAttributes();

        ExternalRelation relation1 = new ExternalRelation(Source.EMPTY, "s3://bucket/data.parquet", metadata, output, fileSet1);
        ExternalRelation relation2 = new ExternalRelation(Source.EMPTY, "s3://bucket/data.parquet", metadata, output, fileSet1);
        ExternalRelation relation3 = new ExternalRelation(Source.EMPTY, "s3://bucket/data.parquet", metadata, output, fileSet2);

        assertEquals(relation1, relation2);
        assertEquals(relation1.hashCode(), relation2.hashCode());
        assertNotEquals(relation1, relation3);
    }

    public void testInfoRoundTripsFileSet() {
        FileSet fileSet = createFileSet();
        ExternalRelation relation = createRelation(fileSet);

        var info = relation.info();
        assertNotNull(info);
        assertSame(fileSet, relation.fileSet());
    }

    // ===== ExternalSourceExec FileSet threading tests =====

    public void testExecWithPushedFilterPreservesFileSet() {
        FileSet fileSet = createFileSet();
        ExternalSourceExec exec = createExec(fileSet);

        ExternalSourceExec filtered = exec.withPushedFilter("some_filter_object");

        assertSame(fileSet, filtered.fileSet());
        assertEquals("some_filter_object", filtered.pushedFilter());
    }

    public void testExecConstructorWithEstimatedRowSizePreservesFileSet() {
        FileSet fileSet = createFileSet();
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
            fileSet
        );

        assertSame(fileSet, exec.fileSet());
        assertEquals(Integer.valueOf(256), exec.estimatedRowSize());
    }

    public void testExecWithoutFileSetHasNullFileSet() {
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

        assertNull(exec.fileSet());

        FileSet fileSet = createFileSet();
        ExternalSourceExec execWithFileSet = new ExternalSourceExec(
            Source.EMPTY,
            "s3://bucket/data.parquet",
            "parquet",
            attrs,
            Map.of(),
            Map.of(),
            null,
            null,
            fileSet
        );

        assertSame(fileSet, execWithFileSet.fileSet());
    }

    public void testExecEqualsAndHashCodeIncludeFileSet() {
        FileSet fileSet1 = createFileSet();
        FileSet fileSet2 = new FileSet(
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
            fileSet1
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
            fileSet1
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
            fileSet2
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

    private static FileSet createFileSet() {
        return new FileSet(
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

    private static ExternalRelation createRelation(FileSet fileSet) {
        return new ExternalRelation(Source.EMPTY, "s3://bucket/data.parquet", createMetadata(), createAttributes(), fileSet);
    }

    private static ExternalSourceExec createExec(FileSet fileSet) {
        return new ExternalSourceExec(
            Source.EMPTY,
            "s3://bucket/data.parquet",
            "parquet",
            createAttributes(),
            Map.of(),
            Map.of(),
            null,
            null,
            fileSet
        );
    }
}
