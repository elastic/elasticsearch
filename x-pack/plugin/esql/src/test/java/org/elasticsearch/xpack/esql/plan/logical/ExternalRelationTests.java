/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.datasources.SourceStatisticsSerializer;
import org.elasticsearch.xpack.esql.datasources.StorageEntry;
import org.elasticsearch.xpack.esql.datasources.glob.GlobExpander;
import org.elasticsearch.xpack.esql.datasources.spi.FileList;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.SimpleSourceMetadata;
import org.elasticsearch.xpack.esql.datasources.spi.SourceMetadata;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.plan.physical.ExternalSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.FragmentExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.planner.mapper.Mapper;
import org.elasticsearch.xpack.esql.session.Versioned;

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

        ExternalRelation relation = new ExternalRelation(
            Source.EMPTY,
            "s3://bucket/data.parquet",
            metadata,
            output,
            FileList.UNRESOLVED,
            Map.of()
        );

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

    /**
     * Pins the logical -> physical lowering shape: {@code toPhysicalExec()} carries the relation's
     * build-time state (fileList, datasetName, the data-only unified schema) onto the exec, while
     * every local-execution pushdown hint starts at its neutral value. Pushdowns are produced later,
     * per-node, by {@code LocalPhysicalPlanOptimizer} rules, so they must not be set at lowering time.
     */
    public void testToPhysicalExecLoweringShape() {
        FileList fileList = createFileList();
        ExternalRelation relation = new ExternalRelation(
            Source.EMPTY,
            "s3://bucket/data.parquet",
            createMetadata(),
            createAttributes(),
            fileList,
            Map.of(),
            "my_dataset"
        );

        ExternalSourceExec exec = relation.toPhysicalExec();

        // Build-time state copied from the relation.
        assertSame(fileList, exec.fileList());
        assertEquals("my_dataset", exec.datasetName());
        assertNotNull("unifiedSchema seeded from the relation", exec.unifiedSchema());
        assertEquals(relation.output(), exec.output());
        assertEquals(relation.sourceType(), exec.sourceType());

        // No splits yet — split discovery is the later bridge that attaches them.
        assertTrue("splits attached only by split discovery", exec.splits().isEmpty());

        // Local-execution pushdown hints all at their neutral values.
        assertNull("no pushed filter at lowering", exec.pushedFilter());
        assertTrue("no pushed expressions at lowering", exec.pushedExpressions().isEmpty());
        assertEquals("no pushed limit at lowering", FormatReader.NO_LIMIT, exec.pushedLimit());
        assertNull("no pushed top-n at lowering", exec.pushedTopN());
        assertFalse("no deferred extraction at lowering", exec.deferredExtraction());
    }

    /**
     * The coordinator {@link Mapper} never lowers an {@link ExternalRelation} to an
     * {@link ExternalSourceExec}; it wraps it in a {@link FragmentExec} so the data-node-bound
     * subtree carries the still-logical relation. The exec only appears later, per-node, during
     * {@code PlannerUtils.localPlan()}. This guards that layering invariant.
     */
    public void testCoordinatorMapperKeepsRelationLogical() {
        ExternalRelation relation = createRelation(createFileList());

        PhysicalPlan mapped = new Mapper().map(new Versioned<>(relation, TransportVersion.current()));

        assertTrue("relation is wrapped in a FragmentExec", mapped.anyMatch(FragmentExec.class::isInstance));
        assertFalse("coordinator-side plan must not contain ExternalSourceExec", mapped.anyMatch(ExternalSourceExec.class::isInstance));
        FragmentExec fragment = (FragmentExec) mapped;
        assertTrue("fragment still holds the logical ExternalRelation", fragment.fragment() instanceof ExternalRelation);
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

        ExternalRelation relation1 = new ExternalRelation(Source.EMPTY, "s3://bucket/data.parquet", metadata, output, fileList1, Map.of());
        ExternalRelation relation2 = new ExternalRelation(Source.EMPTY, "s3://bucket/data.parquet", metadata, output, fileList1, Map.of());
        ExternalRelation relation3 = new ExternalRelation(Source.EMPTY, "s3://bucket/data.parquet", metadata, output, fileList2, Map.of());

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
            256
        ).withFileList(fileList);

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
            null
        ).withFileList(fileList);

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
            null
        ).withFileList(fileList1);
        ExternalSourceExec exec2 = new ExternalSourceExec(
            Source.EMPTY,
            "s3://bucket/data.parquet",
            "parquet",
            attrs,
            Map.of(),
            Map.of(),
            null,
            null
        ).withFileList(fileList1);
        ExternalSourceExec exec3 = new ExternalSourceExec(
            Source.EMPTY,
            "s3://bucket/data.parquet",
            "parquet",
            attrs,
            Map.of(),
            Map.of(),
            null,
            null
        ).withFileList(fileList2);

        assertEquals(exec1, exec2);
        assertEquals(exec1.hashCode(), exec2.hashCode());
        assertNotEquals(exec1, exec3);
    }

    // ===== Secret redaction in nodeProperties() / toString() =====

    public void testNodePropertiesOmitsSecretsSecureStringPath() {
        // Dataset path: SecureString values in config.
        var config = Map.<String, Object>of(
            "access_key",
            "AKID",
            "secret_key",
            new SecureString("S3CR3T_DO_NOT_LEAK_SecureString".toCharArray())
        );
        SimpleSourceMetadata secretMetadata = new SimpleSourceMetadata(
            createAttributes(),
            "parquet",
            "s3://bucket/data.parquet",
            null,
            null,
            null,
            config
        );
        ExternalRelation relation = new ExternalRelation(
            Source.EMPTY,
            "s3://bucket/data.parquet",
            secretMetadata,
            createAttributes(),
            FileList.UNRESOLVED,
            Map.of()
        );

        assertFalse("nodeProperties() must not contain the metadata object", relation.nodeProperties().contains(secretMetadata));
        String rendered = relation.nodeString() + " " + relation.toString();
        assertFalse("EXPLAIN output must not contain the secret value", rendered.contains("S3CR3T_DO_NOT_LEAK_SecureString"));
    }

    public void testNodePropertiesOmitsSecretsPlainStringPath() {
        // Inline EXTERNAL path: plain String values in config (foldOptionLiterals produces plain strings).
        var config = Map.<String, Object>of("secret_key", "PLAINTEXT_DO_NOT_LEAK_String");
        SimpleSourceMetadata secretMetadata = new SimpleSourceMetadata(
            createAttributes(),
            "parquet",
            "s3://bucket/data.parquet",
            null,
            null,
            null,
            config
        );
        ExternalRelation relation = new ExternalRelation(
            Source.EMPTY,
            "s3://bucket/data.parquet",
            secretMetadata,
            createAttributes(),
            FileList.UNRESOLVED,
            Map.of()
        );

        assertFalse("nodeProperties() must not contain the metadata object", relation.nodeProperties().contains(secretMetadata));
        String rendered = relation.nodeString() + " " + relation.toString();
        assertFalse("EXPLAIN output must not contain the secret value", rendered.contains("PLAINTEXT_DO_NOT_LEAK_String"));
    }

    // ===== Partition-column strip in toPhysicalExec → withUnifiedSchema =====

    public void testToPhysicalExecSeedsUnifiedFromDataOnlyWhenNoPartitions() {
        // No partitions → unifiedSchema seeded from metadata.schema() unchanged (by name).
        FileList fileList = createFileList();
        ExternalRelation relation = createRelation(fileList);

        ExternalSourceExec exec = relation.toPhysicalExec();

        assertNotNull("unifiedSchema is seeded", exec.unifiedSchema());
        assertEquals(
            "no partition stripping; matches metadata.schema() column count",
            relation.metadata().schema().size(),
            exec.unifiedSchema().size()
        );
        assertEquals(
            "no partition stripping; column names match",
            relation.metadata().schema().stream().map(a -> a.name()).toList(),
            exec.unifiedSchema().attributes().stream().map(a -> a.name()).toList()
        );
    }

    public void testToPhysicalExecStripsPartitionAttributesFromUnified() {
        // Partitioned dataset: metadata.schema() carries appended partition attrs, but
        // ColumnMapping.index was sized against the data-only unified. toPhysicalExec must seed
        // unifiedSchema from the data-only view.
        Attribute id = attr("id", DataType.LONG);
        Attribute name = attr("name", DataType.KEYWORD);
        Attribute year = attr("year", DataType.INTEGER); // partition column

        // Coordinator shape: partition identity comes from the serialized stamp (written at resolution
        // alongside the resolved fileList), which is what dataOnlyUnifiedSchema now reads.
        SourceMetadata partitionedMetadata = createMetadataWithSchema(
            List.of(id, name, year),
            Map.of(SourceStatisticsSerializer.PARTITION_COLUMNS_KEY, List.of("year"))
        );
        FileList partitionedFiles = createPartitionedFileList(Map.of("year", DataType.INTEGER));
        ExternalRelation relation = new ExternalRelation(
            Source.EMPTY,
            "s3://bucket/year=*/*.parquet",
            partitionedMetadata,
            List.of(id, name, year),
            partitionedFiles,
            Map.of()
        );

        ExternalSourceExec exec = relation.toPhysicalExec();

        assertNotNull("unifiedSchema is seeded", exec.unifiedSchema());
        assertEquals("partition attr stripped", 2, exec.unifiedSchema().size());
        assertFalse("partition column not present in seeded unified", exec.unifiedSchema().names().contains("year"));
        assertTrue("data columns preserved", exec.unifiedSchema().names().contains("id"));
        assertTrue("data columns preserved", exec.unifiedSchema().names().contains("name"));
    }

    public void testToPhysicalExecStripsPartitionAttributesOnDataNodeShape() {
        // Data-node shape: the relation deserializes with FileList.UNRESOLVED, so the OLD fileList-based
        // strip kept the wider, partition-inclusive unifiedSchema here (the P6 latent inconsistency). Reading
        // the serialized stamp strips partition attributes identically to the coordinator.
        Attribute id = attr("id", DataType.LONG);
        Attribute name = attr("name", DataType.KEYWORD);
        Attribute year = attr("year", DataType.INTEGER); // partition column

        SourceMetadata partitionedMetadata = createMetadataWithSchema(
            List.of(id, name, year),
            Map.of(SourceStatisticsSerializer.PARTITION_COLUMNS_KEY, List.of("year"))
        );
        ExternalRelation relation = new ExternalRelation(
            Source.EMPTY,
            "s3://bucket/year=*/*.parquet",
            partitionedMetadata,
            List.of(id, name, year),
            FileList.UNRESOLVED,
            Map.of()
        );

        ExternalSourceExec exec = relation.toPhysicalExec();

        assertEquals("partition attr stripped from the stamp even with an UNRESOLVED fileList", 2, exec.unifiedSchema().size());
        assertFalse("partition column not present in seeded unified", exec.unifiedSchema().names().contains("year"));
    }

    private static SourceMetadata createMetadataWithSchema(List<Attribute> schema) {
        return createMetadataWithSchema(schema, Map.of());
    }

    private static SourceMetadata createMetadataWithSchema(List<Attribute> schema, Map<String, Object> sourceMetadata) {
        return new SourceMetadata() {
            @Override
            public List<Attribute> schema() {
                return schema;
            }

            @Override
            public String sourceType() {
                return "parquet";
            }

            @Override
            public String location() {
                return "s3://bucket/data/*.parquet";
            }

            @Override
            public Map<String, Object> sourceMetadata() {
                return sourceMetadata;
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

    private static FileList createPartitionedFileList(Map<String, DataType> partitionColumns) {
        StoragePath path = StoragePath.of("s3://bucket/year=2024/data.parquet");
        return GlobExpander.fileListOf(
            List.of(new StorageEntry(path, 100, Instant.EPOCH)),
            "s3://bucket/year=*/*.parquet",
            new org.elasticsearch.xpack.esql.datasources.PartitionMetadata(partitionColumns, Map.of(path, Map.of("year", 2024)))
        );
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
        return new ExternalRelation(Source.EMPTY, "s3://bucket/data.parquet", createMetadata(), createAttributes(), fileList, Map.of());
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
            null
        ).withFileList(fileList);
    }
}
