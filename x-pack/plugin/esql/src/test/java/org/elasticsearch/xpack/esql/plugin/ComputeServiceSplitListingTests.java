/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.datasources.ExternalSchema;
import org.elasticsearch.xpack.esql.datasources.FileSplit;
import org.elasticsearch.xpack.esql.datasources.OperatorFactoryRegistry;
import org.elasticsearch.xpack.esql.datasources.SchemaReconciliation;
import org.elasticsearch.xpack.esql.datasources.StorageEntry;
import org.elasticsearch.xpack.esql.datasources.glob.GlobExpander;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalSourceFactory;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalSplit;
import org.elasticsearch.xpack.esql.datasources.spi.FileList;
import org.elasticsearch.xpack.esql.datasources.spi.SegmentableFormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.SimpleSourceMetadata;
import org.elasticsearch.xpack.esql.datasources.spi.SourceMetadata;
import org.elasticsearch.xpack.esql.datasources.spi.SplitDiscoveryResult;
import org.elasticsearch.xpack.esql.datasources.spi.SplitProvider;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.plan.logical.ExternalRelation;
import org.elasticsearch.xpack.esql.plan.physical.ExternalSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.FragmentExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.core.tree.Source.EMPTY;

public class ComputeServiceSplitListingTests extends ESTestCase {

    public void testCopiedSplitsDropListingState() {
        StoragePath path = StoragePath.of("s3://bucket/data/a.parquet");
        FileList fileList = GlobExpander.fileListOf(List.of(new StorageEntry(path, 100, Instant.EPOCH)), "s3://bucket/data/*.parquet");
        Map<StoragePath, SchemaReconciliation.FileSchemaInfo> schemaMap = schemaMap(path);
        ExternalRelation relation = relation(fileList, schemaMap);
        FileSplit split = new FileSplit("parquet", path, 0, 100, "parquet", Map.of(), Map.of("year", 2024));
        List<ExternalSplit> collected = new ArrayList<>();

        PhysicalPlan rewritten = discover(new FragmentExec(relation), collected, splitResult(List.of(split), false));

        FragmentExec fragment = (FragmentExec) rewritten;
        ExternalRelation dropped = (ExternalRelation) fragment.fragment();
        assertSame(FileList.EMPTY, dropped.fileList());
        assertTrue(dropped.schemaMap().isEmpty());
        assertEquals(List.of(split), collected);
    }

    public void testExhaustivePruneClearsFileListAndSchemaMap() {
        StoragePath path = StoragePath.of("s3://bucket/data/a.parquet");
        FileList fileList = GlobExpander.fileListOf(List.of(new StorageEntry(path, 100, Instant.EPOCH)), "s3://bucket/data/*.parquet");
        Map<StoragePath, SchemaReconciliation.FileSchemaInfo> schemaMap = schemaMap(path);
        ExternalRelation relation = relation(fileList, schemaMap);
        List<ExternalSplit> collected = new ArrayList<>();

        PhysicalPlan rewritten = discover(new FragmentExec(relation), collected, new SplitDiscoveryResult(List.of(), 0, true, 0L));

        FragmentExec fragment = (FragmentExec) rewritten;
        ExternalRelation pruned = (ExternalRelation) fragment.fragment();
        assertSame(FileList.EMPTY, pruned.fileList());
        assertTrue(pruned.schemaMap().isEmpty());
        assertTrue(collected.isEmpty());
    }

    public void testNonExhaustiveEmptyResultKeepsFileList() {
        StoragePath path = StoragePath.of("s3://bucket/data/a.parquet");
        FileList fileList = GlobExpander.fileListOf(List.of(new StorageEntry(path, 100, Instant.EPOCH)), "s3://bucket/data/*.parquet");
        Map<StoragePath, SchemaReconciliation.FileSchemaInfo> schemaMap = schemaMap(path);
        ExternalRelation relation = relation(fileList, schemaMap);
        FragmentExec plan = new FragmentExec(relation);
        List<ExternalSplit> collected = new ArrayList<>();

        PhysicalPlan rewritten = discover(plan, collected, SplitDiscoveryResult.EMPTY);

        assertSame(plan, rewritten);
        assertSame(fileList, relation.fileList());
        assertSame(schemaMap, relation.schemaMap());
        assertTrue(collected.isEmpty());
    }

    public void testTopLevelExecDropsListingButKeepsSplits() {
        StoragePath path = StoragePath.of("s3://bucket/data/a.parquet");
        FileList fileList = GlobExpander.fileListOf(List.of(new StorageEntry(path, 100, Instant.EPOCH)), "s3://bucket/data/*.parquet");
        FileSplit split = new FileSplit("parquet", path, 0, 100, "parquet", Map.of(), Map.of());
        ExternalSourceExec exec = new ExternalSourceExec(
            EMPTY,
            "s3://bucket/data/*.parquet",
            "parquet",
            List.of(attr()),
            Map.of(),
            Map.of(),
            null,
            null
        ).withFileList(fileList).withSchemaMap(schemaMap(path)).withSplits(List.of(split));

        ExternalSourceExec dropped = (ExternalSourceExec) ComputeService.dropCopiedListingState(exec);
        assertSame(FileList.EMPTY, dropped.fileList());
        assertTrue(dropped.schemaMap().isEmpty());
        assertEquals(List.of(split), dropped.splits());

        ExternalSourceExec noSplits = exec.withSplits(List.of());
        assertSame(noSplits, ComputeService.dropCopiedListingState(noSplits));
    }

    private static PhysicalPlan discover(PhysicalPlan plan, List<ExternalSplit> collected, SplitDiscoveryResult result) {
        OperatorFactoryRegistry registry = new OperatorFactoryRegistry(
            Map.of("parquet", factory(result)),
            Map.of(),
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        return ComputeService.discoverSplitsFromFragments(
            plan,
            collected,
            SegmentableFormatReader.DEFAULT_MAX_RECORD_BYTES,
            null,
            () -> false,
            registry
        );
    }

    private static SplitDiscoveryResult splitResult(List<ExternalSplit> splits, boolean exhaustivelyPruned) {
        return new SplitDiscoveryResult(splits, splits.size(), exhaustivelyPruned, 0L);
    }

    private static ExternalRelation relation(FileList fileList, Map<StoragePath, SchemaReconciliation.FileSchemaInfo> schemaMap) {
        List<Attribute> output = List.of(attr());
        SourceMetadata metadata = new SimpleSourceMetadata(output, "parquet", "s3://bucket/data/*.parquet", null, null, Map.of(), Map.of());
        return new ExternalRelation(EMPTY, "s3://bucket/data/*.parquet", metadata, output, fileList, schemaMap);
    }

    private static Map<StoragePath, SchemaReconciliation.FileSchemaInfo> schemaMap(StoragePath path) {
        return Map.of(path, new SchemaReconciliation.FileSchemaInfo(new ExternalSchema(List.of(attr())), null, null));
    }

    private static Attribute attr() {
        return new FieldAttribute(Source.EMPTY, "id", new EsField("id", DataType.LONG, Map.of(), false, EsField.TimeSeriesFieldType.NONE));
    }

    private static ExternalSourceFactory factory(SplitDiscoveryResult result) {
        return new ExternalSourceFactory() {
            @Override
            public void validateConfig(String location, Map<String, Object> config) {
                throw new UnsupportedOperationException("test stub does not implement validation");
            }

            @Override
            public String type() {
                return "test";
            }

            @Override
            public boolean canHandle(String location) {
                return true;
            }

            @Override
            public SourceMetadata resolveMetadata(String location, Map<String, Object> config) {
                return null;
            }

            @Override
            public SplitProvider splitProvider() {
                return context -> result;
            }
        };
    }
}
