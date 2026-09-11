/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.CloseableIterator;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.ExternalSchema;
import org.elasticsearch.xpack.esql.datasources.FormatReaderRegistry;
import org.elasticsearch.xpack.esql.datasources.SchemaReconciliation;
import org.elasticsearch.xpack.esql.datasources.SourceStatisticsSerializer;
import org.elasticsearch.xpack.esql.datasources.spi.AggregatePushdownSupport;
import org.elasticsearch.xpack.esql.datasources.spi.FileList;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReadContext;
import org.elasticsearch.xpack.esql.datasources.spi.NoConfigFormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.PassThroughRowPositionStrategy;
import org.elasticsearch.xpack.esql.datasources.spi.RowPositionStrategy;
import org.elasticsearch.xpack.esql.datasources.spi.SimpleSourceMetadata;
import org.elasticsearch.xpack.esql.datasources.spi.SkipWarnings;
import org.elasticsearch.xpack.esql.datasources.spi.SourceMetadata;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Count;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.ExternalRelation;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.physical.FragmentExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.alias;

/**
 * Coordinator warm-gate notices and the {@code Aggregate -> Project -> ExternalRelation} skip pattern.
 */
public class ComputeServiceWarmDiscardNoticesTests extends ESTestCase {

    private static final StoragePath ANCHOR_PATH = StoragePath.of("file:///part-a.parquet");
    private static final StoragePath DRIFT_PATH = StoragePath.of("file:///part-b.parquet");

    public void testWarmDiscardNoticesCountFieldEmitsSummaryAndDetail() {
        ReferenceAttribute x = attr("x", DataType.INTEGER);
        ExternalRelation ext = relation(schemaMap(Map.of("x", DataType.INTEGER), Map.of("x", DataType.LONG)), List.of(x));
        List<String> notices = ComputeService.warmDiscardNotices(countAgg(ext, x), ext, Set.of());
        assertEquals(
            List.of(
                SkipWarnings.incompatiblePlannerTypeFileSummary("parquet", DRIFT_PATH.toString()),
                SkipWarnings.incompatiblePlannerTypeColumnMessage("x", DRIFT_PATH.toString(), DataType.LONG, DataType.INTEGER)
            ),
            notices
        );
    }

    public void testWarmDiscardNoticesCountStarIsSilent() {
        ReferenceAttribute x = attr("x", DataType.INTEGER);
        ExternalRelation ext = relation(schemaMap(Map.of("x", DataType.INTEGER), Map.of("x", DataType.LONG)), List.of(x));
        assertEquals(List.of(), ComputeService.warmDiscardNotices(countStarAgg(ext), ext, Set.of()));
    }

    public void testWarmDiscardNoticesUnreadDriftingColumnIsSilent() {
        ReferenceAttribute x = attr("x", DataType.INTEGER);
        ReferenceAttribute y = attr("y", DataType.INTEGER);
        Map<StoragePath, SchemaReconciliation.FileSchemaInfo> schemaMap = schemaMap(
            Map.of("x", DataType.INTEGER, "y", DataType.INTEGER),
            Map.of("x", DataType.LONG, "y", DataType.INTEGER)
        );
        ExternalRelation ext = relation(schemaMap, List.of(x, y));
        assertEquals(List.of(), ComputeService.warmDiscardNotices(countAgg(ext, y), ext, Set.of()));
    }

    public void testWarmDiscardNoticesWideningFileIsSilent() {
        ReferenceAttribute x = attr("x", DataType.LONG);
        ExternalRelation ext = relation(schemaMap(Map.of("x", DataType.LONG), Map.of("x", DataType.INTEGER)), List.of(x));
        assertEquals(List.of(), ComputeService.warmDiscardNotices(countAgg(ext, x), ext, Set.of()));
    }

    public void testWarmDiscardNoticesDeclaredCoercibleIsSilent() {
        ReferenceAttribute x = attr("x", DataType.INTEGER);
        ExternalRelation ext = relation(schemaMap(Map.of("x", DataType.INTEGER), Map.of("x", DataType.LONG)), List.of(x));
        assertEquals(List.of(), ComputeService.warmDiscardNotices(countAgg(ext, x), ext, Set.of("x")));
    }

    public void testWarmDiscardNoticesTwoDriftingColumnsEmitsOnlyTheConsumedOne() {
        ReferenceAttribute x = attr("x", DataType.INTEGER);
        ReferenceAttribute y = attr("y", DataType.INTEGER);
        Map<StoragePath, SchemaReconciliation.FileSchemaInfo> schemaMap = schemaMap(
            Map.of("x", DataType.INTEGER, "y", DataType.INTEGER),
            Map.of("x", DataType.LONG, "y", DataType.LONG)
        );
        ExternalRelation ext = relation(schemaMap, List.of(x, y));
        List<String> notices = ComputeService.warmDiscardNotices(countAgg(ext, x), ext, Set.of());
        assertEquals(
            List.of(
                SkipWarnings.incompatiblePlannerTypeFileSummary("parquet", DRIFT_PATH.toString()),
                SkipWarnings.incompatiblePlannerTypeColumnMessage("x", DRIFT_PATH.toString(), DataType.LONG, DataType.INTEGER)
            ),
            notices
        );
        assertFalse(notices.toString().contains("Column [y]"));
    }

    public void testCanSkipSplitDiscoverySeesThroughBareProject() {
        ReferenceAttribute x = attr("x", DataType.INTEGER);
        ExternalRelation ext = relation(schemaMap(Map.of("x", DataType.INTEGER), Map.of("x", DataType.LONG)), List.of(x));
        Aggregate direct = countAgg(ext, x);
        Aggregate projected = new Aggregate(Source.EMPTY, new Project(Source.EMPTY, ext, List.of(x)), List.of(), List.of(countAlias(x)));
        FormatReaderRegistry registry = parquetRegistry();
        PhysicalPlan withoutProject = new FragmentExec(direct);
        PhysicalPlan withProject = new FragmentExec(projected);
        assertEquals(
            ComputeService.canSkipSplitDiscovery(withoutProject, registry),
            ComputeService.canSkipSplitDiscovery(withProject, registry)
        );
        assertTrue(ComputeService.canSkipSplitDiscovery(withProject, registry));
    }

    private static Alias countAlias(Attribute field) {
        return alias("c", new Count(Source.EMPTY, field));
    }

    private static Aggregate countAgg(ExternalRelation ext, Attribute field) {
        return new Aggregate(Source.EMPTY, ext, List.of(), List.of(countAlias(field)));
    }

    private static Aggregate countStarAgg(ExternalRelation ext) {
        return new Aggregate(
            Source.EMPTY,
            ext,
            List.of(),
            List.of(alias("c", new Count(Source.EMPTY, Literal.keyword(Source.EMPTY, "*"))))
        );
    }

    private static ExternalRelation relation(Map<StoragePath, SchemaReconciliation.FileSchemaInfo> schemaMap, List<Attribute> output) {
        Map<String, Object> sourceMeta = new HashMap<>();
        sourceMeta.put(SourceStatisticsSerializer.STATS_ROW_COUNT, 4L);
        sourceMeta.put(SourceStatisticsSerializer.columnValueCountKey("x"), 2L);
        sourceMeta.put(SourceStatisticsSerializer.columnNullCountKey("x"), 2L);
        SourceMetadata metadata = new SimpleSourceMetadata(output, "parquet", "file:///glob/*.parquet", null, null, sourceMeta, Map.of());
        return new ExternalRelation(Source.EMPTY, "file:///glob/*.parquet", metadata, output, FileList.UNRESOLVED, schemaMap);
    }

    private static Map<StoragePath, SchemaReconciliation.FileSchemaInfo> schemaMap(
        Map<String, DataType> anchorTypes,
        Map<String, DataType> driftTypes
    ) {
        List<Attribute> anchorAttrs = anchorTypes.entrySet().stream().map(e -> (Attribute) attr(e.getKey(), e.getValue())).toList();
        ExternalSchema anchor = new ExternalSchema(anchorAttrs);
        Map<StoragePath, SchemaReconciliation.FileSchemaInfo> schemaMap = new LinkedHashMap<>();
        schemaMap.put(ANCHOR_PATH, new SchemaReconciliation.FileSchemaInfo(anchor, null, null, Map.copyOf(anchorTypes)));
        schemaMap.put(DRIFT_PATH, new SchemaReconciliation.FileSchemaInfo(anchor, null, null, Map.copyOf(driftTypes)));
        return schemaMap;
    }

    private static ReferenceAttribute attr(String name, DataType type) {
        return new ReferenceAttribute(Source.EMPTY, name, type);
    }

    private static FormatReaderRegistry parquetRegistry() {
        FormatReaderRegistry registry = new FormatReaderRegistry(null);
        AggregatePushdownSupport support = (aggregates, groupings) -> {
            if (groupings.isEmpty() == false) {
                return AggregatePushdownSupport.Pushability.NO;
            }
            for (var agg : aggregates) {
                if (agg instanceof Count) {
                    continue;
                }
                return AggregatePushdownSupport.Pushability.NO;
            }
            return AggregatePushdownSupport.Pushability.YES;
        };
        registry.registerLazy("parquet", (settings, blockFactory) -> new StubParquetReader(support), null, null);
        return registry;
    }

    private static final class StubParquetReader implements NoConfigFormatReader {
        private final AggregatePushdownSupport support;

        StubParquetReader(AggregatePushdownSupport support) {
            this.support = support;
        }

        @Override
        public RowPositionStrategy rowPositionStrategy() {
            return PassThroughRowPositionStrategy.INSTANCE;
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
        public AggregatePushdownSupport aggregatePushdownSupport() {
            return support;
        }

        @Override
        public void close() {}
    }
}
