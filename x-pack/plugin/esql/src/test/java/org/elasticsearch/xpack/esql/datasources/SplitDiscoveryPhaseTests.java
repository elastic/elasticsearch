/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalSourceFactory;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalSplit;
import org.elasticsearch.xpack.esql.datasources.spi.SourceMetadata;
import org.elasticsearch.xpack.esql.datasources.spi.SplitDiscoveryContext;
import org.elasticsearch.xpack.esql.datasources.spi.SplitProvider;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.esql.plan.physical.ExternalSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.FilterExec;
import org.elasticsearch.xpack.esql.plan.physical.LimitExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class SplitDiscoveryPhaseTests extends ESTestCase {

    private static final Source SRC = Source.EMPTY;

    public void testExternalSourceExecGetsSplitsAttached() {
        FileSet fileSet = createFileSet(3);
        ExternalSourceExec exec = createExternalSourceExec(fileSet, "parquet");

        Map<String, ExternalSourceFactory> factories = Map.of("parquet", testFactory(new FileSplitProvider()));

        PhysicalPlan result = SplitDiscoveryPhase.resolveExternalSplits(exec, factories);

        assertTrue(result instanceof ExternalSourceExec);
        ExternalSourceExec resolved = (ExternalSourceExec) result;
        assertEquals(3, resolved.splits().size());
        for (ExternalSplit split : resolved.splits()) {
            assertTrue(split instanceof FileSplit);
        }
    }

    public void testNoExternalSourceUnchanged() {
        PhysicalPlan leaf = createExternalSourceExec(FileSet.UNRESOLVED, "parquet");
        LimitExec limit = new LimitExec(SRC, leaf, new Literal(SRC, 10, DataType.INTEGER), null);

        Map<String, ExternalSourceFactory> factories = Map.of("parquet", testFactory(new FileSplitProvider()));

        PhysicalPlan result = SplitDiscoveryPhase.resolveExternalSplits(limit, factories);

        assertTrue(result instanceof LimitExec);
        ExternalSourceExec child = (ExternalSourceExec) ((LimitExec) result).child();
        assertTrue(child.splits().isEmpty());
    }

    public void testFilterExecAboveExternalSourceCollectsFilters() {
        FileSet fileSet = createFileSet(2);
        ExternalSourceExec exec = createExternalSourceExec(fileSet, "parquet");
        Expression condition = new Equals(SRC, fieldAttr("year", DataType.INTEGER), new Literal(SRC, 2024, DataType.INTEGER));
        FilterExec filter = new FilterExec(SRC, exec, condition);

        RecordingSplitProvider recorder = new RecordingSplitProvider();
        Map<String, ExternalSourceFactory> factories = Map.of("parquet", testFactory(recorder));

        SplitDiscoveryPhase.resolveExternalSplits(filter, factories);

        assertEquals(1, recorder.lastContext.filterHints().size());
    }

    public void testMultipleExternalSourcesEachGetOwnSplits() {
        FileSet fileSet1 = createFileSet(2);
        FileSet fileSet2 = createFileSet(3);
        ExternalSourceExec exec1 = createExternalSourceExec(fileSet1, "parquet");
        ExternalSourceExec exec2 = createExternalSourceExec(fileSet2, "csv");

        LimitExec limit = new LimitExec(SRC, exec1, new Literal(SRC, 10, DataType.INTEGER), null);

        Map<String, ExternalSourceFactory> factories = Map.of(
            "parquet",
            testFactory(new FileSplitProvider()),
            "csv",
            testFactory(new FileSplitProvider())
        );

        PhysicalPlan result1 = SplitDiscoveryPhase.resolveExternalSplits(limit, factories);
        PhysicalPlan result2 = SplitDiscoveryPhase.resolveExternalSplits(exec2, factories);

        ExternalSourceExec resolved1 = (ExternalSourceExec) ((LimitExec) result1).child();
        ExternalSourceExec resolved2 = (ExternalSourceExec) result2;

        assertEquals(2, resolved1.splits().size());
        assertEquals(3, resolved2.splits().size());
    }

    public void testUnknownSourceTypeDefaultsToSingleProvider() {
        FileSet fileSet = createFileSet(3);
        ExternalSourceExec exec = createExternalSourceExec(fileSet, "unknown_type");

        Map<String, ExternalSourceFactory> factories = Map.of();

        PhysicalPlan result = SplitDiscoveryPhase.resolveExternalSplits(exec, factories);

        assertTrue(result instanceof ExternalSourceExec);
        ExternalSourceExec resolved = (ExternalSourceExec) result;
        assertTrue(resolved.splits().isEmpty());
    }

    public void testSplitCountMatchesFileCount() {
        int fileCount = randomIntBetween(1, 10);
        FileSet fileSet = createFileSet(fileCount);
        ExternalSourceExec exec = createExternalSourceExec(fileSet, "parquet");

        Map<String, ExternalSourceFactory> factories = Map.of("parquet", testFactory(new FileSplitProvider()));

        PhysicalPlan result = SplitDiscoveryPhase.resolveExternalSplits(exec, factories);

        ExternalSourceExec resolved = (ExternalSourceExec) result;
        assertEquals(fileCount, resolved.splits().size());
    }

    public void testFiltersFromDifferentBranchesAreNotMixed() {
        FileSet fileSet1 = createFileSet(2);
        FileSet fileSet2 = createFileSet(2);
        ExternalSourceExec exec1 = createExternalSourceExec(fileSet1, "parquet");
        ExternalSourceExec exec2 = createExternalSourceExec(fileSet2, "csv");

        Expression cond1 = new Equals(SRC, fieldAttr("year", DataType.INTEGER), new Literal(SRC, 2024, DataType.INTEGER));
        FilterExec filter1 = new FilterExec(SRC, exec1, cond1);

        Expression cond2 = new Equals(SRC, fieldAttr("month", DataType.INTEGER), new Literal(SRC, 6, DataType.INTEGER));
        FilterExec filter2 = new FilterExec(SRC, exec2, cond2);

        RecordingSplitProvider recorder1 = new RecordingSplitProvider();
        RecordingSplitProvider recorder2 = new RecordingSplitProvider();

        Map<String, ExternalSourceFactory> factories = Map.of("parquet", testFactory(recorder1), "csv", testFactory(recorder2));

        SplitDiscoveryPhase.resolveExternalSplits(filter1, factories);
        SplitDiscoveryPhase.resolveExternalSplits(filter2, factories);

        assertEquals(1, recorder1.lastContext.filterHints().size());
        assertEquals(1, recorder2.lastContext.filterHints().size());
    }

    public void testNestedFiltersAccumulateForDescendantSource() {
        FileSet fileSet = createFileSet(2);
        ExternalSourceExec exec = createExternalSourceExec(fileSet, "parquet");
        Expression cond1 = new Equals(SRC, fieldAttr("year", DataType.INTEGER), new Literal(SRC, 2024, DataType.INTEGER));
        Expression cond2 = new Equals(SRC, fieldAttr("month", DataType.INTEGER), new Literal(SRC, 6, DataType.INTEGER));
        FilterExec inner = new FilterExec(SRC, exec, cond1);
        FilterExec outer = new FilterExec(SRC, inner, cond2);

        RecordingSplitProvider recorder = new RecordingSplitProvider();
        Map<String, ExternalSourceFactory> factories = Map.of("parquet", testFactory(recorder));

        SplitDiscoveryPhase.resolveExternalSplits(outer, factories);

        assertEquals(2, recorder.lastContext.filterHints().size());
    }

    public void testNoFiltersWhenNoFilterExecInPlan() {
        FileSet fileSet = createFileSet(2);
        ExternalSourceExec exec = createExternalSourceExec(fileSet, "parquet");
        LimitExec limit = new LimitExec(SRC, exec, new Literal(SRC, 10, DataType.INTEGER), null);

        RecordingSplitProvider recorder = new RecordingSplitProvider();
        Map<String, ExternalSourceFactory> factories = Map.of("parquet", testFactory(recorder));

        SplitDiscoveryPhase.resolveExternalSplits(limit, factories);

        assertTrue(recorder.lastContext.filterHints().isEmpty());
    }

    // -- helpers --

    private static FileSet createFileSet(int fileCount) {
        List<StorageEntry> entries = new ArrayList<>();
        for (int i = 0; i < fileCount; i++) {
            entries.add(new StorageEntry(StoragePath.of("s3://bucket/data/file" + i + ".parquet"), 100 * (i + 1), Instant.EPOCH));
        }
        return new FileSet(entries, "s3://bucket/data/*.parquet");
    }

    private static ExternalSourceExec createExternalSourceExec(FileSet fileSet, String sourceType) {
        List<Attribute> attrs = List.of(fieldAttr("id", DataType.LONG), fieldAttr("name", DataType.KEYWORD));
        return new ExternalSourceExec(SRC, "s3://bucket/data/*.parquet", sourceType, attrs, Map.of(), Map.of(), null, null, fileSet);
    }

    private static Attribute fieldAttr(String name, DataType type) {
        return new FieldAttribute(SRC, name, new EsField(name, type, Map.of(), false, EsField.TimeSeriesFieldType.NONE));
    }

    private static ExternalSourceFactory testFactory(SplitProvider provider) {
        return new ExternalSourceFactory() {
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
                return provider;
            }
        };
    }

    private static class RecordingSplitProvider implements SplitProvider {
        SplitDiscoveryContext lastContext;

        @Override
        public List<ExternalSplit> discoverSplits(SplitDiscoveryContext context) {
            this.lastContext = context;
            return List.of();
        }
    }
}
