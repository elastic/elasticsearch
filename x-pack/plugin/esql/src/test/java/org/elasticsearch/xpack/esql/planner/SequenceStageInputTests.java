/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.planner;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BlockUtils;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.lucene.EmptyIndexedByShardId;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.querydsl.query.QueryWarnings;
import org.elasticsearch.compute.test.TestBlockFactory;
import org.elasticsearch.compute.test.TestDriverRunner;
import org.elasticsearch.grok.MatcherWatchdog;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.DateEsField;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.execution.sequence.SequenceStageInput;
import org.elasticsearch.xpack.esql.index.EsIndex;
import org.elasticsearch.xpack.esql.index.IndexProperties;
import org.elasticsearch.xpack.esql.optimizer.LogicalPlanOptimizer;
import org.elasticsearch.xpack.esql.plan.logical.Sequence;
import org.elasticsearch.xpack.esql.plan.logical.SequenceStage;
import org.elasticsearch.xpack.esql.plan.physical.FragmentExec;
import org.elasticsearch.xpack.esql.plan.physical.MergeExec;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.xpack.esql.core.type.EsField.TimeSeriesFieldType.NONE;

/** Uses real compute drivers to check the source identity bridge through ordinary stage transformations. */
public class SequenceStageInputTests extends ESTestCase {
    public void testExpandedDerivedValuesKeepOriginalRow() {
        SequenceStage stage = firstStage("""
            sequence by normalized with {"maxspan":"30s"}
            (from a | mv_expand labels
              | eval normalized = TO_LOWER(labels), payload = CONCAT(label, "/", labels)
              | where normalized != "skip" | keep normalized, payload),
            (from b | eval normalized = label | keep normalized, label)
            | limit 3
            """);
        var prepared = new SequenceStageInput(stage);
        assertFalse(prepared.source(0).output().stream().anyMatch(a -> a.name().equals("@timestamp")));
        List<Map<String, Object>> rows = List.of(
            Map.of("label", "first", "labels", List.of("ALPHA", "skip", "Beta")),
            Map.of("label", "second", "labels", List.of("BETA", "Gamma"))
        );
        try (Page raw = raw(prepared.source(0).output(), rows); var bound = prepared.bind(0, raw, blockFactory())) {
            assertEquals(prepared.output(), bound.plan().output());
            assertTrue(bound.plan().collect(FragmentExec.class).isEmpty());
            raw.releaseBlocks(); // The binding must own a reference independent of the search reader's page.
            List<Page> pages = run(bound);
            try {
                assertEquals(
                    Set.of("0|alpha|first/ALPHA", "0|beta|first/Beta", "1|beta|second/BETA", "1|gamma|second/Gamma"),
                    tuples(pages)
                );
            } finally {
                pages.forEach(Page::releaseBlocks);
            }
        }
    }

    public void testUnionBranchPreservesMergedExpressionIds() {
        SequenceStage stage = firstStage("""
            sequence by normalized with {"maxspan":"30s"}
            (from (from a | eval branch = "A"), (from b | eval branch = "B")
              | mv_expand labels
              | eval normalized = TO_LOWER(labels), payload = CONCAT(branch, "/", label, "/", labels)
              | keep normalized, payload),
            (from b | eval normalized = label | keep normalized, label)
            | limit 3
            """);
        var prepared = new SequenceStageInput(stage);
        assertEquals(2, prepared.sourceCount());
        for (int branch = 0; branch < prepared.sourceCount(); branch++) {
            try (
                Page raw = raw(prepared.source(branch).output(), List.of(Map.of("label", "source", "labels", List.of("Alpha", "Beta"))));
                var bound = prepared.bind(branch, raw, blockFactory())
            ) {
                assertEquals(prepared.output(), bound.plan().output());
                assertTrue(bound.plan().collect(MergeExec.class).isEmpty());
                List<Page> pages = run(bound);
                try {
                    String prefix = branch == 0 ? "A" : "B";
                    assertEquals(Set.of("0|alpha|" + prefix + "/source/Alpha", "0|beta|" + prefix + "/source/Beta"), tuples(pages));
                } finally {
                    pages.forEach(Page::releaseBlocks);
                }
            }
        }
    }

    public void testIdentitySurvivesZeroVisibleColumns() {
        SequenceStage stage = firstStage("""
            sequence with {"maxspan":"30s"} (from a), (from b)
            | stats count(*)
            | limit 3
            """);
        var prepared = new SequenceStageInput(stage, List.of());
        assertTrue(prepared.source(0).output().isEmpty());
        try (Page raw = new Page(2); var bound = prepared.bind(0, raw, blockFactory())) {
            assertEquals(List.of(prepared.rowMarker()), bound.plan().output());
            List<Page> pages = run(bound);
            try {
                List<Long> positions = new ArrayList<>();
                for (Page page : pages) {
                    assertEquals(1, page.getBlockCount());
                    LongBlock block = page.getBlock(0);
                    for (int row = 0; row < page.getPositionCount(); row++) {
                        positions.add(block.getLong(block.getFirstValueIndex(row)));
                    }
                }
                assertEquals(List.of(0L, 1L), positions);
            } finally {
                pages.forEach(Page::releaseBlocks);
            }
        }
    }

    public void testBindingValidation() {
        SequenceStage stage = firstStage("""
            sequence by label with {"maxspan":"30s"} (from a | keep label), (from b | keep label) | limit 3
            """);
        var prepared = new SequenceStageInput(stage);
        try (Page raw = new Page(1)) {
            expectThrows(IllegalArgumentException.class, () -> prepared.bind(-1, raw, blockFactory()));
            expectThrows(IllegalArgumentException.class, () -> prepared.bind(1, raw, blockFactory()));
            expectThrows(IllegalArgumentException.class, () -> prepared.bind(0, raw, blockFactory()));
        }
    }

    public void testSourcePredicatePushdownRechecksAndExcludesExpandedFields() {
        var source = new SequenceStageInput(firstStage("""
            sequence by label with {"maxspan":"30s"}
            (from a | where label=="fresh" | keep label), (from b | keep label) | limit 3
            """));
        assertNotNull(source.searchQuery(0));
        try (
            Page raw = raw(source.source(0).output(), List.of(Map.of("label", "stale"), Map.of("label", "fresh")));
            var bound = source.bind(0, raw, blockFactory())
        ) {
            List<Page> pages = run(bound);
            try {
                assertEquals(1, pages.stream().mapToInt(Page::getPositionCount).sum());
            } finally {
                pages.forEach(Page::releaseBlocks);
            }
        }
        var expanded = new SequenceStageInput(firstStage("""
            sequence by labels with {"maxspan":"30s"}
            (from a | mv_expand labels | where labels=="fresh" | keep labels),
            (from b | mv_expand labels | keep labels) | limit 3
            """));
        assertNull(expanded.searchQuery(0));
    }

    private static Set<String> tuples(List<Page> pages) {
        Set<String> tuples = new HashSet<>();
        BytesRef scratch = new BytesRef();
        int count = 0;
        for (Page page : pages) {
            assertEquals(3, page.getBlockCount());
            BytesRefBlock key = page.getBlock(0);
            BytesRefBlock payload = page.getBlock(1);
            LongBlock marker = page.getBlock(2);
            for (int row = 0; row < page.getPositionCount(); row++) {
                tuples.add(
                    marker.getLong(marker.getFirstValueIndex(row))
                        + "|"
                        + key.getBytesRef(key.getFirstValueIndex(row), scratch).utf8ToString()
                        + "|"
                        + payload.getBytesRef(payload.getFirstValueIndex(row), scratch).utf8ToString()
                );
                count++;
            }
        }
        assertEquals("no duplicate transformed rows", count, tuples.size());
        return tuples;
    }

    private static Page raw(List<Attribute> attributes, List<Map<String, Object>> rows) {
        List<List<Object>> values = rows.stream().map(row -> attributes.stream().map(a -> row.get(a.name())).toList()).toList();
        return new Page(rows.size(), BlockUtils.fromList(blockFactory(), values));
    }

    private static BlockFactory blockFactory() {
        return TestBlockFactory.getNonBreakingInstance();
    }

    private static List<Page> run(SequenceStageInput.Bound bound) {
        var planner = new LocalExecutionPlanner(
            "sequence-stage",
            "",
            null,
            BigArrays.NON_RECYCLING_INSTANCE,
            blockFactory(),
            Settings.EMPTY,
            EsqlTestUtils.TEST_CFG,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            ProjectMetadata.builder(ProjectId.DEFAULT).build(),
            new EsPhysicalOperationProviders(
                FoldContext.small(),
                EmptyIndexedByShardId.instance(),
                null,
                PlannerSettings.DEFAULTS,
                () -> 0L,
                QueryWarnings.EMIT
            ),
            null,
            null,
            null,
            0,
            MatcherWatchdog.noop(),
            TransportVersion.current()
        );
        var planned = planner.plan(
            "sequence-stage",
            FoldContext.small(),
            PlannerSettings.DEFAULTS,
            bound.plan(),
            EmptyIndexedByShardId.instance(),
            true
        );
        assertEquals(1, planned.driverFactories.size());
        var operation = planned.driverFactories.getFirst().driverSupplier().physicalOperation();
        var context = new DriverContext(blockFactory().bigArrays(), blockFactory(), null);
        var runner = new TestDriverRunner().builder(context);
        runner.input(operation.sourceOperatorFactory.get(context));
        return runner.run(operation.intermediateOperatorFactories.toArray(Operator.OperatorFactory[]::new));
    }

    private static SequenceStage firstStage(String query) {
        Map<String, EsField> mapping = Map.of(
            "@timestamp",
            DateEsField.dateEsField("@timestamp", Map.of(), true, NONE),
            "label",
            new EsField("label", DataType.KEYWORD, Map.of(), true, NONE),
            "labels",
            new EsField("labels", DataType.KEYWORD, Map.of(), true, NONE)
        );
        var analyzer = EsqlTestUtils.analyzer().minimumTransportVersion(TransportVersion.current());
        for (String index : List.of("a", "b")) {
            analyzer.addIndex(new EsIndex(index, mapping, Map.of(index, new IndexProperties(IndexMode.STANDARD, 0)), Map.of(), Map.of()));
        }
        var analyzed = analyzer.query(query);
        var optimizer = new LogicalPlanOptimizer(
            EsqlTestUtils.logicalOptimizerContext(EsqlTestUtils.TEST_CFG, FoldContext.small(), TransportVersion.current())
        );
        Sequence sequence = optimizer.optimize(analyzed).collect(Sequence.class).getFirst();
        return (SequenceStage) sequence.children().getFirst();
    }
}
