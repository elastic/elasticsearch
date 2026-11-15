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
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.DateEsField;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.execution.sequence.SequenceStageInput;
import org.elasticsearch.xpack.esql.index.EsIndex;
import org.elasticsearch.xpack.esql.index.IndexProperties;
import org.elasticsearch.xpack.esql.optimizer.LogicalPlanOptimizer;
import org.elasticsearch.xpack.esql.plan.logical.Sequence;
import org.elasticsearch.xpack.esql.plan.logical.SequenceStage;
import org.elasticsearch.xpack.esql.plan.logical.join.AbstractSubqueryJoin;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalSupplier;
import org.elasticsearch.xpack.esql.plan.physical.FragmentExec;
import org.elasticsearch.xpack.esql.plan.physical.MergeExec;
import org.junit.BeforeClass;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.xpack.esql.core.type.EsField.TimeSeriesFieldType.NONE;

/** Uses real compute drivers to check the source identity bridge through ordinary stage transformations. */
public class SequenceStageInputTests extends ESTestCase {
    /** These stage plans are analyzed through the running build's default parser. */
    @BeforeClass
    public static void requireSequenceCommand() {
        assumeTrue("requires native SEQUENCE", EsqlCapabilities.Cap.SEQUENCE_COMMAND.isEnabled());
    }

    public void testExpandedDerivedValuesKeepOriginalRow() {
        SequenceStage stage = firstStage("""
            sequence by normalized WITHIN 30 seconds PRIMARY 1
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
            sequence by normalized WITHIN 30 seconds PRIMARY 1
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

    /** A compiled local plan must create a fresh source for every separately bound source page. */
    public void testReusedLocalPlanReadsFreshBindings() {
        var prepared = new SequenceStageInput(firstStage("""
            sequence by normalized WITHIN 30 seconds PRIMARY 1
            (from a | mv_expand labels
              | eval normalized = TO_LOWER(labels), payload = CONCAT(label, "/", labels)
              | keep normalized, payload),
            (from b | eval normalized = label | keep normalized, label)
            | limit 3
            """));
        LocalExecutionPlanner.LocalExecutionPlan planned = null;
        for (String value : List.of("first", "second", "third")) {
            try (
                Page raw = raw(prepared.source(0).output(), List.of(Map.of("label", value, "labels", List.of("Alpha", "Beta"))));
                var bound = prepared.bind(0, raw, blockFactory())
            ) {
                if (planned == null) {
                    planned = plan(bound);
                }
                List<Page> pages = run(planned);
                try {
                    assertEquals(Set.of("0|alpha|" + value + "/Alpha", "0|beta|" + value + "/Beta"), tuples(pages));
                } finally {
                    pages.forEach(Page::releaseBlocks);
                }
            }
        }
    }

    /** Prepared membership data belongs to the query and remains readable by each fresh stage driver. */
    public void testReusedLocalPlanKeepsPreparedMembershipTable() {
        SequenceStage logical = firstStage("""
            sequence by normalized WITHIN 30 seconds PRIMARY 1
            (from a | where label IN (from b | keep label)
              | eval normalized = TO_LOWER(label), payload = CONCAT(label, "/", labels)
              | keep normalized, payload),
            (from b | eval normalized = label | keep normalized, label)
            | limit 3
            """);
        AbstractSubqueryJoin membership = logical.child().collect(AbstractSubqueryJoin.class).getFirst();
        var table = new AtomicReference<>(raw(membership.right().output(), List.of(Map.of("label", "Alpha"), Map.of("label", "Beta"))));
        try {
            // The session lowers a materialized membership result before mapping the stage. Force its supported
            // hash-join route so every fresh driver must read the query-owned deduplicated table and sentinel.
            var inlined = AbstractSubqueryJoin.inlineData(
                membership,
                new LocalRelation(membership.right().source(), membership.right().output(), LocalSupplier.of(table.get())),
                0,
                blockFactory(),
                table
            );
            SequenceStage stage = logical.replaceChild(
                logical.child().transformUp(AbstractSubqueryJoin.class, join -> join == membership ? inlined : join)
            );
            assertTrue(stage.collect(AbstractSubqueryJoin.class).isEmpty());
            assertNotNull("hash-join route must retain its deduplicated membership table", table.get());
            var prepared = new SequenceStageInput(stage);
            assertEquals(1, prepared.sourceCount());
            LocalExecutionPlanner.LocalExecutionPlan planned = null;
            for (String value : List.of("first", "second", "third")) {
                try (
                    Page raw = raw(
                        prepared.source(0).output(),
                        List.of(
                            Map.of("label", "Alpha", "labels", value),
                            Map.of("label", "skip", "labels", value),
                            Map.of("label", "Beta", "labels", value)
                        )
                    );
                    var bound = prepared.bind(0, raw, blockFactory())
                ) {
                    if (planned == null) {
                        planned = plan(bound);
                    }
                    List<Page> pages = run(planned);
                    try {
                        assertEquals(Set.of("0|alpha|Alpha/" + value, "2|beta|Beta/" + value), tuples(pages));
                    } finally {
                        pages.forEach(Page::releaseBlocks);
                    }
                }
            }
        } finally {
            Page retained = table.getAndSet(null);
            if (retained != null) {
                retained.releaseBlocks();
            }
        }
    }

    public void testIdentitySurvivesZeroVisibleColumns() {
        SequenceStage stage = firstStage("""
            sequence WITHIN 30 seconds PRIMARY 1 (from a), (from b)
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
            sequence by label WITHIN 30 seconds PRIMARY 1 (from a | keep label), (from b | keep label) | limit 3
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
            sequence by label WITHIN 30 seconds PRIMARY 1
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
            sequence by labels WITHIN 30 seconds PRIMARY 1
            (from a | mv_expand labels | where labels=="fresh" | keep labels),
            (from b | mv_expand labels | keep labels) | limit 3
            """));
        assertNull(expanded.searchQuery(0));
    }

    public void testSelectionRetentionExportsExistingWhereValueWithoutOutputOnlyEval() {
        SequenceStage stage = firstStage("""
            sequence by label WITHIN 30 seconds PRIMARY 1
            (from a | eval lower=TO_LOWER(label), output_only=TO_LONG(label)
              | where lower!="skip" | keep label, lower, output_only),
            (from b | keep label) | limit 3
            """);
        Attribute lower = stage.output().stream().filter(attribute -> attribute.name().equals("lower")).findFirst().orElseThrow();
        var retained = SequenceStageInput.selection(stage, List.of(lower));
        assertTrue(retained.retention(0).direct());
        assertEquals(List.of(lower), retained.retention(0).retained());
        assertEquals(List.of("label"), retained.source(0).output().stream().map(Attribute::name).toList());
        try (
            Page raw = raw(
                retained.source(0).output(),
                List.of(Map.of("label", "ALPHA"), Map.of("label", "skip"), Map.of("label", "Beta"))
            );
            var bound = retained.bind(0, raw, blockFactory())
        ) {
            List<Page> pages = run(bound);
            try {
                assertEquals(Set.of("0|ALPHA|alpha", "2|Beta|beta"), tuples(pages));
            } finally {
                pages.forEach(Page::releaseBlocks);
            }
        }
        Attribute output = stage.output().stream().filter(attribute -> attribute.name().equals("output_only")).findFirst().orElseThrow();
        var replay = SequenceStageInput.selection(stage, List.of(output));
        assertFalse("an EVAL sibling absent from the selection closure must replay", replay.retention(0).direct());
        assertEquals(List.of("label"), replay.source(0).output().stream().map(Attribute::name).toList());
        assertEquals(2, replay.output(0).size());
    }

    public void testSelectionRetentionTracesGenuineMetadataAndRawMappedAliases() {
        SequenceStage stage = firstStage("""
            sequence by label WITHIN 30 seconds PRIMARY 1
            (from a metadata _id, _index, _source | rename _id as original_id, _source as body, labels as values
              | keep label, original_id, _index, body, values),
            (from b | keep label) | limit 3
            """);
        List<Attribute> demands = stage.output().stream().filter(attribute -> attribute.name().equals("label") == false).toList();
        var selection = SequenceStageInput.selection(stage, demands);
        var retention = selection.retention(0);
        assertTrue(retention.direct());
        assertTrue(retention.retained().isEmpty());
        assertEquals(
            List.of("_id", "_index", "_source", "labels"),
            retention.bindings().stream().map(binding -> binding.source().name()).toList()
        );
        assertEquals(
            List.of(
                SequenceStageInput.ValueKind.METADATA,
                SequenceStageInput.ValueKind.METADATA,
                SequenceStageInput.ValueKind.METADATA,
                SequenceStageInput.ValueKind.FIELD
            ),
            retention.bindings().stream().map(SequenceStageInput.ValueBinding::kind).toList()
        );
        assertEquals(List.of("_source", "labels"), retention.fetched().stream().map(Attribute::name).toList());
        assertEquals(List.of("label"), selection.source(0).output().stream().map(Attribute::name).toList());
        SequenceStage overwritten = firstStage("""
            sequence by label WITHIN 30 seconds PRIMARY 1
            (from a metadata _id | eval _id=TO_UPPER(label) | keep label, _id),
            (from b | keep label) | limit 3
            """);
        Attribute changedId = overwritten.output().stream().filter(attribute -> attribute.name().equals("_id")).findFirst().orElseThrow();
        assertFalse(SequenceStageInput.selection(overwritten, List.of(changedId)).retention(0).direct());
    }

    public void testSelectionRetentionIsBranchLocalAcrossUnionProducerBindings() {
        SequenceStage stage = firstStage("""
            sequence by label WITHIN 30 seconds PRIMARY 1
            (from (from a | eval value=TO_LOWER(label) | where value!="skip" | keep label, value),
                  (from b | eval value=TO_UPPER(label) | keep label, value)),
            (from b | keep label) | limit 3
            """);
        Attribute value = stage.output().stream().filter(attribute -> attribute.name().equals("value")).findFirst().orElseThrow();
        var selection = SequenceStageInput.selection(stage, List.of(value));
        assertTrue(selection.retention(0).direct());
        assertEquals(List.of(value), selection.retention(0).retained());
        assertEquals(3, selection.output(0).size());
        assertFalse(selection.retention(1).direct());
        assertEquals(2, selection.output(1).size());
        for (int branch : List.of(0, 1)) {
            assertEquals(List.of("label"), selection.source(branch).output().stream().map(Attribute::name).toList());
            try (
                Page raw = raw(selection.source(branch).output(), List.of(Map.of("label", "ALPHA")));
                var bound = selection.bind(branch, raw, blockFactory())
            ) {
                List<Page> pages = run(bound);
                try {
                    assertEquals(selection.output(branch), bound.plan().output());
                    assertEquals(1, pages.stream().mapToInt(Page::getPositionCount).sum());
                    assertEquals(branch == 0 ? 3 : 2, pages.getFirst().getBlockCount());
                } finally {
                    pages.forEach(Page::releaseBlocks);
                }
            }
        }
    }

    /** Retained proof objects must describe the current optimized output, not an earlier attribute wrapper. */
    public void testRetainedExpandedOriginalUsesCurrentOutputAttribute() {
        SequenceStage stage = firstStage("""
            SEQUENCE BY label WITHIN 30 seconds PRIMARY 2
            (FROM a | EVAL copied = labels | MV_EXPAND copied | KEEP label, labels),
            (FROM b | KEEP label)
            | EVAL original = MEMBER(1, labels) | KEEP label, original | LIMIT 1
            """);
        Attribute current = stage.output().stream().filter(attribute -> attribute.name().equals("labels")).findFirst().orElseThrow();
        Attribute source = new SequenceStageInput(stage).source(0)
            .output()
            .stream()
            .filter(attribute -> attribute.id().equals(current.id()))
            .findFirst()
            .orElseThrow();
        assertNotEquals("the optimizer must exercise the stale wrapper boundary", source, current);
        var selection = SequenceStageInput.selection(stage, List.of(source));
        var retained = selection.retention(0);
        assertTrue(retained.direct());
        assertEquals(1, retained.retained().size());
        assertSame(current, retained.retained().getFirst());
        assertTrue(selection.output(0).stream().anyMatch(attribute -> attribute == retained.retained().getFirst()));
    }

    /** Each union branch keeps the original array while its copied value expands through the same stage. */
    public void testUnionExpandedCopyPreservesOriginalArrayInEveryBranch() {
        SequenceStage stage = firstStage("""
            SEQUENCE BY label WITHIN 30 seconds PRIMARY 2
            (FROM (FROM a), (FROM b)
              | EVAL copied = labels | MV_EXPAND copied
              | WHERE copied != "skip" | KEEP label, labels),
            (FROM b | KEEP label)
            | EVAL original = MEMBER(1, labels) | KEEP label, original | LIMIT 1
            """);
        Attribute original = stage.output().stream().filter(attribute -> attribute.name().equals("labels")).findFirst().orElseThrow();
        var selection = SequenceStageInput.selection(stage, List.of(original));
        assertEquals(2, selection.sourceCount());
        for (int branch = 0; branch < selection.sourceCount(); branch++) {
            assertTrue(selection.retention(branch).direct());
            try (
                Page raw = raw(selection.source(branch).output(), List.of(Map.of("label", "source", "labels", List.of("Alpha", "Beta"))));
                var bound = selection.bind(branch, raw, blockFactory())
            ) {
                List<Page> pages = run(bound);
                try {
                    assertEquals(2, pages.stream().mapToInt(Page::getPositionCount).sum());
                    for (Page page : pages) {
                        assertEquals(3, page.getBlockCount());
                        BytesRefBlock labels = page.getBlock(1);
                        for (int row = 0; row < page.getPositionCount(); row++) {
                            assertEquals(2, labels.getValueCount(row));
                            int first = labels.getFirstValueIndex(row);
                            assertEquals("Alpha", labels.getBytesRef(first, new BytesRef()).utf8ToString());
                            assertEquals("Beta", labels.getBytesRef(first + 1, new BytesRef()).utf8ToString());
                        }
                    }
                } finally {
                    pages.forEach(Page::releaseBlocks);
                }
            }
        }
    }

    /** A broken internal retained binding must identify its input stage even outside a numbered source operator. */
    public void testRetentionFailureIdentifiesStageLocation() {
        SequenceStage stage = firstStage("""
            SEQUENCE BY label WITHIN 30 seconds PRIMARY 1
            (FROM a | EVAL lower = TO_LOWER(label) | WHERE lower != "skip" | KEEP label, lower),
            (FROM b | KEEP label) | KEEP label, lower | LIMIT 1
            """);
        Attribute current = stage.output().stream().filter(attribute -> attribute.name().equals("lower")).findFirst().orElseThrow();
        Attribute wrongType = new ReferenceAttribute(
            current.source(),
            null,
            current.name(),
            DataType.INTEGER,
            current.nullable(),
            current.id(),
            current.synthetic()
        );
        String message = expectThrows(
            IllegalStateException.class,
            () -> SequenceStageInput.selection(stage, List.of(wrongType)).retention(0)
        ).getMessage();
        assertTrue(message, message.contains("output changed type of column [lower#"));
        assertTrue(
            message,
            message.contains(
                "stage at [line " + stage.source().source().getLineNumber() + ", column " + stage.source().source().getColumnNumber() + "]"
            )
        );
    }

    public void testSelectionRetentionKeepsFullExtractorLayoutAndExpandedCellIdentity() {
        SequenceStage extracted = firstStage("""
            sequence by extracted_key WITHIN 30 seconds PRIMARY 1
            (from a | dissect label "%{extracted_key}/%{value}" | keep extracted_key, value),
            (from b | dissect label "%{extracted_key}/%{value}" | keep extracted_key, value) | limit 3
            """);
        Attribute value = extracted.output().stream().filter(attribute -> attribute.name().equals("value")).findFirst().orElseThrow();
        var selection = SequenceStageInput.selection(extracted, List.of(value));
        assertTrue(selection.retention(0).direct());
        try (
            Page raw = raw(selection.source(0).output(), List.of(Map.of("label", "alpha/one")));
            var bound = selection.bind(0, raw, blockFactory())
        ) {
            List<Page> pages = run(bound);
            try {
                assertEquals(Set.of("0|alpha|one"), tuples(pages));
            } finally {
                pages.forEach(Page::releaseBlocks);
            }
        }
        SequenceStage expanded = firstStage("""
            sequence by label WITHIN 30 seconds PRIMARY 1
            (from a | mv_expand labels | keep label, labels), (from b | keep label) | limit 3
            """);
        Attribute labels = expanded.output().stream().filter(attribute -> attribute.name().equals("labels")).findFirst().orElseThrow();
        var expansion = SequenceStageInput.selection(expanded, List.of(labels));
        assertTrue(expansion.retention(0).direct());
        assertEquals(List.of(labels), expansion.retention(0).retained());
        try (
            Page raw = raw(expansion.source(0).output(), List.of(Map.of("label", "shared", "labels", List.of("a", "b"))));
            var bound = expansion.bind(0, raw, blockFactory())
        ) {
            List<Page> pages = run(bound);
            try {
                assertEquals(Set.of("0|shared|a", "0|shared|b"), tuples(pages));
            } finally {
                pages.forEach(Page::releaseBlocks);
            }
        }
    }

    /** An output-free stage needs no eligibility proof or its temporary-plan admission. */
    public void testEmptyDemandSelectionSkipsRetentionProofAdmission() {
        SequenceStage stage = firstStage("""
            sequence WITHIN 30 seconds PRIMARY 1 (from a), (from b)
            | stats count(*) | limit 3
            """);
        var selection = SequenceStageInput.selection(stage, List.of(), bytes -> {
            throw new AssertionError("empty output demand reserved retention proof bytes [" + bytes + "]");
        });
        assertEquals(1, selection.sourceCount());
        assertTrue(selection.source(0).output().isEmpty());
        LocalExecutionPlanner.LocalExecutionPlan planned = null;
        for (int rows : List.of(2, 3)) {
            try (Page raw = new Page(rows); var bound = selection.bind(0, raw, blockFactory())) {
                assertEquals(List.of(selection.rowMarker()), bound.plan().output());
                if (planned == null) {
                    planned = plan(bound);
                }
                List<Page> pages = run(planned);
                try {
                    List<Long> actual = new ArrayList<>();
                    for (Page page : pages) {
                        assertEquals(1, page.getBlockCount());
                        LongBlock marker = page.getBlock(0);
                        for (int row = 0; row < page.getPositionCount(); row++) {
                            actual.add(marker.getLong(marker.getFirstValueIndex(row)));
                        }
                    }
                    assertEquals(rows == 2 ? List.of(0L, 1L) : List.of(0L, 1L, 2L), actual);
                } finally {
                    pages.forEach(Page::releaseBlocks);
                }
            }
        }
    }

    /** Empty payloads must still run key, predicate and expansion work on every fresh page binding. */
    public void testEmptyDemandSelectionPreservesKeysPredicatesAndExpansion() {
        SequenceStage stage = firstStage("""
            sequence by normalized WITHIN 30 seconds PRIMARY 1
            (from a | mv_expand labels | eval normalized=TO_LOWER(labels)
              | where normalized!="skip" | keep normalized),
            (from b | eval normalized=label | keep normalized)
            | stats count(*) | limit 3
            """);
        var ordinary = new SequenceStageInput(stage, List.of());
        var selection = SequenceStageInput.selection(stage, List.of());
        assertEquals(ordinary.source(0).output(), selection.source(0).output());
        assertEquals(List.of("labels"), selection.source(0).output().stream().map(Attribute::name).toList());
        for (var input : List.of(ordinary, selection)) {
            LocalExecutionPlanner.LocalExecutionPlan planned = null;
            for (String suffix : List.of("Alpha", "Beta")) {
                try (
                    Page raw = raw(
                        input.source(0).output(),
                        List.of(Map.of("labels", List.of(suffix, "skip")), Map.of("labels", List.of("Gamma", "skip")))
                    );
                    var bound = input.bind(0, raw, blockFactory())
                ) {
                    if (planned == null) {
                        planned = plan(bound);
                    }
                    List<Page> pages = run(planned);
                    try {
                        List<String> actual = new ArrayList<>();
                        BytesRef scratch = new BytesRef();
                        for (Page page : pages) {
                            assertEquals(2, page.getBlockCount());
                            BytesRefBlock key = page.getBlock(0);
                            LongBlock marker = page.getBlock(1);
                            for (int row = 0; row < page.getPositionCount(); row++) {
                                actual.add(
                                    marker.getLong(marker.getFirstValueIndex(row))
                                        + "|"
                                        + key.getBytesRef(key.getFirstValueIndex(row), scratch).utf8ToString()
                                );
                            }
                        }
                        assertEquals(List.of("0|" + (suffix.equals("Alpha") ? "alpha" : "beta"), "1|gamma"), actual);
                    } finally {
                        pages.forEach(Page::releaseBlocks);
                    }
                }
            }
        }
    }

    /** The empty-demand shortcut must leave nonempty branch proofs lazy, admitted and memoized. */
    public void testNonemptyDemandSelectionStillAdmitsEachUsedBranchProof() {
        SequenceStage stage = firstStage("""
            sequence by label WITHIN 30 seconds PRIMARY 1
            (from (from a | eval value=TO_LOWER(label) | where value!="skip" | keep label, value),
                  (from b | eval value=TO_UPPER(label) | keep label, value)),
            (from b | keep label) | limit 3
            """);
        Attribute value = stage.output().stream().filter(attribute -> attribute.name().equals("value")).findFirst().orElseThrow();
        List<Long> admitted = new ArrayList<>();
        var selection = SequenceStageInput.selection(stage, List.of(value), admitted::add);
        assertEquals(2, selection.sourceCount());
        assertTrue(admitted.isEmpty());
        assertTrue(selection.retention(0).direct());
        assertEquals(1, admitted.size());
        assertTrue(admitted.getFirst() > 0);
        selection.source(0);
        selection.searchQuery(0);
        selection.output(0);
        assertEquals(1, admitted.size());
        assertFalse(selection.retention(1).direct());
        assertEquals(2, admitted.size());
        assertTrue(admitted.get(1) > 0);
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
        return run(plan(bound));
    }

    private static LocalExecutionPlanner.LocalExecutionPlan plan(SequenceStageInput.Bound bound) {
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
            false,
            null,
            0,
            MatcherWatchdog.noop(),
            TransportVersion.current()
        );
        return planner.plan(
            "sequence-stage",
            FoldContext.small(),
            PlannerSettings.DEFAULTS,
            bound.plan(),
            EmptyIndexedByShardId.instance(),
            true
        );
    }

    private static List<Page> run(LocalExecutionPlanner.LocalExecutionPlan planned) {
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
        var analyzed = analyzer.statement(query);
        var optimizer = new LogicalPlanOptimizer(
            EsqlTestUtils.logicalOptimizerContext(EsqlTestUtils.TEST_CFG, FoldContext.small(), TransportVersion.current())
        );
        Sequence sequence = optimizer.optimize(analyzed).collect(Sequence.class).getFirst();
        return (SequenceStage) sequence.children().getFirst();
    }
}
