/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.TestAnalyzer;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.DateEsField;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.index.EsIndex;
import org.elasticsearch.xpack.esql.index.IndexProperties;
import org.elasticsearch.xpack.esql.index.IndexResolution;
import org.elasticsearch.xpack.esql.optimizer.LogicalPlanOptimizer;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Sequence;
import org.elasticsearch.xpack.esql.plan.logical.SequenceCondition;
import org.elasticsearch.xpack.esql.plan.logical.SequenceStage;
import org.elasticsearch.xpack.esql.session.FieldNameUtils;
import org.junit.BeforeClass;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.analyzer;
import static org.elasticsearch.xpack.esql.core.type.EsField.TimeSeriesFieldType.NONE;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;

/** Verifies final earlier-stage visibility and WHERE-site own bindings independently of native condition execution. */
public class SequenceConditionAnalyzerTests extends ESTestCase {
    @BeforeClass
    public static void requireSequenceCommand() {
        assumeTrue("requires native SEQUENCE", EsqlCapabilities.Cap.SEQUENCE_COMMAND.isEnabled());
    }

    public void testBackwardBindingsResolveIncrementallyIncludingEarlierPrimaryAndKeys() {
        Sequence sequence = analyzed("""
            SEQUENCE BY key WITHIN 30s PRIMARY start_event
            (FROM a | KEEP key,number) AS start_event,
            (FROM b | WHERE number > MEMBER(start_event,number) | KEEP key,number) AS middle_event,
            (FROM c | WHERE number == MEMBER(1,number) + MEMBER(middle_event,number) AND key == MEMBER(start_event,key))
            | KEEP key | LIMIT 10
            """);
        assertTrue(sequence.resolved());
        assertEquals(3, sequence.collect(SequenceCondition.class).size());
        SequenceCondition multi = sequence.collect(SequenceCondition.class)
            .stream()
            .filter(condition -> condition.referencedStages().size() == 2)
            .findFirst()
            .orElseThrow();
        assertEquals(2, multi.ownStage());
        assertEquals(List.of(0, 1), multi.referencedStages());
        assertEquals(Set.of(0, 1, 2), new HashSet<>(multi.inputs().stream().map(SequenceCondition.Input::stage).toList()));
        for (SequenceCondition condition : sequence.collect(SequenceCondition.class)) {
            assertTrue(condition.expressionsResolved());
            assertTrue(condition.inputs().stream().allMatch(input -> input.reference().synthetic()));
            assertTrue(condition.inputs().stream().allMatch(input -> input.reference().id().equals(input.source().id()) == false));
        }
    }

    public void testOlderClusterRejectsEverySequenceShape() {
        TransportVersion old = TransportVersionUtils.getPreviousVersion(Sequence.ESQL_SEQUENCE_R6_SOURCE_SCOPE);
        for (String stages : List.of(
            "(FROM a),(FROM b)",
            "(FROM a),(FROM b) WITHIN 5s",
            "(FROM a),(FROM b | WHERE label == MEMBER(1,label))"
        )) {
            configured().minimumTransportVersion(old)
                .statementError(
                    "SEQUENCE WITHIN 30s PRIMARY 2 " + stages + " | KEEP key | LIMIT 10",
                    containsString("SEQUENCE is not supported by all nodes in the cluster")
                );
        }
    }

    public void testEarlierComputedAndRenamedOutputSurvivesPruningWithoutPublicConditionColumns() {
        LogicalPlan plan = configured().statement("""
            SEQUENCE BY key WITHIN 30s PRIMARY 2
            (FROM a | EVAL computed = TO_UPPER(label) | KEEP key,computed | RENAME computed AS visible),
            (FROM b | WHERE label == MEMBER(1,visible))
            | KEEP key | LIMIT 10
            """);
        Sequence sequence = optimizer().optimize(plan).collect(Sequence.class).getFirst();
        SequenceCondition condition = sequence.collect(SequenceCondition.class).getFirst();
        SequenceCondition.Input earlier = condition.activeInputs().stream().filter(input -> input.stage() == 0).findFirst().orElseThrow();
        assertEquals("visible", earlier.source().name());
        assertTrue(((SequenceStage) sequence.children().getFirst()).output().stream().anyMatch(a -> a.id().equals(earlier.source().id())));
        assertTrue(sequence.references().contains(earlier.source()));
        assertEquals(List.of("key"), sequence.output().stream().map(Attribute::name).toList());
    }

    public void testEarlierFinalKeepDropAndRenameAreAuthoritative() {
        for (String projection : List.of("KEEP key", "DROP label", "RENAME label AS renamed")) {
            configured().statementError(
                """
                    SEQUENCE BY key WITHIN 30s PRIMARY 2
                    (FROM a | %s), (FROM b | WHERE label == MEMBER(1,label))
                    | KEEP key | LIMIT 10
                    """.formatted(projection),
                allOf(
                    containsString("SEQUENCE stage [1]"),
                    containsString("final output column [label]"),
                    containsString("Expose [label] in stage [1] final output"),
                    containsString("add it to that stage's KEEP"),
                    containsString("undo its DROP"),
                    containsString("use the renamed column in MEMBER")
                )
            );
        }
        analyzed("""
            SEQUENCE BY key WITHIN 30s PRIMARY 2
            (FROM a | RENAME label AS renamed), (FROM b | WHERE label == MEMBER(1,renamed))
            | KEEP key | LIMIT 10
            """);
    }

    public void testOwnInputsBindBeforeTrailingProjectionAndStayPrivate() {
        Sequence sequence = analyzed("""
            SEQUENCE BY key WITHIN 30s PRIMARY 2
            (FROM a | KEEP key,label),
            (FROM b | EVAL own = TO_UPPER(label) | WHERE own == MEMBER(1,label)
             | DROP own | RENAME label AS renamed | KEEP key,renamed)
            | KEEP key,renamed | LIMIT 10
            """);
        SequenceCondition condition = sequence.collect(SequenceCondition.class).getFirst();
        SequenceCondition.Input own = condition.inputs().stream().filter(input -> input.stage() == 1).findFirst().orElseThrow();
        SequenceCondition.Input earlier = condition.inputs().stream().filter(input -> input.stage() == 0).findFirst().orElseThrow();
        assertEquals("own", own.source().name());
        assertTrue(condition.child().output().stream().anyMatch(a -> a.id().equals(own.source().id())));
        assertTrue(condition.references().contains(own.source()));
        assertFalse(condition.references().contains(earlier.source()));
        assertFalse(((SequenceStage) sequence.children().get(1)).output().stream().anyMatch(a -> a.id().equals(own.source().id())));
        assertTrue(sequence.output().stream().noneMatch(a -> a.name().contains("own") || a.synthetic()));
    }

    /** Merging consecutive WHEREs preserves the computed input's identity before a later projection removes its name. */
    public void testConsecutiveWheresPreserveOwnBindingAndErrors() {
        Sequence sequence = analyzed("""
            SEQUENCE BY key WITHIN 30s PRIMARY 2 (FROM a | KEEP key,label),
            (FROM b | EVAL own=TO_UPPER(label) | WHERE own != "SKIP" | WHERE own == MEMBER(1,label)
             | WHERE key == MEMBER(1,key) | DROP own | KEEP key,label)
            | KEEP key,label | LIMIT 10
            """);
        SequenceCondition condition = sequence.collect(SequenceCondition.class)
            .stream()
            .filter(candidate -> candidate.inputs().stream().anyMatch(input -> input.stage() == 1 && input.source().name().equals("own")))
            .findFirst()
            .orElseThrow();
        SequenceCondition.Input own = condition.inputs()
            .stream()
            .filter(input -> input.stage() == 1 && input.source().name().equals("own"))
            .findFirst()
            .orElseThrow();
        assertTrue(condition.child().output().stream().anyMatch(attribute -> attribute.id().equals(own.source().id())));
        assertFalse(((SequenceStage) sequence.children().get(1)).output().stream().anyMatch(attribute -> attribute.name().equals("own")));
        configured().statementError("""
            SEQUENCE BY key WITHIN 30s PRIMARY 2 (FROM a),
            (FROM b | WHERE missing_own == 1 | WHERE label == MEMBER(1,label)) | KEEP key | LIMIT 10
            """, allOf(containsString("Unknown column"), containsString("missing_own")));
        configured().statementError("""
            SEQUENCE BY key WITHIN 30s PRIMARY 2 (FROM a),
            (FROM b | WHERE label != "skip" | WHERE label == MEMBER(1,missing_member)) | KEEP key | LIMIT 10
            """, containsString("final output column [missing_member]"));
    }

    /** MEMBER bindings live in the left input even when they are used after one or more ordinary lookup joins. */
    public void testLookupJoinAfterSequencePreservesMemberBinding() {
        var lookup = new EsIndex(
            "supporting",
            Map.of(
                "key",
                new EsField("key", DataType.KEYWORD, Map.of(), true, NONE),
                "indicator",
                new EsField("indicator", DataType.KEYWORD, Map.of(), true, NONE)
            ),
            Map.of("supporting", new IndexProperties(IndexMode.LOOKUP, 0)),
            Map.of(),
            Map.of()
        );
        LogicalPlan plan = configured().addLookupIndex(IndexResolution.valid(lookup)).statement("""
            SEQUENCE BY key WITHIN 30s PRIMARY 2 (FROM a),(FROM b)
            | LOOKUP JOIN supporting ON key
            | EVAL earlier=MEMBER(1,label) | KEEP key,label,earlier,indicator | LIMIT 10
            """);
        assertEquals(List.of("key", "label", "earlier", "indicator"), plan.output().stream().map(Attribute::name).toList());
        assertEquals(plan.output(), optimizer().optimize(plan).output());
    }

    public void testEarlierPrimaryConditionPreservesOrdinaryBindings() {
        List<String> expected = List.of("key", "label");
        for (String stage : List.of(
            "FROM b | WHERE label != MEMBER(issued,label)",
            "FROM b | EVAL own=label | WHERE own != MEMBER(issued,label) | KEEP key,label | RENAME label AS renamed | DROP renamed"
        )) {
            var parsed = EsqlTestUtils.TEST_PARSER.createStatement("""
                SEQUENCE BY key WITHIN 30s PRIMARY issued
                (FROM a | KEEP key,label) AS issued, (%s) AS signed
                | KEEP key,label | LIMIT 10
                """.formatted(stage));
            LogicalPlan analyzed = configured().buildAnalyzer().analyze(parsed.plan());
            LogicalPlan optimized = optimizer().optimize(analyzed);
            for (LogicalPlan plan : List.of(analyzed, optimized)) {
                assertEquals(expected, plan.output().stream().map(Attribute::name).toList());
                assertEquals(expected.size(), plan.output().stream().map(Attribute::id).distinct().count());
                Sequence sequence = plan.collect(Sequence.class).getFirst();
                SequenceCondition condition = sequence.collect(SequenceCondition.class).getFirst();
                SequenceCondition.Input earlier = condition.activeInputs()
                    .stream()
                    .filter(input -> input.stage() == 0)
                    .findFirst()
                    .orElseThrow();
                assertEquals("label", earlier.source().name());
                assertFalse(earlier.source().synthetic());
                Attribute primary = plan.output().stream().filter(attribute -> attribute.name().equals("label")).findFirst().orElseThrow();
                int primaryColumn = sequenceColumn(plan, sequence, primary);
                assertEquals(Integer.valueOf(0), sequence.sourceStages().get(primaryColumn));
                assertEquals("label", sequence.sourceAttributes().get(primaryColumn).name());
                assertFalse(sequence.sourceAttributes().get(primaryColumn).synthetic());
            }
        }
    }

    public void testOriginalSchedulingSetSurvivesScalarFoldingWithoutKeepingDeadValues() {
        LogicalPlan plan = configured().statement("""
            SEQUENCE BY key WITHIN 30s PRIMARY 2
            (FROM a | KEEP key,flag),(FROM b | WHERE true OR MEMBER(1,flag))
            | KEEP key | LIMIT 10
            """);
        Sequence sequence = optimizer().optimize(plan).collect(Sequence.class).getFirst();
        SequenceCondition condition = sequence.collect(SequenceCondition.class).getFirst();
        assertEquals(List.of(0), condition.referencedStages());
        assertEquals(Boolean.TRUE, condition.condition().fold(FoldContext.small()));
        assertTrue(condition.activeInputs().isEmpty());
    }

    public void testDiscoveryAndCanonicalBindingsAcrossAnEarlierMultiIndexBranch() {
        String query = """
            SEQUENCE BY key WITHIN 30s PRIMARY 2
            (FROM (FROM a | EVAL computed=TO_UPPER(label) | KEEP key,computed),
                  (FROM c | EVAL computed=TO_UPPER(label) | KEEP key,computed) | RENAME computed AS visible),
            (FROM b | WHERE label == MEMBER(1,visible) AND key == MEMBER(1,key))
            | KEEP key | LIMIT 10
            """;
        var parsed = EsqlTestUtils.TEST_PARSER.createStatement(query);
        Set<String> fields = FieldNameUtils.resolveFieldNames(parsed.plan(), false, false).fieldNames();
        assertTrue(fields.contains("label"));
        assertTrue(fields.contains("key"));
        assertTrue(fields.stream().noneMatch(name -> name.startsWith("MEMBER(")));
        Sequence sequence = optimizer().optimize(configured().statement(query)).collect(Sequence.class).getFirst();
        SequenceStage earlier = (SequenceStage) sequence.children().getFirst();
        for (SequenceCondition condition : sequence.collect(SequenceCondition.class)) {
            condition.activeInputs().stream().filter(input -> input.stage() == 0).forEach(input -> {
                assertEquals(
                    1,
                    earlier.output()
                        .stream()
                        .filter(a -> a.id().equals(input.source().id()) && a.dataType() == input.source().dataType())
                        .count()
                );
            });
        }
    }

    public void testConditionMustBeBooleanAndCannotUseFullTextOrScoring() {
        configured().statementError("""
            SEQUENCE WITHIN 30s PRIMARY 2 (FROM a),(FROM b | WHERE MEMBER(1,number))
            | LIMIT 10
            """, containsString("Condition expression needs to be boolean"));
        for (String predicate : List.of("MATCH(label,\"term\")", "SCORE(MATCH(label,\"term\")) > 0")) {
            configured().statementError("""
                SEQUENCE WITHIN 30s PRIMARY 2
                (FROM a),(FROM b | WHERE MEMBER(1,flag) OR %s)
                | LIMIT 10
                """.formatted(predicate), containsString("SEQUENCE MEMBER condition"));
        }
    }

    /** Cross-stage comparisons use the same scalar type rules as ordinary WHERE. */
    public void testCrossStageComparisonTypes() {
        configured().statementError("""
            SEQUENCE WITHIN 30s PRIMARY 2 (FROM a),(FROM b | WHERE label == MEMBER(1,number))
            | KEEP key | LIMIT 10
            """, allOf(containsString("keyword"), containsString("long")));
        Sequence sequence = analyzed("""
            SEQUENCE WITHIN 30s PRIMARY 2
            (FROM a | EVAL nanos = TO_DATE_NANOS(@timestamp)),(FROM b | WHERE @timestamp == MEMBER(1,nanos))
            | KEEP key | LIMIT 10
            """);
        assertTrue(sequence.collect(SequenceCondition.class).getFirst().expressionsResolved());
    }

    public void testConditionOnlySourceTypesAreVerifiedAfterPruning() {
        var unsupported = analyzer().minimumTransportVersion(TransportVersion.current());
        Map<String, EsField> fields = Map.of(
            "@timestamp",
            DateEsField.dateEsField("@timestamp", Map.of(), true, NONE),
            "key",
            new EsField("key", DataType.KEYWORD, Map.of(), true, NONE),
            "flag",
            new EsField("flag", DataType.BOOLEAN, Map.of(), true, NONE),
            "shape",
            new EsField("shape", DataType.GEO_SHAPE, Map.of(), true, NONE)
        );
        for (String name : List.of("a", "b")) {
            unsupported.addIndex(new EsIndex(name, fields, Map.of(name, new IndexProperties(IndexMode.STANDARD, 0)), Map.of(), Map.of()));
        }
        for (String stages : List.of(
            "(FROM a | KEEP key,shape),(FROM b | WHERE MEMBER(1,shape) IS NULL)",
            "(FROM a | KEEP key,flag),(FROM b | WHERE MEMBER(1,flag) OR shape IS NULL | DROP shape)"
        )) {
            String query = "SEQUENCE BY key WITHIN 30s PRIMARY 2 " + stages + " | KEEP key | LIMIT 10";
            Sequence point = optimizer().optimize(configured().statement(query)).collect(Sequence.class).getFirst();
            assertEquals(List.of("key"), point.output().stream().map(Attribute::name).toList());
            assertTrue(
                point.collect(SequenceCondition.class)
                    .stream()
                    .flatMap(condition -> condition.activeInputs().stream())
                    .anyMatch(input -> input.source().name().equals("shape") && input.source().dataType() == DataType.GEO_POINT)
            );
            LogicalPlan plan = unsupported.statement(query);
            assertThat(
                expectThrows(VerificationException.class, () -> optimizer().optimize(plan)).getMessage(),
                containsString("cannot read source column [shape]")
            );
        }
    }

    private Sequence analyzed(String query) {
        return configured().statement(query).collect(Sequence.class).getFirst();
    }

    // Follow actual alias identities through presentation and capture, rather than inferring a role from a stage's SOURCE count.
    private static int sequenceColumn(LogicalPlan plan, Sequence sequence, Attribute published) {
        List<Alias> aliases = new ArrayList<>();
        plan.forEachExpressionDown(Alias.class, aliases::add);
        Set<NameId> visited = new HashSet<>();
        Attribute current = published;
        while (visited.add(current.id())) {
            for (int column = 0; column < sequence.output().size(); column++) {
                if (sequence.output().get(column).id().equals(current.id())) {
                    return column;
                }
            }
            NameId id = current.id();
            List<Attribute> inputs = aliases.stream()
                .filter(alias -> alias.id().equals(id))
                .map(alias -> Alias.unwrap(alias.child()))
                .filter(Attribute.class::isInstance)
                .map(Attribute.class::cast)
                .toList();
            assertFalse("missing alias for published binding " + published.name(), inputs.isEmpty());
            assertEquals(1, inputs.stream().map(Attribute::id).distinct().count());
            current = inputs.getFirst();
        }
        throw new AssertionError("cyclic published binding " + published.name());
    }

    private static LogicalPlanOptimizer optimizer() {
        return new LogicalPlanOptimizer(
            EsqlTestUtils.logicalOptimizerContext(EsqlTestUtils.TEST_CFG, FoldContext.small(), TransportVersion.current())
        );
    }

    private static TestAnalyzer configured() {
        var analyzer = analyzer().minimumTransportVersion(TransportVersion.current());
        Map<String, EsField> fields = Map.of(
            "@timestamp",
            DateEsField.dateEsField("@timestamp", Map.of(), true, NONE),
            "key",
            new EsField("key", DataType.KEYWORD, Map.of(), true, NONE),
            "label",
            new EsField("label", DataType.KEYWORD, Map.of(), true, NONE),
            "number",
            new EsField("number", DataType.LONG, Map.of(), true, NONE),
            "flag",
            new EsField("flag", DataType.BOOLEAN, Map.of(), true, NONE),
            "shape",
            new EsField("shape", DataType.GEO_POINT, Map.of(), true, NONE)
        );
        for (String name : List.of("a", "b", "c")) {
            analyzer.addIndex(new EsIndex(name, fields, Map.of(name, new IndexProperties(IndexMode.STANDARD, 0)), Map.of(), Map.of()));
        }
        return analyzer;
    }
}
