/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.TestAnalyzer;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.capabilities.TranslationAware;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.CompactMultiTypeEsField;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.DateEsField;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.expression.function.fulltext.Match;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToLong;
import org.elasticsearch.xpack.esql.expression.function.vector.Knn;
import org.elasticsearch.xpack.esql.index.EsIndex;
import org.elasticsearch.xpack.esql.index.IndexProperties;
import org.elasticsearch.xpack.esql.optimizer.LogicalPlanOptimizer;
import org.elasticsearch.xpack.esql.optimizer.rules.physical.local.LucenePushdownPredicates;
import org.elasticsearch.xpack.esql.plan.EsqlStatement;
import org.elasticsearch.xpack.esql.plan.QuerySettings;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.RegexExtract;
import org.elasticsearch.xpack.esql.plan.logical.Sequence;
import org.elasticsearch.xpack.esql.plan.logical.SequenceMatchOutput;
import org.elasticsearch.xpack.esql.plan.logical.SequenceStage;
import org.elasticsearch.xpack.esql.plan.logical.SequenceStageProjection;
import org.elasticsearch.xpack.esql.plan.logical.TopN;
import org.elasticsearch.xpack.esql.plan.logical.TopNBy;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;
import org.junit.BeforeClass;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.analyzer;
import static org.elasticsearch.xpack.esql.core.type.EsField.TimeSeriesFieldType.NONE;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;

/** Verifies independent stage scopes, the wide schema, and candidate completeness boundaries. */
public class SequenceAnalyzerTests extends ESTestCase {
    /** These analyzer fixtures parse the command using the running build's default parser. */
    @BeforeClass
    public static void requireSequenceCommand() {
        assumeTrue("requires native SEQUENCE", EsqlCapabilities.Cap.SEQUENCE_COMMAND.isEnabled());
    }

    /** Stage aliases lower before field discovery; they can share ordinary field names without changing bindings. */
    public void testNamedStagesPreservePositionalSchemaAndDemands() {
        String numeric = """
            SEQUENCE BY host.id WITHIN 30s PRIMARY 2
            (FROM a | KEEP host.id, label), (FROM b | KEEP host.id, label)
            | EVAL from_name = MEMBER(1, label), from_number = MEMBER(1, label)
            | KEEP host.id, label, from_name, from_number | LIMIT 1
            """;
        String named = numeric.replace("PRIMARY 2", "PRIMARY execution")
            .replace("(FROM a | KEEP host.id, label)", "(FROM a | KEEP host.id, label) AS label")
            .replace("(FROM b | KEEP host.id, label)", "(FROM b | KEEP host.id, label) AS execution")
            .replace("from_name = MEMBER(1, label)", "from_name = MEMBER(label, label)");
        LogicalPlan numbered = configured().statement(numeric);
        LogicalPlan aliased = configured().statement(named);
        assertEquals(numbered.output().stream().map(Attribute::name).toList(), aliased.output().stream().map(Attribute::name).toList());
        assertEquals(
            numbered.output().stream().map(Attribute::dataType).toList(),
            aliased.output().stream().map(Attribute::dataType).toList()
        );
        Sequence numberedSequence = numbered.collect(Sequence.class).getFirst();
        Sequence namedSequence = aliased.collect(Sequence.class).getFirst();
        assertEquals(numberedSequence.sourceStages(), namedSequence.sourceStages());
        assertEquals(
            numberedSequence.sourceAttributes().stream().map(Attribute::dataType).toList(),
            namedSequence.sourceAttributes().stream().map(Attribute::dataType).toList()
        );
        assertEquals(numberedSequence.maxSpanNanos(), namedSequence.maxSpanNanos());
        assertTrue(aliased.collect(SequenceMatchOutput.Declaration.class).isEmpty());
        assertTrue(aliased.collect(SequenceMatchOutput.Scope.class).isEmpty());
    }

    /** Reserve MEMBER names, including occupied fallback suffixes, before outer user edits. */
    public void testQuotedTemporaryBindingsSurviveOuterEdits() throws Exception {
        for (int primary : List.of(1, 2)) {
            for (String edit : List.of("EVAL `placeholder` = \"ordinary\"", "EVAL value = \"ordinary\" | RENAME value AS `placeholder`")) {
                EsqlStatement parsed = quotedCollision("""
                    SEQUENCE BY host.id WITHIN 30s PRIMARY %s
                    (FROM a | KEEP host.id, label), (FROM b | KEEP host.id, label)
                    | %s
                    | EVAL `placeholder_1` = "reserved", `placeholder_2` = "reserved", ordinary = MEMBER(%s, label)
                    | KEEP `placeholder`, ordinary | LIMIT 1
                    """.formatted(primary, edit, 3 - primary));
                LogicalPlan analyzed = configured().buildAnalyzer().analyze(parsed.plan());
                assertEquals(List.of(quotedName(parsed), "ordinary"), analyzed.output().stream().map(Attribute::name).toList());
                assertEquals(2, analyzed.output().stream().map(Attribute::id).distinct().count());
            }
        }
    }

    /** Extracted keys resolve independently, while typed payloads survive pruning and MEMBER publication. */
    public void testExtractionKeysAndTypedPayload() {
        for (String extraction : List.of(
            "DISSECT label \"%{parsed_key}:%{payload}:%{unused}\"",
            "GROK label \"%{WORD:parsed_key}:%{INT:payload:int}:%{WORD:unused}\""
        )) {
            for (int primary : List.of(1, 2)) {
                String query = """
                    SEQUENCE BY parsed_key WITHIN 30s PRIMARY %s
                    (FROM a | %s | RENAME payload AS parsed_value | DROP unused | KEEP parsed_key, parsed_value),
                    (FROM b | %s | RENAME payload AS parsed_value | DROP unused | KEEP parsed_key, parsed_value)
                    | EVAL other_value = MEMBER(%s, parsed_value)
                    | KEEP parsed_key, parsed_value, other_value | LIMIT 1
                    """.formatted(primary, extraction, extraction, 3 - primary);
                assertEquals(
                    List.of("parsed_key", "parsed_value", "other_value"),
                    configured().statement(query).output().stream().map(Attribute::name).toList()
                );
                LogicalPlan analyzed = configured().statement(query);
                DataType payloadType = extraction.startsWith("GROK") ? DataType.INTEGER : DataType.KEYWORD;
                assertEquals(
                    List.of(DataType.KEYWORD, payloadType, payloadType),
                    analyzed.output().subList(0, 3).stream().map(Attribute::dataType).toList()
                );
                LogicalPlan optimized = optimizer().optimize(analyzed);
                assertEquals(analyzed.output(), optimized.output());
                assertEquals(2, optimized.collect(RegexExtract.class).size());
                Sequence sequence = optimized.collect(Sequence.class).getFirst();
                assertTrue(sequence.sourceAttributes().stream().noneMatch(attribute -> attribute.name().equals("unused")));
                assertEquals(0L, sequence.sourceAttributes().stream().filter(a -> a.dataType() == DataType.SOURCE).count());
            }
        }
    }

    /** A nullified expansion input keeps the ordinary schema. */
    public void testNullifyExpandedOrdinaryPrimaryOne() {
        assertNullifyExpandedBindings(1);
    }

    /** The second primary has the same ordinary null-typed expansion contract. */
    public void testNullifyExpandedOrdinaryPrimaryTwo() {
        assertNullifyExpandedBindings(2);
    }

    /** LOAD must report the member's missing field rather than trying to load the MEMBER display spelling. */
    public void testLoadMissingMemberHasScopedDiagnostic() {
        for (int primary : List.of(1, 2)) {
            int other = 3 - primary;
            configured().statementError(
                """
                    SET unmapped_fields="load";
                    SEQUENCE BY host.id WITHIN 1m PRIMARY %s (FROM a), (FROM b)
                    | EVAL missing = MEMBER(%s, absent_optional) | KEEP missing | LIMIT 10
                    """.formatted(primary, other),
                containsString(
                    "Cannot load missing field [absent_optional] for SEQUENCE member ["
                        + other
                        + "]; use a mapped or stage-produced MEMBER field"
                )
            );
        }
    }

    /** Ordinary LOAD analysis and mapped or stage-produced MEMBER fields retain their existing behavior. */
    public void testLoadPreservesOrdinaryAndStageProducedMemberFields() {
        var ordinary = configured().statement("""
            SET unmapped_fields="load";
            FROM a | EVAL ordinary=absent_optional | KEEP ordinary | LIMIT 10
            """);
        assertEquals(List.of("ordinary"), ordinary.output().stream().map(Attribute::name).toList());
        assertEquals(List.of(DataType.KEYWORD), ordinary.output().stream().map(Attribute::dataType).toList());
        for (int primary : List.of(1, 2)) {
            int other = 3 - primary;
            for (String memberField : List.of("label", "stage_value")) {
                var analyzed = configured().statement("""
                    SET unmapped_fields="load";
                    SEQUENCE BY host.id WITHIN 1m PRIMARY %s
                    (FROM a | EVAL stage_value=label), (FROM b | EVAL stage_value=label)
                    | EVAL other=MEMBER(%s, %s) | KEEP other | LIMIT 10
                    """.formatted(primary, other, memberField));
                assertEquals(List.of("other"), analyzed.output().stream().map(Attribute::name).toList());
                assertEquals(List.of(DataType.KEYWORD), analyzed.output().stream().map(Attribute::dataType).toList());
            }
        }
    }

    private void assertNullifyExpandedBindings(int primary) {
        String query = """
            SET unmapped_fields="nullify";
            SEQUENCE BY host.id WITHIN 1m PRIMARY %s
            (FROM a | EVAL pick=absent_optional | MV_EXPAND pick | KEEP host.id,pick),
            (FROM b | EVAL pick=absent_optional | MV_EXPAND pick | KEEP host.id,pick)
            | EVAL other=MEMBER(%s,pick) | KEEP host.id,pick,other | SORT host.id | LIMIT 10
            """.formatted(primary, 3 - primary);
        EsqlStatement parsed = EsqlTestUtils.TEST_PARSER.createStatement(query);
        LogicalPlan analyzed = configured().unmappedResolution(parsed.setting(QuerySettings.UNMAPPED_FIELDS))
            .buildAnalyzer()
            .analyze(parsed.plan());
        assertEquals(List.of("host.id", "pick", "other"), analyzed.output().stream().map(Attribute::name).toList());
        assertEquals(List.of(DataType.KEYWORD, DataType.NULL, DataType.NULL), analyzed.output().stream().map(Attribute::dataType).toList());
        assertEquals(analyzed.output().size(), analyzed.output().stream().map(Attribute::id).distinct().count());
        assertTrue(analyzed.collect(SequenceMatchOutput.Scope.class).isEmpty());
        LogicalPlan optimized = optimizer().optimize(analyzed);
        assertEquals(analyzed.output(), optimized.output());
        assertEquals(
            0L,
            optimized.collect(Sequence.class).getFirst().sourceAttributes().stream().filter(a -> a.dataType() == DataType.SOURCE).count()
        );
    }

    /** Metadata-shaped stage captures remain ordinary processed PRIMARY and MEMBER values. */
    public void testExtractedMetadataNamesRemainOrdinary() {
        for (String extraction : List.of(
            "DISSECT label \"%{_id}:%{_index}:%{_source}\"",
            "GROK label \"%{WORD:_id}:%{WORD:_index}:%{WORD:_source}\""
        )) {
            for (int primary : List.of(1, 2)) {
                LogicalPlan analyzed = configured().statement("""
                    SEQUENCE BY host.id WITHIN 30s PRIMARY %s
                    (FROM a | %s), (FROM b | %s)
                    | EVAL other_id=MEMBER(%s,_id), other_index=MEMBER(%s,_index), other_source=MEMBER(%s,_source)
                    | KEEP _id,_index,_source,other_id,other_index,other_source | LIMIT 1
                    """.formatted(primary, extraction, extraction, 3 - primary, 3 - primary, 3 - primary));
                assertEquals(
                    List.of("_id", "_index", "_source", "other_id", "other_index", "other_source"),
                    analyzed.output().stream().map(Attribute::name).toList()
                );
                assertTrue(analyzed.output().stream().allMatch(attribute -> attribute.dataType() == DataType.KEYWORD));
                assertEquals(analyzed.output().size(), analyzed.output().stream().map(Attribute::id).distinct().count());
            }
        }
    }

    /** Extraction cannot replace the source timestamp needed by the matcher. */
    public void testExtractionCannotOverwriteTimestamp() {
        for (String extraction : List.of("DISSECT label \"%{@timestamp}\"", "GROK label \"%{WORD:@timestamp}\"")) {
            configured().statementError("""
                SEQUENCE BY host.id WITHIN 30s PRIMARY 1
                (FROM a | %s), (FROM b)
                | KEEP host.id | LIMIT 1
                """.formatted(extraction), containsString("cannot overwrite the source [@timestamp]"));
        }
    }

    /** An extraction whose processed outputs are all unused is pruned. */
    public void testUnusedExtractionIsPruned() {
        for (String extraction : List.of("DISSECT label \"%{unused}\"", "GROK label \"%{WORD:unused}\"")) {
            LogicalPlan analyzed = configured().statement("""
                SEQUENCE BY host.id WITHIN 30s PRIMARY 2
                (FROM a | %s), (FROM b | %s)
                | KEEP host.id | LIMIT 1
                """.formatted(extraction, extraction));
            LogicalPlan optimized = optimizer().optimize(analyzed);
            assertEquals(analyzed.output(), optimized.output());
            assertTrue(optimized.collect(RegexExtract.class).isEmpty());
            assertEquals(
                0L,
                optimized.collect(Sequence.class)
                    .getFirst()
                    .sourceAttributes()
                    .stream()
                    .filter(a -> a.dataType() == DataType.SOURCE)
                    .count()
            );
        }
    }

    /** The generated match column is an ordinary projection, separate from user metadata-shaped values. */
    public void testProvenanceIsOrdinaryAndCollisionSafe() {
        for (int primary : List.of(1, 2)) {
            String base = """
                SEQUENCE BY host.id WITHIN 30s PRIMARY %s
                (FROM a | KEEP host.id,label), (FROM b | KEEP host.id,label)
                """.formatted(primary);
            LogicalPlan kept = configured().statement(base + " | KEEP sequence_provenance | RENAME sequence_provenance AS events");
            assertEquals(List.of("events"), kept.output().stream().map(Attribute::name).toList());
            assertEquals(DataType.KEYWORD, kept.output().getFirst().dataType());
            Sequence sequence = kept.collect(Sequence.class).getFirst();
            List<Attribute> markers = sequence.sourceAttributes().stream().filter(Sequence::isProvenanceAttribute).toList();
            assertEquals(1, markers.size());
            assertFalse(sequence.references().contains(markers.getFirst()));
            assertEquals(1, sequence.resolveSchema().sourceAttributes().stream().filter(Sequence::isProvenanceAttribute).count());
            assertEquals(sequence.output(), sequence.resolveSchema().output());
            LogicalPlan dropped = optimizer().optimize(configured().statement(base + " | DROP sequence_provenance | KEEP label"));
            assertTrue(dropped.collect(Sequence.class).getFirst().sourceAttributes().stream().noneMatch(Sequence::isProvenanceAttribute));
            LogicalPlan replaced = optimizer().optimize(
                configured().statement(base + " | EVAL sequence_provenance=\"ordinary\" | KEEP sequence_provenance")
            );
            assertTrue(replaced.collect(Sequence.class).getFirst().sourceAttributes().stream().noneMatch(Sequence::isProvenanceAttribute));
            String collisions = base.replace(
                "KEEP host.id,label",
                "EVAL sequence_provenance=\"caller\", sequence_provenance_1=\"reserved\", _sequence_provenance=\"private-name-caller\" | KEEP host.id,label,sequence_provenance,sequence_provenance_1,_sequence_provenance"
            );
            LogicalPlan collision = configured().statement(
                collisions + " | KEEP sequence_provenance,sequence_provenance_1,_sequence_provenance,sequence_provenance_2"
            );
            assertEquals(
                List.of("sequence_provenance", "sequence_provenance_1", "_sequence_provenance", "sequence_provenance_2"),
                collision.output().stream().map(Attribute::name).toList()
            );
            Sequence owned = collision.collect(Sequence.class).getFirst();
            assertEquals(1, owned.sourceAttributes().stream().filter(Sequence::isProvenanceAttribute).count());
            assertFalse(
                Sequence.isProvenanceAttribute(
                    new org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute(
                        Source.EMPTY,
                        null,
                        "_sequence_provenance",
                        DataType.KEYWORD
                    )
                )
            );
            assertFalse(
                Sequence.isProvenanceAttribute(
                    new org.elasticsearch.xpack.esql.core.expression.MetadataAttribute(
                        Source.EMPTY,
                        "_sequence_provenance",
                        DataType.KEYWORD,
                        false
                    )
                )
            );
        }
        assertWarnings("No limit defined, adding default limit of [1000]");
    }

    /** Calibrate a real process-local MEMBER identity without resetting or replacing the ID generator. */
    private static EsqlStatement quotedCollision(String template) throws Exception {
        var parser = EsqlTestUtils.TEST_PARSER;
        var counterField = org.elasticsearch.xpack.esql.core.expression.NameId.class.getDeclaredField("COUNTER");
        counterField.setAccessible(true);
        AtomicLong counter = (AtomicLong) counterField.get(null);
        parser.createStatement(template);
        long before = counter.get();
        EsqlStatement calibration = parser.createStatement(template);
        long offset = bindingId(calibration.plan()) - before;
        String name = Attribute.rawTemporaryName("sequence_member", Long.toString(counter.get() + offset));
        EsqlStatement parsed = parser.createStatement(template.replace("placeholder", name));
        assertEquals(name, quotedName(parsed));
        return parsed;
    }

    private static String quotedName(EsqlStatement parsed) {
        return Attribute.rawTemporaryName("sequence_member", Long.toString(bindingId(parsed.plan())));
    }

    /** Read the compiler-created binding rather than introducing test-only IDs into a plan. */
    private static long bindingId(LogicalPlan parsed) {
        var references = new ArrayList<SequenceMatchOutput.MemberReference>();
        parsed.forEachExpressionDown(SequenceMatchOutput.MemberReference.class, references::add);
        assertFalse(references.isEmpty());
        assertEquals(1, references.stream().map(Attribute::id).distinct().count());
        return Long.parseLong(references.getFirst().id().toString());
    }

    /** The ordinary schema prefix is unchanged; one exact compiler-only marker has its own stable binding IDs. */
    private static List<Attribute> ordinarySequenceOutput(Sequence sequence, int ordinaryColumns) {
        assertEquals(ordinaryColumns + 1, sequence.output().size());
        assertEquals(ordinaryColumns + 1, sequence.sourceAttributes().size());
        assertEquals(ordinaryColumns + 1, sequence.sourceStages().size());
        Attribute marker = sequence.sourceAttributes().getLast();
        assertEquals(MetadataAttribute.class, marker.getClass());
        assertEquals("_sequence_provenance", marker.name());
        assertTrue(marker.synthetic());
        assertEquals(DataType.KEYWORD, marker.dataType());
        assertTrue(Sequence.isProvenanceAttribute(marker));
        assertEquals(1, sequence.sourceAttributes().stream().filter(Sequence::isProvenanceAttribute).count());
        assertEquals(Integer.valueOf(0), sequence.sourceStages().getLast());
        Attribute output = sequence.output().getLast();
        assertEquals(ReferenceAttribute.class, output.getClass());
        assertFalse(output.synthetic());
        assertEquals(DataType.KEYWORD, output.dataType());
        assertNotEquals(marker.id(), output.id());
        assertEquals(marker.id(), sequence.resolveSchema().sourceAttributes().getLast().id());
        assertEquals(output.id(), sequence.resolveSchema().output().getLast().id());
        return sequence.output().subList(0, ordinaryColumns);
    }

    public void testTypedPrimaryOutputAndPostSequenceMemberFilter() {
        var plan = configured().statement("""
            sequence by host.id WITHIN 30s PRIMARY 1
            (from a | eval label = "network" | keep host.id, @timestamp, label),
            (from b | eval label = "process" | keep host.id, @timestamp, label)
            | where MEMBER(2, @timestamp) >= @timestamp
            | eval second_label = MEMBER(2, label)
            """);
        Sequence sequence = plan.collect(Sequence.class).getFirst();
        List<Attribute> ordinary = ordinarySequenceOutput(sequence, 5);
        assertEquals(
            List.of("host.id", "stage1.@timestamp", "stage1.label", "stage2.@timestamp", "stage2.label"),
            ordinary.stream().map(Attribute::name).toList()
        );
        assertEquals(
            List.of(DataType.KEYWORD, DataType.DATETIME, DataType.KEYWORD, DataType.DATETIME, DataType.KEYWORD),
            ordinary.stream().map(Attribute::dataType).toList()
        );
        assertEquals(
            List.of("host.id", "@timestamp", "label", "sequence_provenance", "second_label"),
            plan.output().stream().map(Attribute::name).toList()
        );
        assertTrue(sequence.childrenResolved());
        assertEquals(1, plan.collect(Limit.class).size());
        for (var stage : sequence.children()) {
            assertTrue(stage.collect(Limit.class).isEmpty());
        }
        assertSame(sequence, sequence.resolveSchema());
        assertEquals(sequence.output(), sequence.replaceChildren(sequence.children()).output());
        assertWarnings("No limit defined, adding default limit of [1000]");
    }

    /** Generated internal stage names are not query aliases after PRIMARY establishes the ordinary field context. */
    public void testLegacyStageQualifiedFieldsAreNotPublicAliases() {
        for (int primary : List.of(1, 2)) {
            String query = """
                SEQUENCE BY host.id WITHIN 30s PRIMARY %s
                (FROM a | KEEP host.id, label), (FROM b | KEEP host.id, label)
                """.formatted(primary);
            var plan = configured().statement(query + " | LIMIT 1");
            assertEquals(List.of("host.id", "label", "sequence_provenance"), plan.output().stream().map(Attribute::name).toList());
            ordinarySequenceOutput(plan.collect(Sequence.class).getFirst(), 3);
            configured().statementError(query + " | KEEP stage1.label | LIMIT 1", containsString("Unknown column [stage1.label]"));
            configured().statementError(query + " | KEEP stage2.label | LIMIT 1", containsString("Unknown column [stage2.label]"));
        }
    }

    /** A real mapped primary field remains usable even when its name resembles the retired generated spelling. */
    public void testStageNamedMappedFieldRemainsOrdinaryPrimaryField() {
        var fields = new HashMap<>(mapping());
        fields.put(
            "stage1",
            new EsField(
                "stage1",
                DataType.OBJECT,
                Map.of("label", new EsField("label", DataType.KEYWORD, Map.of(), true, NONE)),
                false,
                NONE
            )
        );
        var plan = configured().addIndex(index("named_events", fields)).statement("""
            SEQUENCE BY host.id WITHIN 30s PRIMARY 1
            (FROM named_events), (FROM b)
            | KEEP stage1.label | LIMIT 1
            """);
        assertEquals(List.of("stage1.label"), plan.output().stream().map(Attribute::name).toList());
    }

    public void testDerivedExpandedKeysResolveInEachStage() {
        var plan = configured().statement("""
            sequence by commonkey WITHIN 2m PRIMARY 1
            (from a | eval commonkey = TO_LOWER(host.id) | mv_expand commonkey | keep commonkey, label),
            (from b | eval commonkey = TO_LOWER(label) | mv_expand commonkey | keep commonkey, label)
            | eval second_label = MEMBER(2, label)
            | keep commonkey, label, second_label
            | limit 4
            """);
        Sequence sequence = plan.collect(Sequence.class).getFirst();
        var first = (SequenceStage) sequence.children().getFirst();
        var second = (SequenceStage) sequence.children().get(1);
        assertTrue(first.keys().getFirst().resolved());
        assertTrue(second.keys().getFirst().resolved());
        assertNotEquals(first.keys().getFirst().id(), second.keys().getFirst().id());
        assertEquals(
            List.of("commonkey", "stage1.label", "stage2.label"),
            ordinarySequenceOutput(sequence, 3).stream().map(Attribute::name).toList()
        );
        assertTrue(first.child().collect(Limit.class).isEmpty());
        assertTrue(second.child().collect(Limit.class).isEmpty());
    }

    public void testKeysMustSurviveProjection() {
        configured().statementError("""
            sequence by host.id WITHIN 30s PRIMARY 1
            (from a | keep label), (from b | keep host.id)
            """, containsString("Unknown column [host.id]"));
        assertWarnings("No limit defined, adding default limit of [1000]");
    }

    /** MEMBER sees the stage's final schema: RENAME replaces the old spelling and EVAL can publish a new column. */
    public void testMemberUsesFinalStageColumnNames() {
        String query = """
            SEQUENCE BY host.id WITHIN 30 seconds PRIMARY execution
            (FROM a | RENAME label AS actor | EVAL normalized = TO_LOWER(actor) | KEEP host.id, actor, normalized) AS download,
            (FROM b | KEEP host.id, label) AS execution
            | EVAL observed = MEMBER(download, actor), derived = MEMBER(download, normalized)
            | KEEP host.id, observed, derived | LIMIT 10
            """;
        LogicalPlan analyzed = configured().statement(query);
        LogicalPlan optimized = optimizer().optimize(analyzed);
        assertEquals(List.of("host.id", "observed", "derived"), optimized.output().stream().map(Attribute::name).toList());
        assertEquals(
            List.of(DataType.KEYWORD, DataType.KEYWORD, DataType.KEYWORD),
            optimized.output().stream().map(Attribute::dataType).toList()
        );
        configured().statementError(
            query.replace("MEMBER(download, actor)", "MEMBER(download, label)"),
            allOf(containsString("Unknown column"), containsString("label"))
        );
    }

    /** An explicit projection is a schema boundary even when a mapped source field could otherwise be loaded. */
    public void testMemberCannotReadFieldRemovedFromNamedStage() {
        for (String projection : List.of("KEEP host.id", "DROP label")) {
            configured().statementError("""
                SEQUENCE BY host.id WITHIN 30 seconds PRIMARY execution
                (FROM a | %s) AS download, (FROM b | KEEP host.id, label) AS execution
                | EVAL observed = MEMBER(download, label) | KEEP host.id, observed | LIMIT 10
                """.formatted(projection), allOf(containsString("Unknown column"), containsString("label")));
        }
    }

    public void testExactSourceSearchConjunctsAreAccepted() {
        for (String predicate : List.of(
            "MATCH(label, \"term\")",
            "MATCH(label, \"term\", {\"boost\": 2.0})",
            "label : \"term\"",
            "MATCH_PHRASE(label, \"term\")",
            "QSTR(\"label:term\")",
            "KQL(\"label:term\")",
            "KQL(\"label:term or not label:other\")"
        )) {
            assertNotNull(configured().statement(Strings.format("""
                sequence by host.id WITHIN 30s PRIMARY 1
                (from a | where %s AND label != "skip" | keep host.id), (from b | keep host.id) | limit 1
                """, predicate)));
        }
    }

    public void testIndexedSearchOutsideExactSourceConjunctIsRejected() {
        for (String commands : List.of(
            "WHERE KQL(\"label:term\") OR label == \"other\"",
            "WHERE NOT KQL(\"label:term\")",
            "WHERE KQL(\"label:term\") AND (KQL(\"label:term\") OR label == \"other\")",
            "EVAL copy=label | WHERE MATCH(label, \"term\")",
            "EVAL copy=label | WHERE KQL(\"label:term\")",
            "MV_EXPAND label | WHERE KQL(\"label:term\")"
        )) {
            configured().statementError(Strings.format("""
                sequence by host.id WITHIN 30s PRIMARY 1
                (from a | %s | keep host.id), (from b | keep host.id) | limit 1
                """, commands), allOf(containsString("exact top-level AND term"), containsString("OR or NOT outside the search string")));
        }
        configured().statementError("""
            sequence by host.id WITHIN 30s PRIMARY 1 (from a),
            (from b | where KQL("label:term") OR label == MEMBER(1,label)) | limit 1
            """, allOf(containsString("Indexed search function [KQL]"), containsString("OR or NOT outside the search string")));
    }

    /** Runtime MATCH already works over computed values and remains independent of the indexed-search gate. */
    public void testRuntimeSearchAfterStageTransformsRemainsAccepted() {
        for (String commands : List.of(
            "EVAL searched=label | WHERE MATCH(searched, \"term\")",
            "EVAL searched=label | MV_EXPAND searched | WHERE MATCH(searched, \"term\") OR searched == \"other\""
        )) {
            assertNotNull(configured().statement(Strings.format("""
                sequence by host.id WITHIN 30s PRIMARY 1
                (from a | %s | keep host.id), (from b | keep host.id) | limit 1
                """, commands)));
        }
    }

    /** Indexed scoring is not preserved by the search reader's filter context; ordinary/runtime score paths keep their contract. */
    public void testIndexedSourceSearchScoreBoundaryIsBranchLocal() {
        LogicalPlan plain = configured().statement("""
            sequence by host.id WITHIN 30s PRIMARY 1
            (from a | where MATCH(label, "term")), (from b) | limit 1
            """);
        SequenceStage first = (SequenceStage) plain.collect(Sequence.class).getFirst().children().getFirst();
        assertTrue(
            SequenceStageProjection.eventSources(first)
                .stream()
                .flatMap(source -> source.output().stream())
                .noneMatch(MetadataAttribute::isScoreAttribute)
        );
        configured().statementError("""
            sequence by host.id WITHIN 30s PRIMARY 1
            (from a metadata _score | where MATCH(label, "term") | keep host.id,_score), (from b)
            | keep host.id,_score | limit 1
            """, containsString("does not support METADATA [_score]"));
        for (String commands : List.of("WHERE label == \"term\"", "EVAL searched=TO_UPPER(label) | WHERE MATCH(searched, \"TERM\")")) {
            assertNotNull(optimizer().optimize(configured().statement("""
                sequence by host.id WITHIN 30s PRIMARY 1
                (from a metadata _score | %s | keep host.id,_score), (from b) | keep host.id,_score | limit 1
                """.formatted(commands))));
        }
        assertNotNull(configured().statement("""
            sequence by host.id WITHIN 30s PRIMARY 1
            (from (from a metadata _score | where label == "term" | keep host.id,_score),
                  (from b | where MATCH(label, "term") | keep host.id)),
            (from b | keep host.id) | keep host.id | limit 1
            """));
        configured().statementError("""
            sequence by host.id WITHIN 30s PRIMARY 1
            (from (from a metadata _score | where MATCH(label, "term") | keep host.id,_score),
                  (from b | where label == "term" | keep host.id)),
            (from b | keep host.id) | keep host.id | limit 1
            """, containsString("does not support METADATA [_score]"));
    }

    /** A raw-field alias can become indexed after analysis; its score refusal must follow the optimized source branch. */
    public void testIndexedScoreRefusalAfterAliasFolding() {
        for (String firstStage : List.of(
            "from a metadata _score | eval searched=label | where MATCH(searched, \"term\") | keep host.id,_score",
            "from (from a metadata _score | eval searched=label | where MATCH(searched, \"term\") | keep host.id,_score), "
                + "(from b | where label == \"term\" | keep host.id)"
        )) {
            var analyzed = configured().statement(
                "sequence by host.id WITHIN 30s PRIMARY 1 (" + firstStage + "), (from b) | keep host.id,_score | limit 1"
            );
            assertThat(
                expectThrows(VerificationException.class, () -> optimizer().optimize(analyzed)).getMessage(),
                containsString("does not support METADATA [_score]")
            );
        }
        var separateBranches = configured().statement("""
            sequence by host.id WITHIN 30s PRIMARY 1
            (from (from a metadata _score | where label == "term" | keep host.id,_score),
                  (from b | eval searched=label | where MATCH(searched,"term") | keep host.id)),
            (from b) | keep host.id,_score | limit 1
            """);
        assertNotNull(optimizer().optimize(separateBranches));
        var membership = configured().statement("""
            sequence by host.id WITHIN 30s PRIMARY 1
            (from a metadata _score | where host.id IN
                (from b metadata _score | where MATCH(label,"term") AND _score >= 0 | keep host.id) | keep host.id,_score),
            (from b) | keep host.id,_score | limit 1
            """);
        assertNotNull(optimizer().optimize(membership));
    }

    /** No current full-text implementation requests a recheck; this subclass pins refusal if one does in future. */
    public void testApproximateSourceTranslationCannotElideItsRecheck() {
        LogicalPlan plan = configured().statement("""
            sequence by host.id WITHIN 30s PRIMARY 1
            (from a | where MATCH(label, "term")), (from b) | limit 1
            """);
        Filter filter = plan.collect(Filter.class).getFirst();
        Match exact = (Match) filter.condition();
        Match approximate = new Match(exact.source(), exact.field(), exact.query(), exact.options(), exact.queryBuilder()) {
            @Override
            public TranslationAware.Translatable translatable(LucenePushdownPredicates predicates) {
                return TranslationAware.Translatable.RECHECK;
            }
        };
        assertTrue(SequenceStageProjection.isExactSourceSearchPredicate(filter, exact));
        assertFalse(SequenceStageProjection.isExactSourceSearchPredicate(filter, approximate));

        var vector = new FieldAttribute(Source.EMPTY, "vector", new EsField("vector", DataType.DENSE_VECTOR, Map.of(), true, NONE));
        var knn = new Knn(Source.EMPTY, vector, new Literal(Source.EMPTY, List.of(1.0, 0.0), DataType.DOUBLE), null).withImplicitK(1);
        var vectorSource = ((EsRelation) filter.child()).withAttributes(List.of(vector));
        assertEquals(TranslationAware.FinishedTranslatable.YES, knn.translatable(LucenePushdownPredicates.DEFAULT).finish());
        assertFalse(SequenceStageProjection.isExactSourceSearchPredicate(new Filter(Source.EMPTY, vectorSource, knn), knn));
    }

    public void testViewStagesRequireTheirOwnRequestFilterScope() {
        configured().addView("computed_events", "FROM a | EVAL computed = \"yes\"").statementError("""
            sequence by host.id WITHIN 30s PRIMARY 1
            (from computed_events,a | keep host.id, computed), (from b | keep host.id)
            | limit 1
            """, containsString("Views are not supported inside a SEQUENCE stage"));
    }

    public void testCrossClusterEventStageIsRejectedDuringVerification() {
        var remote = new EsIndex(
            "remote:logs",
            mapping(),
            Map.of("remote:logs", new IndexProperties(IndexMode.STANDARD, 1)),
            Map.of("remote", List.of("logs")),
            Map.of("remote", List.of("logs"))
        );
        String message = configured().addIndex(remote).statementError("""
            sequence by host.id WITHIN 30s PRIMARY 1
            (from remote:logs | keep host.id), (from b | keep host.id)
            | limit 1
            """, containsString("Cross-cluster sources are not supported inside a SEQUENCE stage"));
        assertThat(message, containsString("line 2:"));
    }

    public void testConflictingKeyTypes() {
        configured().statementError("""
            sequence by commonkey WITHIN 30s PRIMARY 1
            (from a | eval commonkey = host.id),
            (from b | eval commonkey = 1)
            """, containsString("SEQUENCE BY column [commonkey] has conflicting types"));
        assertWarnings("No limit defined, adding default limit of [1000]");
    }

    public void testSourceTimestampRequiredDespiteProjection() {
        configured().addIndex(index("c", Map.of("host", host()))).statementError("""
            sequence by host.id WITHIN 30s PRIMARY 1 (from a | keep host.id), (from c | keep host.id)
            """, containsString("require a source [@timestamp]"));
        configured().statementError("""
            sequence by host.id WITHIN 30s PRIMARY 1
            (from a | eval @timestamp = NOW()), (from b)
            """, containsString("cannot overwrite the source [@timestamp]"));
        assertWarnings("No limit defined, adding default limit of [1000]");
    }

    public void testEmptyKeyAndThreeStages() {
        var plan = configured().statement("""
            sequence WITHIN 30s PRIMARY 1
            (from a | keep label), (from b | keep label), (from a | keep label)
            | eval second_label = MEMBER(2, label), third_label = MEMBER(3, label)
            """);
        assertEquals(
            List.of("label", "sequence_provenance", "second_label", "third_label"),
            plan.output().stream().map(Attribute::name).toList()
        );
        ordinarySequenceOutput(plan.collect(Sequence.class).getFirst(), 3);
        assertEquals(3, plan.collect(Sequence.class).getFirst().children().size());
        assertWarnings("No limit defined, adding default limit of [1000]");
    }

    public void testOldClusterRejectedBeforePlanning() {
        configured().minimumTransportVersion(TransportVersionUtils.getPreviousVersion(Sequence.ESQL_SEQUENCE_R5)).statementError("""
            sequence by host.id WITHIN 30s PRIMARY 1 (from a), (from b)
            """, containsString("SEQUENCE is not supported by all nodes in the cluster"));
        assertWarnings("No limit defined, adding default limit of [1000]");
    }

    public void testOptimizerKeepsCompletedRowLimitsOutsideStages() {
        var analyzed = configured().statement("""
            sequence by host.id WITHIN 30s PRIMARY 1
            (from a | eval transformed = TO_LOWER(label) | keep host.id, transformed),
            (from b | eval transformed = TO_LOWER(label) | keep host.id, transformed)
            | eval second_transformed = MEMBER(2, transformed)
            | where transformed != second_transformed
            | sort second_transformed
            | limit 1 by host.id
            | limit 3
            """);
        var optimizer = new LogicalPlanOptimizer(
            EsqlTestUtils.logicalOptimizerContext(EsqlTestUtils.TEST_CFG, FoldContext.small(), TransportVersion.current())
        );
        var optimized = optimizer.optimize(analyzed);
        Sequence sequence = optimized.collect(Sequence.class).getFirst();
        assertEquals(analyzed.output(), optimized.output());
        for (var stage : sequence.children()) {
            assertTrue(stage.collect(Limit.class).isEmpty());
            assertTrue(stage.collect(TopN.class).isEmpty());
            assertTrue(stage.collect(TopNBy.class).isEmpty());
        }
    }

    public void testDateNanosSource() {
        var nanosMapping = new HashMap<>(mapping());
        nanosMapping.put("@timestamp", new EsField("@timestamp", DataType.DATE_NANOS, Map.of(), true, NONE));
        var plan = configured().addIndex(index("nanos", nanosMapping)).statement("""
            sequence by host.id WITHIN 1nanos PRIMARY 1
            (from nanos | keep host.id, @timestamp), (from nanos | keep host.id, @timestamp)
            | eval other_time = MEMBER(2, @timestamp)
            | limit 3
            """);
        assertEquals(
            List.of("host.id", "@timestamp", "sequence_provenance", "other_time"),
            plan.output().stream().map(Attribute::name).toList()
        );
        ordinarySequenceOutput(plan.collect(Sequence.class).getFirst(), 3);
        assertEquals(
            List.of(DataType.KEYWORD, DataType.DATE_NANOS, DataType.KEYWORD, DataType.DATE_NANOS),
            plan.output().stream().map(Attribute::dataType).toList()
        );
    }

    public void testRenameCannotOverwriteTimestamp() {
        for (String stage : List.of(
            "from a | rename label as @timestamp",
            "from a | drop @timestamp | rename label as @timestamp",
            "from a | rename label as time | rename time as @timestamp"
        )) {
            configured().statementError(
                "sequence by host.id WITHIN 30s PRIMARY 1 (" + stage + "), (from b) | limit 1",
                containsString("cannot overwrite the source [@timestamp]")
            );
        }
        var restored = configured().statement("""
            sequence by host.id WITHIN 30s PRIMARY 1
            (from a | rename @timestamp as original | rename original as @timestamp), (from b)
            | limit 1
            """);
        assertEquals(
            DataType.DATETIME,
            restored.output().stream().filter(a -> a.name().equals("@timestamp")).findFirst().orElseThrow().dataType()
        );
    }

    public void testTimestampRenameThroughUnion() {
        var restored = configured().statement("""
            sequence by host.id WITHIN 30s PRIMARY 1
            (from (from a), (from b) | rename @timestamp as original | rename original as @timestamp), (from a)
            | limit 1
            """);
        assertEquals(
            DataType.DATETIME,
            restored.output().stream().filter(a -> a.name().equals("@timestamp")).findFirst().orElseThrow().dataType()
        );
        configured().statementError("""
            sequence by host.id WITHIN 30s PRIMARY 1
            (from (from a), (from b | eval other_time = @timestamp + 1 second | rename other_time as @timestamp)), (from a)
            | limit 1
            """, containsString("cannot overwrite the source [@timestamp]"));
    }

    public void testUnusedStagePayloadColumnsArePruned() {
        var analyzed = configured().statement("""
            sequence by host.id WITHIN 30s PRIMARY 1
            (from a | eval unused = TO_LOWER(label)), (from b | eval unused = TO_LOWER(label))
            | keep host.id
            | limit 3
            """);
        var optimized = optimizer().optimize(analyzed);
        Sequence sequence = optimized.collect(Sequence.class).getFirst();
        assertEquals(List.of("host.id"), sequence.output().stream().map(Attribute::name).toList());
        assertEquals(1, sequence.sourceAttributes().size());
        assertEquals(List.of(0), sequence.sourceStages());
        assertTrue(sequence.collect(Eval.class).isEmpty());
        for (var stage : sequence.children()) {
            assertTrue(((SequenceStage) stage).keys().getFirst().resolved());
        }
    }

    public void testEmptyStageProducesEmptyWideResult() {
        for (int emptyStage = 0; emptyStage < 3; emptyStage++) {
            List<String> stages = new java.util.ArrayList<>();
            for (int stage = 0; stage < 3; stage++) {
                stages.add("(from a" + (stage == emptyStage ? " | where false" : "") + " | keep host.id, label)");
            }
            var analyzed = configured().statement("sequence by host.id WITHIN 30s PRIMARY 1 " + String.join(", ", stages) + " | limit 3");
            var optimized = optimizer().optimize(analyzed);
            assertEquals(analyzed.output(), optimized.output());
            assertTrue(optimized.collect(Sequence.class).isEmpty());
            assertTrue(optimized.collect(LocalRelation.class).stream().anyMatch(LocalRelation::hasEmptySupplier));
        }
    }

    public void testKeylessCountRetainsEventStages() {
        var analyzed = configured().statement("""
            sequence WITHIN 30s PRIMARY 1 (from a), (from b)
            | stats count(*)
            | limit 3
            """);
        var optimized = optimizer().optimize(analyzed);
        Sequence sequence = optimized.collect(Sequence.class).getFirst();
        assertTrue(sequence.output().isEmpty());
        assertEquals(2, sequence.children().size());
        assertFalse(sequence.collect(org.elasticsearch.xpack.esql.plan.logical.EsRelation.class).isEmpty());
    }

    public void testRequiredRawSourceTypesAreRejectedAfterOptimization() {
        for (DataType type : List.of(
            DataType.GEO_SHAPE,
            DataType.CARTESIAN_SHAPE,
            DataType.DENSE_VECTOR,
            DataType.TSID_DATA_TYPE,
            DataType.DATE_RANGE,
            DataType.DOUBLE_RANGE
        )) {
            var mapping = new HashMap<>(mapping());
            mapping.put("special", new EsField("special", type, Map.of(), true, NONE));
            var analyzed = configured().addIndex(index("special_events", mapping)).statement("""
                sequence by host.id WITHIN 30s PRIMARY 1 (from special_events), (from b)
                | keep host.id, special | limit 3
                """);
            var error = expectThrows(VerificationException.class, () -> optimizer().optimize(analyzed));
            assertThat(
                error.getMessage(),
                containsString("SEQUENCE stage [1] cannot read source column [special] of type [" + type.typeName() + "]")
            );
        }
    }

    public void testRawPointOutputsAreAcceptedAfterOptimization() {
        for (DataType type : List.of(DataType.GEO_POINT, DataType.CARTESIAN_POINT)) {
            var mapping = new HashMap<>(mapping());
            mapping.put("location", new EsField("location", type, Map.of(), true, NONE));
            var analyzed = configured().addIndex(index("spatial", mapping)).statement("""
                sequence by host.id WITHIN 30s PRIMARY 1 (from spatial), (from b)
                | keep host.id, location | limit 3
                """);
            assertEquals(List.of("host.id", "location"), optimizer().optimize(analyzed).output().stream().map(Attribute::name).toList());
        }
    }

    public void testUnusedUnsupportedReaderFieldsCanBePruned() {
        var mapping = new HashMap<>(mapping());
        mapping.put("location", new EsField("location", DataType.GEO_SHAPE, Map.of(), true, NONE));
        for (String stage : List.of(
            "from spatial",
            "from spatial | eval unused=TO_STRING(location)",
            "from (from spatial), (from spatial)"
        )) {
            var analyzed = configured().addIndex(index("spatial", mapping))
                .statement("sequence by host.id WITHIN 30s PRIMARY 1 (" + stage + "), (from b) | keep host.id | limit 3");
            var optimized = optimizer().optimize(analyzed);
            assertEquals(List.of("host.id"), optimized.output().stream().map(Attribute::name).toList());
        }
    }

    public void testRequiredMixedMappingConversionIsRejectedAfterOptimization() {
        var mapping = mixedMapping();
        var analyzed = configured().addIndex(index("mixed_events", mapping)).statement("""
            sequence by host.id WITHIN 30s PRIMARY 1 (from mixed_events), (from b)
            | keep host.id, mixed | limit 3
            """);
        assertThat(
            expectThrows(VerificationException.class, () -> optimizer().optimize(analyzed)).getMessage(),
            containsString("cannot read source column [mixed] requiring a conversion across mappings")
        );
    }

    public void testUnusedMixedMappingConversionCanBePruned() {
        for (String stage : List.of("from mixed_events", "from mixed_events | eval unused=mixed+1")) {
            var analyzed = configured().addIndex(index("mixed_events", mixedMapping()))
                .statement("sequence by host.id WITHIN 30s PRIMARY 1 (" + stage + "), (from b) | keep host.id | limit 3");
            assertEquals(List.of("host.id"), optimizer().optimize(analyzed).output().stream().map(Attribute::name).toList());
        }
    }

    private static Map<String, EsField> mixedMapping() {
        var mapping = new HashMap<>(mapping());
        var raw = new FieldAttribute(
            Source.EMPTY,
            null,
            null,
            "mixed",
            new EsField("mixed", DataType.KEYWORD, Map.of(), true, NONE),
            false
        );
        mapping.put(
            "mixed",
            new CompactMultiTypeEsField("mixed", DataType.LONG, false, Map.of(DataType.KEYWORD, new ToLong(Source.EMPTY, raw)), NONE, null)
        );
        return mapping;
    }

    public void testPointFieldsUsedByStagePredicatesAndDerivedKeysAreAccepted() {
        for (DataType type : List.of(DataType.GEO_POINT, DataType.CARTESIAN_POINT)) {
            var mapping = new HashMap<>(mapping());
            mapping.put("location", new EsField("location", type, Map.of(), true, NONE));
            for (String query : List.of("""
                sequence by host.id WITHIN 30s PRIMARY 1
                (from spatial | where ST_X(location)>0 | keep host.id), (from b | keep host.id)
                | keep host.id | limit 3
                """, """
                sequence by normalized WITHIN 30s PRIMARY 1
                (from spatial | eval normalized=TO_STRING(location) | keep normalized),
                (from b | eval normalized=label | keep normalized)
                | stats count(*) | limit 3
                """)) {
                var analyzed = configured().addIndex(index("spatial", mapping)).statement(query);
                assertEquals(1, optimizer().optimize(analyzed).output().size());
            }
        }
    }

    public void testMembershipRightSideKeepsOrdinarySourceTypeSupport() {
        var mapping = new HashMap<>(mapping());
        mapping.put("location", new EsField("location", DataType.GEO_POINT, Map.of(), true, NONE));
        var analyzed = configured().addIndex(index("spatial", mapping)).statement("""
            sequence by host.id WITHIN 30s PRIMARY 1
            (from a | where host.id IN (from spatial | where ST_X(location)>0 | keep host.id) | keep host.id),
            (from b | keep host.id) | keep host.id | limit 3
            """);
        var optimized = optimizer().optimize(analyzed);
        assertEquals(List.of("host.id"), optimized.output().stream().map(Attribute::name).toList());
    }

    public void testPhysicalPlanningKeepsStagesBehindCoordinatorSource() {
        var analyzed = configured().statement("""
            sequence by host.id WITHIN 30s PRIMARY 1
            (from a | eval normalized = TO_LOWER(label)), (from b | eval normalized = TO_LOWER(label))
            | eval second_normalized = MEMBER(2, normalized)
            | where normalized != second_normalized
            | sort second_normalized
            | limit 3
            """);
        var optimized = optimizer().optimize(analyzed);
        var physical = new org.elasticsearch.xpack.esql.planner.mapper.Mapper().map(
            new org.elasticsearch.xpack.esql.session.Versioned<>(optimized, TransportVersion.current())
        );
        var source = physical.collect(org.elasticsearch.xpack.esql.plan.physical.SequenceExec.class).getFirst();
        assertEquals(optimized.collect(Sequence.class).getFirst(), source.sequence());
        assertEquals(analyzed.output(), physical.output());
        assertTrue(physical.collect(org.elasticsearch.xpack.esql.plan.physical.FragmentExec.class).isEmpty());
        assertTrue(physical.collect(org.elasticsearch.xpack.esql.plan.physical.MergeExec.class).isEmpty());
        assertTrue(source.children().isEmpty());
        assertTrue(source.references().isEmpty());
        assertEquals(2, source.sequence().children().size());
        for (var stage : source.sequence().children()) {
            assertTrue(stage.collect(Limit.class).isEmpty());
            assertTrue(stage.collect(TopN.class).isEmpty());
        }
    }

    private LogicalPlanOptimizer optimizer() {
        return new LogicalPlanOptimizer(
            EsqlTestUtils.logicalOptimizerContext(EsqlTestUtils.TEST_CFG, FoldContext.small(), TransportVersion.current())
        );
    }

    private TestAnalyzer configured() {
        return analyzer().minimumTransportVersion(TransportVersion.current())
            .addIndex(index("a", mapping()))
            .addIndex(index("b", mapping()));
    }

    private static Map<String, EsField> mapping() {
        return Map.of(
            "@timestamp",
            DateEsField.dateEsField("@timestamp", Map.of(), true, NONE),
            "host",
            host(),
            "label",
            new EsField("label", DataType.KEYWORD, Map.of(), true, NONE)
        );
    }

    private static EsField host() {
        return new EsField("host", DataType.OBJECT, Map.of("id", new EsField("id", DataType.KEYWORD, Map.of(), true, NONE)), false, NONE);
    }

    private static EsIndex index(String name, Map<String, EsField> mapping) {
        return new EsIndex(name, mapping, Map.of(name, new IndexProperties(IndexMode.STANDARD, 0)), Map.of(), Map.of());
    }
}
