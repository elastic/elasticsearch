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
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.CompactMultiTypeEsField;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.DateEsField;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToLong;
import org.elasticsearch.xpack.esql.index.EsIndex;
import org.elasticsearch.xpack.esql.index.IndexProperties;
import org.elasticsearch.xpack.esql.optimizer.LogicalPlanOptimizer;
import org.elasticsearch.xpack.esql.plan.EsqlStatement;
import org.elasticsearch.xpack.esql.plan.QuerySettings;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.RegexExtract;
import org.elasticsearch.xpack.esql.plan.logical.Sequence;
import org.elasticsearch.xpack.esql.plan.logical.SequenceMatchOutput;
import org.elasticsearch.xpack.esql.plan.logical.SequenceStage;
import org.elasticsearch.xpack.esql.plan.logical.TopN;
import org.elasticsearch.xpack.esql.plan.logical.TopNBy;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;
import org.elasticsearch.xpack.esql.session.FieldNameUtils;
import org.junit.BeforeClass;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;

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
            SEQUENCE BY host.id WITH {"maxspan":"30s"} PRIMARY 2
            (FROM a | KEEP host.id, label), (FROM b | KEEP host.id, label)
            | EVAL from_name = MEMBER(1, label), from_number = MEMBER(1, label)
            | KEEP host.id, label, from_name, from_number | LIMIT 1
            """;
        String named = numeric.replace("PRIMARY 2", "PRIMARY execution")
            .replace("(FROM a | KEEP host.id, label)", "(FROM a | KEEP host.id, label) AS label")
            .replace("(FROM b | KEEP host.id, label)", "(FROM b | KEEP host.id, label) AS execution")
            .replace("from_name = MEMBER(1, label)", "from_name = MEMBER(label, label)");
        for (String retention : List.of("", " | RETURN MATCHES")) {
            LogicalPlan numbered = configured().statement(numeric + retention);
            LogicalPlan aliased = configured().statement(named + retention);
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
    }

    /** Quoted aliases can spell the original's actual generated name; ID-preserved KEEP must still carry genuine originals. */
    public void testQuotedTemporaryOriginalSurvivesStageEdits() throws Exception {
        for (int primary : List.of(1, 2)) {
            for (String edit : List.of("EVAL `placeholder` = \"ordinary\"", "EVAL value = \"ordinary\" | RENAME value AS `placeholder`")) {
                String value = primary == 1 ? "`placeholder`" : "MEMBER(1, `placeholder`)";
                EsqlStatement parsed = quotedCollision("""
                    SEQUENCE BY host.id WITH {"maxspan":"30s"} PRIMARY %s
                    (FROM a | %s | KEEP host.id, label, `placeholder`), (FROM b | KEEP host.id, label)
                    | EVAL ordinary = %s | KEEP ordinary | LIMIT 1 | RETURN MATCHES
                    """.formatted(primary, edit, value), "input");
                assertCompleteOriginalSchema(parsed, configured(), List.of("ordinary"));
            }
        }
    }

    /** Reserve original, key and MEMBER names, including occupied fallback suffixes, before any outer user edit. */
    public void testQuotedTemporaryBindingsSurviveOuterEdits() throws Exception {
        for (int primary : List.of(1, 2)) {
            for (String binding : List.of("output", "key", "member")) {
                for (String edit : List.of(
                    "EVAL `placeholder` = \"ordinary\"",
                    "EVAL value = \"ordinary\" | RENAME value AS `placeholder`"
                )) {
                    EsqlStatement parsed = quotedCollision("""
                        SEQUENCE BY host.id WITH {"maxspan":"30s"} PRIMARY %s
                        (FROM a | KEEP host.id, label), (FROM b | KEEP host.id, label)
                        | %s
                        | EVAL `placeholder_1` = "reserved", `placeholder_2` = "reserved", ordinary = MEMBER(%s, label)
                        | KEEP `placeholder`, ordinary | LIMIT 1 | RETURN MATCHES
                        """.formatted(primary, edit, 3 - primary), binding);
                    assertCompleteOriginalSchema(parsed, configured(), List.of(quotedName(parsed, binding), "ordinary"));
                }
            }
        }
    }

    /** Resolved mapped names were not all present at parse time; rebinding KEEP by ID must follow the new source alias name. */
    public void testMappedTemporaryOriginalSurvivesStageKeep() throws Exception {
        for (int primary : List.of(1, 2)) {
            EsqlStatement parsed = quotedCollision("""
                SEQUENCE BY host.id WITH {"maxspan":"30s"} PRIMARY %s
                (FROM a | KEEP host.id, label), (FROM b | KEEP host.id, label)
                | KEEP label | LIMIT 1 | RETURN MATCHES
                """.formatted(primary), "input");
            String mappedName = quotedName(parsed, "input");
            Map<String, EsField> fields = new HashMap<>(mapping());
            for (String name : List.of(mappedName, mappedName + "_1", mappedName + "_2")) {
                fields.put(name, new EsField(name, DataType.KEYWORD, Map.of(), true, NONE));
            }
            assertCompleteOriginalSchema(
                parsed,
                analyzer().minimumTransportVersion(TransportVersion.current()).addIndex(index("a", fields)).addIndex(index("b", fields)),
                List.of("label")
            );
        }
    }

    /** Compiler-only KEEP references are not mapped-field demands, while an ordinary equal-name field remains a demand. */
    public void testProtectedKeepReferencesDoNotHideOrdinaryFieldDiscovery() throws Exception {
        EsqlStatement parsed = quotedCollision("""
            SEQUENCE BY host.id WITH {"maxspan":"30s"} PRIMARY 1
            (FROM a | KEEP host.id, label, `placeholder`), (FROM b | KEEP host.id, label)
            | KEEP label | LIMIT 1 | RETURN MATCHES
            """, "input");
        Set<String> fields = FieldNameUtils.resolveFieldNames(parsed.plan(), false, false).fieldNames();
        assertTrue(fields.contains(quotedName(parsed, "input")));
        Set<String> compilerNames = new HashSet<>();
        parsed.plan().forEachExpressionDown(SequenceMatchOutput.BindingReference.class, ref -> compilerNames.add(ref.name()));
        compilerNames.remove(quotedName(parsed, "input"));
        assertFalse(compilerNames.isEmpty());
        assertTrue(compilerNames.stream().noneMatch(fields::contains));
    }

    /** Capture names participate in reservation even when no ordinary alias declares the colliding name. */
    public void testExtractedTemporaryOriginalSurvivesStageKeep() throws Exception {
        for (int primary : List.of(1, 2)) {
            EsqlStatement parsed = quotedCollision("""
                SEQUENCE BY host.id WITH {"maxspan":"30s"} PRIMARY %s
                (FROM a | DISSECT label "%%{placeholder}" | KEEP host.id, `placeholder`), (FROM b)
                | EVAL ordinary = %s | KEEP ordinary | LIMIT 1 | RETURN MATCHES
                """.formatted(primary, primary == 1 ? "`placeholder`" : "MEMBER(1, `placeholder`)"), "input");
            assertCompleteOriginalSchema(parsed, configured(), List.of("ordinary"));
        }
    }

    /** Extracted keys resolve independently, while typed payloads survive pruning and MEMBER publication. */
    public void testRetainedExtractionKeysAndTypedPayload() {
        for (String extraction : List.of(
            "DISSECT label \"%{parsed_key}:%{payload}:%{unused}\"",
            "GROK label \"%{WORD:parsed_key}:%{INT:payload:int}:%{WORD:unused}\""
        )) {
            for (int primary : List.of(1, 2)) {
                String query = """
                    SEQUENCE BY parsed_key WITH {"maxspan":"30s"} PRIMARY %s
                    (FROM a | %s | RENAME payload AS parsed_value | DROP unused | KEEP parsed_key, parsed_value),
                    (FROM b | %s | RENAME payload AS parsed_value | DROP unused | KEEP parsed_key, parsed_value)
                    | EVAL other_value = MEMBER(%s, parsed_value)
                    | KEEP parsed_key, parsed_value, other_value | LIMIT 1 | RETURN MATCHES
                    """.formatted(primary, extraction, extraction, 3 - primary);
                EsqlStatement parsed = EsqlTestUtils.TEST_PARSER.createStatement(query);
                assertCompleteOriginalSchema(parsed, configured(), List.of("parsed_key", "parsed_value", "other_value"));
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
                assertEquals(2L, sequence.sourceAttributes().stream().filter(a -> a.dataType() == DataType.SOURCE).count());
            }
        }
    }

    /** A nullified expansion input keeps the ordinary schema independently of retained evidence. */
    public void testNullifyExpandedOrdinaryPrimaryOne() {
        assertNullifyExpandedBindings(1, false);
    }

    /** The second primary has the same ordinary null-typed expansion contract. */
    public void testNullifyExpandedOrdinaryPrimaryTwo() {
        assertNullifyExpandedBindings(2, false);
    }

    /** Compiler-owned key and original bindings survive late NULLIFY projection recomputation. */
    public void testNullifyExpandedRetainedPrimaryOne() {
        assertNullifyExpandedBindings(1, true);
    }

    /** Retained dependencies survive with the second primary as well as the first. */
    public void testNullifyExpandedRetainedPrimaryTwo() {
        assertNullifyExpandedBindings(2, true);
    }

    /** LOAD must report the member's missing field rather than trying to load the MEMBER display spelling. */
    public void testLoadMissingMemberHasScopedDiagnostic() {
        for (int primary : List.of(1, 2)) {
            int other = 3 - primary;
            configured().statementError(
                """
                    SET unmapped_fields="load";
                    SEQUENCE BY host.id WITH {"maxspan":"1m"} PRIMARY %s (FROM a), (FROM b)
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
                    SEQUENCE BY host.id WITH {"maxspan":"1m"} PRIMARY %s
                    (FROM a | EVAL stage_value=label), (FROM b | EVAL stage_value=label)
                    | EVAL other=MEMBER(%s, %s) | KEEP other | LIMIT 10
                    """.formatted(primary, other, memberField));
                assertEquals(List.of("other"), analyzed.output().stream().map(Attribute::name).toList());
                assertEquals(List.of(DataType.KEYWORD), analyzed.output().stream().map(Attribute::dataType).toList());
            }
        }
    }

    private void assertNullifyExpandedBindings(int primary, boolean capture) {
        String query = """
            SET unmapped_fields="nullify";
            SEQUENCE BY host.id WITH {"maxspan":"1m"} PRIMARY %s
            (FROM a | EVAL pick=absent_optional | MV_EXPAND pick | KEEP host.id,pick),
            (FROM b | EVAL pick=absent_optional | MV_EXPAND pick | KEEP host.id,pick)
            | EVAL other=MEMBER(%s,pick) | KEEP host.id,pick,other | SORT host.id | LIMIT 10
            """.formatted(primary, 3 - primary) + (capture ? " | RETURN MATCHES" : "");
        EsqlStatement parsed = EsqlTestUtils.TEST_PARSER.createStatement(query);
        LogicalPlan analyzed = configured().unmappedResolution(parsed.setting(QuerySettings.UNMAPPED_FIELDS))
            .buildAnalyzer()
            .analyze(parsed.plan());
        List<String> names = new ArrayList<>(List.of("host.id", "pick", "other"));
        List<DataType> types = new ArrayList<>(List.of(DataType.KEYWORD, DataType.NULL, DataType.NULL));
        if (capture) {
            names.addAll(
                List.of(
                    "match_key_1",
                    "member_1_index",
                    "member_1_id",
                    "member_1_source",
                    "member_2_index",
                    "member_2_id",
                    "member_2_source"
                )
            );
            types.addAll(
                List.of(
                    DataType.KEYWORD,
                    DataType.KEYWORD,
                    DataType.KEYWORD,
                    DataType.SOURCE,
                    DataType.KEYWORD,
                    DataType.KEYWORD,
                    DataType.SOURCE
                )
            );
        }
        assertEquals(names, analyzed.output().stream().map(Attribute::name).toList());
        assertEquals(types, analyzed.output().stream().map(Attribute::dataType).toList());
        assertEquals(analyzed.output().size(), analyzed.output().stream().map(Attribute::id).distinct().count());
        assertEquals(capture ? 10 : 0, SequenceMatchOutput.deliveryMetadata(parsed, analyzed.output(), Map.of()).size());
        assertTrue(analyzed.collect(SequenceMatchOutput.Capture.class).isEmpty());
        assertTrue(analyzed.collect(SequenceMatchOutput.Scope.class).isEmpty());
        assertTrue(analyzed.collect(SequenceMatchOutput.Originals.class).isEmpty());
        LogicalPlan optimized = optimizer().optimize(analyzed);
        assertEquals(analyzed.output(), optimized.output());
        assertEquals(
            capture ? 2L : 0L,
            optimized.collect(Sequence.class).getFirst().sourceAttributes().stream().filter(a -> a.dataType() == DataType.SOURCE).count()
        );
    }

    /** Public metadata-shaped captures remain processed values, separate from the protected source slots. */
    public void testExtractedMetadataNamesPreserveOriginals() {
        for (String extraction : List.of(
            "DISSECT label \"%{_id}:%{_index}:%{_source}\"",
            "GROK label \"%{WORD:_id}:%{WORD:_index}:%{WORD:_source}\""
        )) {
            assertProtectedBindings("| " + extraction, "", "_id", "_index", "_source");
        }
    }

    /** Original retention does not relax the event-time source contract for extracted fields. */
    public void testRetainedExtractionCannotOverwriteTimestamp() {
        for (String extraction : List.of("DISSECT label \"%{@timestamp}\"", "GROK label \"%{WORD:@timestamp}\"")) {
            configured().statementError("""
                SEQUENCE BY host.id WITH {"maxspan":"30s"} PRIMARY 1
                (FROM a | %s), (FROM b)
                | KEEP host.id | LIMIT 1 | RETURN MATCHES
                """.formatted(extraction), containsString("cannot overwrite the source [@timestamp]"));
        }
    }

    /** Retaining authentic originals does not keep an extraction whose processed outputs are all unused. */
    public void testUnusedRetainedExtractionIsPruned() {
        for (String extraction : List.of("DISSECT label \"%{unused}\"", "GROK label \"%{WORD:unused}\"")) {
            LogicalPlan analyzed = configured().statement("""
                SEQUENCE BY host.id WITH {"maxspan":"30s"} PRIMARY 2
                (FROM a | %s), (FROM b | %s)
                | KEEP host.id | LIMIT 1 | RETURN MATCHES
                """.formatted(extraction, extraction));
            LogicalPlan optimized = optimizer().optimize(analyzed);
            assertEquals(analyzed.output(), optimized.output());
            assertTrue(optimized.collect(RegexExtract.class).isEmpty());
            assertEquals(
                2L,
                optimized.collect(Sequence.class)
                    .getFirst()
                    .sourceAttributes()
                    .stream()
                    .filter(a -> a.dataType() == DataType.SOURCE)
                    .count()
            );
        }
    }

    /** Independent union branches resolve ordinary PRIMARY names and MEMBER demands against their own stages. */
    public void testUnionResolvesIndependentPrimaryAndMemberContexts() {
        String first = """
            SEQUENCE BY host.id WITH {"maxspan":"30s"} PRIMARY 1
            (FROM a | KEEP host.id, label), (FROM b | KEEP host.id, label)
            | EVAL other = MEMBER(2, label) | KEEP label, other
            """;
        String second = """
            SEQUENCE BY host.id WITH {"maxspan":"30s"} PRIMARY 2
            (FROM a | KEEP host.id, label), (FROM b | KEEP host.id, label)
            | EVAL other = MEMBER(1, label) | KEEP label, other
            """;
        for (List<String> branches : List.of(List.of(first, second), List.of(second, first))) {
            LogicalPlan analyzed = configured().statement(
                "FROM (" + branches.get(0) + "), (" + branches.get(1) + ") | STATS n = COUNT(*) BY label, other | SORT label | LIMIT 10"
            );
            assertEquals(List.of("n", "label", "other"), analyzed.output().stream().map(Attribute::name).toList());
            assertEquals(
                List.of(DataType.LONG, DataType.KEYWORD, DataType.KEYWORD),
                analyzed.output().stream().map(Attribute::dataType).toList()
            );
            assertEquals(2, analyzed.collect(Sequence.class).size());
            assertTrue(analyzed.collect(SequenceMatchOutput.Scope.class).isEmpty());
            LogicalPlan optimized = optimizer().optimize(analyzed);
            assertEquals(analyzed.output(), optimized.output());
            assertEquals(2, optimized.collect(Sequence.class).size());
        }
    }

    /** Ordinary union alignment retains its normal null-fill and conflicting-type behavior for sequence branches. */
    public void testUnionSequenceUsesOrdinarySchemaAlignment() {
        String sequence = """
            SEQUENCE BY host.id WITH {"maxspan":"30s"} PRIMARY 2 (FROM a), (FROM b)
            | EVAL supporting = MEMBER(1, label) | KEEP label, supporting
            """;
        for (String branches : List.of("(" + sequence + "), (FROM a | KEEP label)", "(FROM a | KEEP label), (" + sequence + ")")) {
            LogicalPlan analyzed = configured().statement("FROM " + branches + " | KEEP label, supporting | SORT label | LIMIT 10");
            assertEquals(List.of("label", "supporting"), analyzed.output().stream().map(Attribute::name).toList());
            assertEquals(List.of(DataType.KEYWORD, DataType.KEYWORD), analyzed.output().stream().map(Attribute::dataType).toList());
        }
        // Bare conflicting columns are published as UNSUPPORTED, just as in an ordinary source union.
        // Only a downstream operation that needs a usable type must reject that conflict.
        String integerLabel = "FROM a | EVAL label = 1 | KEEP label";
        for (String keywordLabel : List.of("FROM b | KEEP label", sequence)) {
            for (List<String> branches : List.of(List.of(keywordLabel, integerLabel), List.of(integerLabel, keywordLabel))) {
                LogicalPlan analyzed = configured().statement(
                    "FROM (" + branches.get(0) + "), (" + branches.get(1) + ") | KEEP label | LIMIT 10"
                );
                assertEquals(List.of("label"), analyzed.output().stream().map(Attribute::name).toList());
                assertEquals(List.of(DataType.UNSUPPORTED), analyzed.output().stream().map(Attribute::dataType).toList());
            }
        }
    }

    /** Validate the same final typed roles used by delivery, not just a familiar set of display labels. */
    private static void assertCompleteOriginalSchema(EsqlStatement parsed, TestAnalyzer analyzer, List<String> ordinary) {
        LogicalPlan analyzed = analyzer.buildAnalyzer().analyze(parsed.plan());
        List<String> expected = new ArrayList<>(ordinary);
        expected.addAll(
            List.of("match_key_1", "member_1_index", "member_1_id", "member_1_source", "member_2_index", "member_2_id", "member_2_source")
        );
        assertEquals(expected, analyzed.output().stream().map(Attribute::name).toList());
        assertEquals(analyzed.output().size(), analyzed.output().stream().map(Attribute::id).distinct().count());
        assertEquals(analyzed.output().size(), SequenceMatchOutput.deliveryMetadata(parsed, analyzed.output(), Map.of()).size());
    }

    /**
     * Calibrate an actual process-local ID without resetting the counter or mutating a plan. An ordinary caller could
     * supply this quoted name too; reflection only makes the exact collision deterministic for this regression.
     */
    private static EsqlStatement quotedCollision(String template, String binding) throws Exception {
        var parser = EsqlTestUtils.TEST_PARSER;
        var counterField = org.elasticsearch.xpack.esql.core.expression.NameId.class.getDeclaredField("COUNTER");
        counterField.setAccessible(true);
        AtomicLong counter = (AtomicLong) counterField.get(null);
        parser.createStatement(template);
        long before = counter.get();
        EsqlStatement calibration = parser.createStatement(template);
        long offset = bindingId(calibration.plan(), binding) - before;
        String name = Attribute.rawTemporaryName(
            binding.equals("key") ? "sequence_key" : binding.equals("member") ? "sequence_member" : "sequence_original",
            Long.toString(counter.get() + offset)
        );
        EsqlStatement parsed = parser.createStatement(template.replace("placeholder", name));
        assertEquals(name, quotedName(parsed, binding));
        return parsed;
    }

    private static String quotedName(EsqlStatement parsed, String binding) {
        String prefix = binding.equals("key") ? "sequence_key" : binding.equals("member") ? "sequence_member" : "sequence_original";
        return Attribute.rawTemporaryName(prefix, Long.toString(bindingId(parsed.plan(), binding)));
    }

    /** Obtain compiler identities from the actual parsed context rather than introducing test-only IDs into a plan. */
    private static long bindingId(LogicalPlan parsed, String binding) {
        var contexts = parsed.collect(SequenceMatchOutput.Scope.class)
            .getFirst()
            .nodeProperties()
            .stream()
            .filter(property -> property.getClass().getName().equals(SequenceMatchOutput.class.getName() + "$Context"))
            .toList();
        assertEquals(1, contexts.size());
        String context = contexts.getFirst().toString();
        String expression = switch (binding) {
            case "input", "output" -> "Role\\[stage=0, metadata=_id, input=(\\d+), output=(\\d+)\\]";
            case "key" -> "keys=\\[(\\d+)\\]";
            case "member" -> "Member\\[stage=\\d+, field=label\\]=(\\d+)";
            default -> throw new IllegalArgumentException("unknown binding " + binding);
        };
        var matcher = Pattern.compile(expression).matcher(context);
        assertTrue(context, matcher.find());
        return Long.parseLong(matcher.group(binding.equals("output") ? 2 : 1));
    }

    /** A public calculation must not replace the protected identity retained for the first member. */
    public void testProtectedOriginalSurvivesOrdinaryEval() {
        assertProtectedBindings("", "| EVAL member_1_id = \"ordinary\"", "member_1_id");
    }

    /** Reusing a published key name must leave the original correlation value available for delivery. */
    public void testFrozenKeySurvivesOrdinaryEval() {
        assertProtectedBindings("", "| EVAL match_key_1 = \"ordinary\"", "match_key_1");
    }

    /** A stage calculation may use a former internal name without removing authentic source metadata. */
    public void testStageOriginalSurvivesOrdinaryEval() {
        assertProtectedBindings("| EVAL sequence_input0_id = \"ordinary\"", "", "sequence_input0_id");
    }

    /** Renaming a processed stage field must preserve the separately captured original identity. */
    public void testStageOriginalSurvivesOrdinaryRename() {
        assertProtectedBindings("| RENAME label AS sequence_input0_id", "", "sequence_input0_id");
    }

    /** Publication must preserve ordinary fields even when both a preferred label and its first suffix are occupied. */
    public void testProtectedPublicationReservesOrdinaryNames() {
        assertProtectedBindings(
            "",
            "| EVAL member_1_id = \"ordinary\", member_1_id_1 = \"reserved\", match_key_1 = \"ordinary\", match_key_1_1 = \"reserved\"",
            "member_1_id",
            "member_1_id_1",
            "match_key_1",
            "match_key_1_1"
        );
    }

    /** Ordinary output names must not remove the distinct bindings needed to publish a complete match. */
    private void assertProtectedBindings(String stageCommand, String outerCommand, String... ordinaryNames) {
        var ordinary = List.of(ordinaryNames);
        var expectedNames = new ArrayList<>(ordinary);
        for (String name : List.of(
            "match_key_1",
            "member_1_index",
            "member_1_id",
            "member_1_source",
            "member_2_index",
            "member_2_id",
            "member_2_source"
        )) {
            expectedNames.add(ordinary.contains(name) ? name + (ordinary.contains(name + "_1") ? "_2" : "_1") : name);
        }
        var expectedTypes = new ArrayList<DataType>();
        ordinary.forEach(ignored -> expectedTypes.add(DataType.KEYWORD));
        expectedTypes.addAll(
            List.of(
                DataType.KEYWORD,
                DataType.KEYWORD,
                DataType.KEYWORD,
                DataType.SOURCE,
                DataType.KEYWORD,
                DataType.KEYWORD,
                DataType.SOURCE
            )
        );
        for (int primary : List.of(1, 2)) {
            var plan = configured().statement(Strings.format("""
                SEQUENCE BY host.id WITH {"maxspan":"30s"} PRIMARY %s
                (FROM a %s), (FROM b %s)
                %s | KEEP %s | LIMIT 1 | RETURN MATCHES
                """, primary, stageCommand, stageCommand, outerCommand, String.join(", ", ordinary)));
            assertEquals(expectedNames, plan.output().stream().map(Attribute::name).toList());
            assertEquals(expectedTypes, plan.output().stream().map(Attribute::dataType).toList());
            assertEquals(plan.output().size(), plan.output().stream().map(Attribute::id).distinct().count());
        }
    }

    public void testTypedPrimaryOutputAndPostSequenceMemberFilter() {
        var plan = configured().statement("""
            sequence by host.id with {"maxspan":"30s"} PRIMARY 1
            (from a | eval label = "network" | keep host.id, @timestamp, label),
            (from b | eval label = "process" | keep host.id, @timestamp, label)
            | where MEMBER(2, @timestamp) >= @timestamp
            | eval second_label = MEMBER(2, label)
            """);
        Sequence sequence = plan.collect(Sequence.class).getFirst();
        assertEquals(
            List.of("host.id", "stage1.@timestamp", "stage1.label", "stage2.@timestamp", "stage2.label"),
            sequence.output().stream().map(Attribute::name).toList()
        );
        assertEquals(
            List.of(DataType.KEYWORD, DataType.DATETIME, DataType.KEYWORD, DataType.DATETIME, DataType.KEYWORD),
            sequence.output().stream().map(Attribute::dataType).toList()
        );
        assertEquals(List.of("host.id", "@timestamp", "label", "second_label"), plan.output().stream().map(Attribute::name).toList());
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
                SEQUENCE BY host.id WITH {"maxspan":"30s"} PRIMARY %s
                (FROM a | KEEP host.id, label), (FROM b | KEEP host.id, label)
                """.formatted(primary);
            var plan = configured().statement(query + " | LIMIT 1");
            assertEquals(List.of("host.id", "label"), plan.output().stream().map(Attribute::name).toList());
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
            SEQUENCE BY host.id WITH {"maxspan":"30s"} PRIMARY 1
            (FROM named_events), (FROM b)
            | KEEP stage1.label | LIMIT 1
            """);
        assertEquals(List.of("stage1.label"), plan.output().stream().map(Attribute::name).toList());
    }

    public void testDerivedExpandedKeysResolveInEachStage() {
        var plan = configured().statement("""
            sequence by commonkey with {"maxspan":"2m"} PRIMARY 1
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
        assertEquals(List.of("commonkey", "stage1.label", "stage2.label"), sequence.output().stream().map(Attribute::name).toList());
        assertTrue(first.child().collect(Limit.class).isEmpty());
        assertTrue(second.child().collect(Limit.class).isEmpty());
    }

    public void testKeysMustSurviveProjection() {
        configured().statementError("""
            sequence by host.id with {"maxspan":"30s"} PRIMARY 1
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

    public void testIndexedSearchRequiresAnExplicitStageBoundary() {
        for (String predicate : List.of("MATCH(label, \"term\")", "QSTR(\"label:term\")", "KQL(\"label:term\")")) {
            configured().statementError(Strings.format("""
                sequence by host.id with {"maxspan":"30s"} PRIMARY 1
                (from a | where %s | keep host.id), (from b | keep host.id) | limit 1
                """, predicate), containsString("is not supported inside a SEQUENCE stage"));
        }
    }

    public void testViewStagesRequireTheirOwnRequestFilterScope() {
        configured().addView("computed_events", "FROM a | EVAL computed = \"yes\"").statementError("""
            sequence by host.id with {"maxspan":"30s"} PRIMARY 1
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
            sequence by host.id with {"maxspan":"30s"} PRIMARY 1
            (from remote:logs | keep host.id), (from b | keep host.id)
            | limit 1
            """, containsString("Cross-cluster sources are not supported inside a SEQUENCE stage"));
        assertThat(message, containsString("line 2:"));
    }

    public void testConflictingKeyTypes() {
        configured().statementError("""
            sequence by commonkey with {"maxspan":"30s"} PRIMARY 1
            (from a | eval commonkey = host.id),
            (from b | eval commonkey = 1)
            """, containsString("SEQUENCE BY column [commonkey] has conflicting types"));
        assertWarnings("No limit defined, adding default limit of [1000]");
    }

    public void testSourceTimestampRequiredDespiteProjection() {
        configured().addIndex(index("c", Map.of("host", host()))).statementError("""
            sequence by host.id with {"maxspan":"30s"} PRIMARY 1 (from a | keep host.id), (from c | keep host.id)
            """, containsString("require a source [@timestamp]"));
        configured().statementError("""
            sequence by host.id with {"maxspan":"30s"} PRIMARY 1
            (from a | eval @timestamp = NOW()), (from b)
            """, containsString("cannot overwrite the source [@timestamp]"));
        assertWarnings("No limit defined, adding default limit of [1000]");
    }

    public void testEmptyKeyAndThreeStages() {
        var plan = configured().statement("""
            sequence with {"maxspan":"30s"} PRIMARY 1
            (from a | keep label), (from b | keep label), (from a | keep label)
            | eval second_label = MEMBER(2, label), third_label = MEMBER(3, label)
            """);
        assertEquals(List.of("label", "second_label", "third_label"), plan.output().stream().map(Attribute::name).toList());
        assertEquals(3, plan.collect(Sequence.class).getFirst().children().size());
        assertWarnings("No limit defined, adding default limit of [1000]");
    }

    public void testOldClusterRejectedBeforePlanning() {
        configured().minimumTransportVersion(TransportVersionUtils.getPreviousVersion(Sequence.ESQL_SEQUENCE)).statementError("""
            sequence by host.id with {"maxspan":"30s"} PRIMARY 1 (from a), (from b)
            """, containsString("SEQUENCE is not supported by all nodes in the cluster"));
        assertWarnings("No limit defined, adding default limit of [1000]");
    }

    public void testOptimizerKeepsCompletedRowLimitsOutsideStages() {
        var analyzed = configured().statement("""
            sequence by host.id with {"maxspan":"30s"} PRIMARY 1
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
            sequence by host.id with {"maxspan":"1nanos"} PRIMARY 1
            (from nanos | keep host.id, @timestamp), (from nanos | keep host.id, @timestamp)
            | eval other_time = MEMBER(2, @timestamp)
            | limit 3
            """);
        assertEquals(
            List.of(DataType.KEYWORD, DataType.DATE_NANOS, DataType.DATE_NANOS),
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
                "sequence by host.id with {\"maxspan\":\"30s\"} PRIMARY 1 (" + stage + "), (from b) | limit 1",
                containsString("cannot overwrite the source [@timestamp]")
            );
        }
        var restored = configured().statement("""
            sequence by host.id with {"maxspan":"30s"} PRIMARY 1
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
            sequence by host.id with {"maxspan":"30s"} PRIMARY 1
            (from (from a), (from b) | rename @timestamp as original | rename original as @timestamp), (from a)
            | limit 1
            """);
        assertEquals(
            DataType.DATETIME,
            restored.output().stream().filter(a -> a.name().equals("@timestamp")).findFirst().orElseThrow().dataType()
        );
        configured().statementError("""
            sequence by host.id with {"maxspan":"30s"} PRIMARY 1
            (from (from a), (from b | eval other_time = @timestamp + 1 second | rename other_time as @timestamp)), (from a)
            | limit 1
            """, containsString("cannot overwrite the source [@timestamp]"));
    }

    public void testUnusedStagePayloadColumnsArePruned() {
        var analyzed = configured().statement("""
            sequence by host.id with {"maxspan":"30s"} PRIMARY 1
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
            var analyzed = configured().statement(
                "sequence by host.id with {\"maxspan\":\"30s\"} PRIMARY 1 " + String.join(", ", stages) + " | limit 3"
            );
            var optimized = optimizer().optimize(analyzed);
            assertEquals(analyzed.output(), optimized.output());
            assertTrue(optimized.collect(Sequence.class).isEmpty());
            assertTrue(optimized.collect(LocalRelation.class).stream().anyMatch(LocalRelation::hasEmptySupplier));
        }
    }

    public void testKeylessCountRetainsEventStages() {
        var analyzed = configured().statement("""
            sequence with {"maxspan":"30s"} PRIMARY 1 (from a), (from b)
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
            DataType.GEO_POINT,
            DataType.GEO_SHAPE,
            DataType.CARTESIAN_POINT,
            DataType.CARTESIAN_SHAPE,
            DataType.DENSE_VECTOR,
            DataType.TSID_DATA_TYPE,
            DataType.DATE_RANGE,
            DataType.DOUBLE_RANGE
        )) {
            var mapping = new HashMap<>(mapping());
            mapping.put("special", new EsField("special", type, Map.of(), true, NONE));
            var analyzed = configured().addIndex(index("special_events", mapping)).statement("""
                sequence by host.id with {"maxspan":"30s"} PRIMARY 1 (from special_events), (from b)
                | keep host.id, special | limit 3
                """);
            var error = expectThrows(VerificationException.class, () -> optimizer().optimize(analyzed));
            assertThat(
                error.getMessage(),
                containsString("SEQUENCE stage [1] cannot read source column [special] of type [" + type.typeName() + "]")
            );
        }
    }

    public void testUnusedUnsupportedReaderFieldsCanBePruned() {
        var mapping = new HashMap<>(mapping());
        mapping.put("location", new EsField("location", DataType.GEO_POINT, Map.of(), true, NONE));
        for (String stage : List.of(
            "from spatial",
            "from spatial | eval unused=TO_STRING(location)",
            "from (from spatial), (from spatial)"
        )) {
            var analyzed = configured().addIndex(index("spatial", mapping))
                .statement("sequence by host.id with {\"maxspan\":\"30s\"} PRIMARY 1 (" + stage + "), (from b) | keep host.id | limit 3");
            var optimized = optimizer().optimize(analyzed);
            assertEquals(List.of("host.id"), optimized.output().stream().map(Attribute::name).toList());
        }
    }

    public void testRequiredMixedMappingConversionIsRejectedAfterOptimization() {
        var mapping = mixedMapping();
        var analyzed = configured().addIndex(index("mixed_events", mapping)).statement("""
            sequence by host.id with {"maxspan":"30s"} PRIMARY 1 (from mixed_events), (from b)
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
                .statement("sequence by host.id with {\"maxspan\":\"30s\"} PRIMARY 1 (" + stage + "), (from b) | keep host.id | limit 3");
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

    public void testUnsupportedReaderFieldsUsedByStagePredicatesAndKeysAreRejected() {
        var mapping = new HashMap<>(mapping());
        mapping.put("location", new EsField("location", DataType.GEO_POINT, Map.of(), true, NONE));
        for (String query : List.of("""
            sequence by host.id with {"maxspan":"30s"} PRIMARY 1
            (from spatial | where ST_X(location)>0 | keep host.id), (from b | keep host.id)
            | keep host.id | limit 3
            """, """
            sequence by normalized with {"maxspan":"30s"} PRIMARY 1
            (from spatial | eval normalized=TO_STRING(location) | keep normalized),
            (from b | eval normalized=label | keep normalized)
            | stats count(*) | limit 3
            """)) {
            var analyzed = configured().addIndex(index("spatial", mapping)).statement(query);
            assertThat(
                expectThrows(VerificationException.class, () -> optimizer().optimize(analyzed)).getMessage(),
                containsString("cannot read source column [location] of type [geo_point]")
            );
        }
    }

    public void testMembershipRightSideKeepsOrdinarySourceTypeSupport() {
        var mapping = new HashMap<>(mapping());
        mapping.put("location", new EsField("location", DataType.GEO_POINT, Map.of(), true, NONE));
        var analyzed = configured().addIndex(index("spatial", mapping)).statement("""
            sequence by host.id with {"maxspan":"30s"} PRIMARY 1
            (from a | where host.id IN (from spatial | where ST_X(location)>0 | keep host.id) | keep host.id),
            (from b | keep host.id) | keep host.id | limit 3
            """);
        var optimized = optimizer().optimize(analyzed);
        assertEquals(List.of("host.id"), optimized.output().stream().map(Attribute::name).toList());
    }

    public void testPhysicalPlanningKeepsStagesBehindCoordinatorSource() {
        var analyzed = configured().statement("""
            sequence by host.id with {"maxspan":"30s"} PRIMARY 1
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
