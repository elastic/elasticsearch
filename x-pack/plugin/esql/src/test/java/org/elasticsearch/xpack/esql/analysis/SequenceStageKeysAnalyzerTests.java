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
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.TestAnalyzer;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.DateEsField;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.index.EsIndex;
import org.elasticsearch.xpack.esql.index.IndexProperties;
import org.elasticsearch.xpack.esql.plan.EsqlStatement;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Sequence;
import org.elasticsearch.xpack.esql.plan.logical.SequenceStage;
import org.junit.BeforeClass;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.xpack.esql.core.type.EsField.TimeSeriesFieldType.NONE;
import static org.hamcrest.Matchers.containsString;

/** Distinguishes a positional key from an opposite-named payload in each member's own field scope. */
public class SequenceStageKeysAnalyzerTests extends ESTestCase {
    /** Fixtures enter through the build's default parser. */
    @BeforeClass
    public static void requireSequenceCommand() {
        assumeTrue("requires native SEQUENCE", EsqlCapabilities.Cap.SEQUENCE_COMMAND.isEnabled());
    }

    /** PRIMARY names shared key values through its own key attribute; MEMBER retains authentic opposite-named payload bindings. */
    public void testPrimaryAndMemberLocalNamesDoNotCaptureOppositePayload() {
        for (boolean primaryProcess : List.of(true, false)) {
            String primary = primaryProcess ? "proc" : "conn";
            String other = primaryProcess ? "conn" : "proc";
            String ownKey = primaryProcess ? "process.entity_id" : "process.parent.entity_id";
            String ownPayload = primaryProcess ? "process.parent.entity_id" : "process.entity_id";
            String otherKey = primaryProcess ? "process.parent.entity_id" : "process.entity_id";
            String otherPayload = primaryProcess ? "process.entity_id" : "process.parent.entity_id";
            String query = """
                SEQUENCE BY host.id WITHIN 1m PRIMARY %s
                (FROM a) AS proc BY process.entity_id,
                (FROM b) AS conn BY process.parent.entity_id
                | EVAL other_key=MEMBER(%s, %s), other_payload=MEMBER(%s, %s)
                | KEEP host.id, %s, %s, other_key, other_payload | LIMIT 10
                """.formatted(primary, other, otherKey, other, otherPayload, ownKey, ownPayload);
            EsqlStatement parsed = EsqlTestUtils.TEST_PARSER.createStatement(query);
            LogicalPlan analyzed = configured(DataType.KEYWORD).buildAnalyzer().analyze(parsed.plan());
            assertEquals(
                List.of("host.id", ownKey, ownPayload, "other_key", "other_payload"),
                analyzed.output().subList(0, 5).stream().map(Attribute::name).toList()
            );
            assertEquals(new Binding(0, "process.entity_id", true), binding(analyzed, ownKey));
            assertEquals(new Binding(0, "process.entity_id", true), binding(analyzed, "other_key"));
            assertEquals(new Binding(primaryProcess ? 0 : 1, ownPayload, false), binding(analyzed, ownPayload));
            assertEquals(new Binding(primaryProcess ? 1 : 0, otherPayload, false), binding(analyzed, "other_payload"));
        }
    }

    /** Names differ legitimately; incompatible types at the same local position still fail normal key verification. */
    public void testLocalKeyTypesStillMatchByPosition() {
        configured(DataType.LONG).statementError("""
            SEQUENCE BY host.id WITHIN 1m PRIMARY conn
            (FROM a) AS proc BY process.entity_id,
            (FROM b) AS conn BY process.parent.entity_id
            | KEEP host.id | LIMIT 10
            """, containsString("has conflicting types"));
    }

    /** The overall shortcut changes no key binding, ordinary schema or source-stage ownership. */
    public void testOverallWithinLowersToTheExistingMaxspan() {
        for (String primary : List.of("proc", "conn")) {
            String legacy = """
                SEQUENCE BY host.id WITHIN 30s PRIMARY %s
                (FROM a) AS proc BY process.entity_id,
                (FROM b) AS conn BY process.parent.entity_id
                | KEEP host.id,process.entity_id,process.parent.entity_id | LIMIT 10
                """.formatted(primary);
            LogicalPlan mapped = configured(DataType.KEYWORD).statement(legacy);
            LogicalPlan shortened = configured(DataType.KEYWORD).statement(legacy.replace("WITHIN 30s", "WITHIN 30 seconds"));
            assertEquals(mapped.output().stream().map(Attribute::name).toList(), shortened.output().stream().map(Attribute::name).toList());
            assertEquals(
                mapped.output().stream().map(Attribute::dataType).toList(),
                shortened.output().stream().map(Attribute::dataType).toList()
            );
            Sequence first = mapped.collect(Sequence.class).getFirst();
            Sequence second = shortened.collect(Sequence.class).getFirst();
            assertEquals(30_000_000_000L, second.maxSpanNanos());
            assertEquals(first.maxSpanNanos(), second.maxSpanNanos());
            assertEquals(first.sourceStages(), second.sourceStages());
            assertEquals(
                first.sourceAttributes().stream().map(Attribute::name).toList(),
                second.sourceAttributes().stream().map(Attribute::name).toList()
            );
        }
    }

    private static Binding binding(LogicalPlan plan, String outputName) {
        Sequence sequence = plan.collect(Sequence.class).getFirst();
        Map<NameId, Expression> aliases = new HashMap<>();
        plan.forEachExpressionDown(Alias.class, alias -> aliases.put(alias.id(), alias.child()));
        Attribute attribute = plan.output().stream().filter(value -> value.name().equals(outputName)).findFirst().orElseThrow();
        Set<NameId> visited = new HashSet<>();
        while (visited.add(attribute.id())) {
            for (int column = 0; column < sequence.output().size(); column++) {
                if (sequence.output().get(column).id().equals(attribute.id())) {
                    return new Binding(
                        sequence.sourceStages().get(column),
                        sequence.sourceAttributes().get(column).name(),
                        column < ((SequenceStage) sequence.children().getFirst()).keys().size()
                    );
                }
            }
            Expression value = aliases.get(attribute.id());
            assertTrue("expected an alias of a direct sequence column: " + attribute, value instanceof Attribute);
            attribute = (Attribute) value;
        }
        throw new AssertionError("cyclic alias binding for " + outputName);
    }

    private record Binding(int stage, String field, boolean key) {}

    private static TestAnalyzer configured(DataType connectionKeyType) {
        return EsqlTestUtils.analyzer()
            .minimumTransportVersion(TransportVersion.current())
            .addIndex(index("a", DataType.KEYWORD))
            .addIndex(index("b", connectionKeyType));
    }

    private static EsIndex index(String name, DataType parentType) {
        Map<String, EsField> fields = Map.of(
            "@timestamp",
            DateEsField.dateEsField("@timestamp", Map.of(), true, NONE),
            "host",
            new EsField("host", DataType.OBJECT, Map.of("id", new EsField("id", DataType.KEYWORD, Map.of(), true, NONE)), false, NONE),
            "process",
            new EsField(
                "process",
                DataType.OBJECT,
                Map.of(
                    "entity_id",
                    new EsField("entity_id", DataType.KEYWORD, Map.of(), true, NONE),
                    "parent",
                    new EsField(
                        "parent",
                        DataType.OBJECT,
                        Map.of("entity_id", new EsField("entity_id", parentType, Map.of(), true, NONE)),
                        false,
                        NONE
                    )
                ),
                false,
                NONE
            )
        );
        return new EsIndex(name, fields, Map.of(name, new IndexProperties(IndexMode.STANDARD, 0)), Map.of(), Map.of());
    }
}
