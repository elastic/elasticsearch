/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.parser;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Sequence;
import org.elasticsearch.xpack.esql.plan.logical.SequenceCondition;
import org.elasticsearch.xpack.esql.plan.logical.SequenceMatchOutput;

import java.util.List;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_FUNCTION_REGISTRY;
import static org.hamcrest.Matchers.containsString;

/** Pins the narrow deferred-WHERE authoring boundary before analysis or execution. */
public class SequenceConditionParserTests extends ESTestCase {
    private final EsqlParser parser = new EsqlParser(new EsqlConfig(true, TEST_FUNCTION_REGISTRY));

    public void testNoConditionAndOuterMemberRemainOrdinaryStagePlans() {
        for (String link : List.of("", " WITHIN 5s")) {
            Sequence sequence = sequence("""
                SEQUENCE WITHIN 30s PRIMARY 2 (FROM a), (FROM b)%s
                | WHERE label != MEMBER(1, label)
                """.formatted(link));
            assertFalse(sequence.hasConditions());
        }
    }

    public void testInStageConditionAllowsEarlierPrimaryByNumberOrName() {
        for (String member : List.of("1", "issued")) {
            Sequence sequence = sequence("""
                SEQUENCE WITHIN 30s PRIMARY issued
                (FROM a) AS issued, (FROM b | WHERE label != MEMBER(%s,label)) AS signed
                """.formatted(member));
            SequenceCondition condition = sequence.collect(SequenceCondition.class).getFirst();
            assertEquals(1, condition.ownStage());
            assertEquals(List.of(0), condition.referencedStages());
            assertEquals(1, condition.condition().collect(SequenceMatchOutput.MemberReference.class).size());
        }
    }

    public void testEarlierPrimaryConditionsAllowTrailingProjections() {
        for (String stage : List.of(
            "FROM b | WHERE label != MEMBER(issued,label)",
            "FROM b | EVAL own=label | WHERE own != MEMBER(issued,label) | KEEP label | RENAME label AS renamed | DROP renamed"
        )) {
            LogicalPlan plan = parser.createStatement("""
                SEQUENCE WITHIN 30s PRIMARY issued
                (FROM a) AS issued, (%s) AS signed
                """.formatted(stage)).plan();
            Sequence sequence = plan.collect(Sequence.class).getFirst();
            assertEquals(0, plan.collect(SequenceMatchOutput.Scope.class).getFirst().primary());
            SequenceCondition condition = sequence.collect(SequenceCondition.class).getFirst();
            assertEquals(1, condition.ownStage());
            assertEquals(List.of(0), condition.referencedStages());
        }
    }

    public void testOriginalConjunctsStaySeparateAndMixedOrStaysDeferred() {
        Sequence sequence = sequence("""
            SEQUENCE WITHIN 30s PRIMARY 3
            (FROM a) AS start_event, (FROM b) AS middle_event,
            (FROM c | WHERE label != "skip" AND label == MEMBER(start_event,label) AND label != MEMBER(middle_event,label))
            """);
        assertEquals(1, sequence.collect(Filter.class).size());
        List<SequenceCondition> conditions = sequence.collect(SequenceCondition.class);
        assertEquals(2, conditions.size());
        assertEquals(List.of(0), conditions.get(0).referencedStages());
        assertEquals(List.of(1), conditions.get(1).referencedStages());
        Sequence mixed = sequence("""
            SEQUENCE WITHIN 30s PRIMARY 2
            (FROM a),(FROM b | WHERE label == "own" OR label == MEMBER(1,label))
            """);
        assertTrue(mixed.collect(Filter.class).isEmpty());
        assertEquals(1, mixed.collect(SequenceCondition.class).size());
    }

    public void testOnlyProjectionCommandsMayFollowTheMemberWhere() {
        sequence("""
            SEQUENCE WITHIN 30s PRIMARY 2
            (FROM a),(FROM b | WHERE label == MEMBER(1,label) | KEEP label | RENAME label AS renamed | DROP renamed)
            """);
        for (String after : List.of("EVAL x = 1", "MV_EXPAND label", "DISSECT label \"%{x}\"", "GROK label \"%{WORD:x}\"")) {
            error(
                "SEQUENCE WITHIN 30s PRIMARY 2 (FROM a),(FROM b | WHERE label == MEMBER(1,label) | " + after + ")",
                "only KEEP, DROP or RENAME may follow"
            );
        }
        error(
            "SEQUENCE WITHIN 30s PRIMARY 2 (FROM a),(FROM b | WHERE label == MEMBER(1,label) | KEEP label | WHERE true)",
            "only KEEP, DROP or RENAME may follow"
        );
        error("SEQUENCE WITHIN 30s PRIMARY 2 (FROM a),(FROM b | EVAL x = MEMBER(1,label))", "final WHERE");
    }

    public void testMemberMustReadAnEarlierStageAndSubqueriesStayInOwnFilter() {
        for (String member : List.of("2", "later")) {
            error(
                "SEQUENCE WITHIN 30s PRIMARY 2 (FROM a),(FROM b | WHERE label == MEMBER(" + member + ",label)) AS later",
                "must refer to an earlier stage"
            );
        }
        error("SEQUENCE WITHIN 30s PRIMARY 2 (FROM a | WHERE label == MEMBER(2,label)),(FROM b)", "earlier stage");
        error("SEQUENCE WITHIN 30s PRIMARY 2 (FROM a | WHERE label == MEMBER(1,label)),(FROM b)", "earlier stage");
        error("""
            SEQUENCE WITHIN 30s PRIMARY 2 (FROM a),
            (FROM b | WHERE label == MEMBER(1,label) OR label IN (FROM c | KEEP label))
            """, "IN subqueries are not supported in a SEQUENCE MEMBER condition");
        sequence("""
            SEQUENCE WITHIN 30s PRIMARY 2 (FROM a),
            (FROM b | WHERE label IN (FROM c | KEEP label) AND label == MEMBER(1,label))
            """);
    }

    public void testNegationRemainsOneDeferredConjunctAndConsecutiveWheresMerge() {
        SequenceCondition condition = sequence("""
            SEQUENCE WITHIN 30s PRIMARY 2 (FROM a),(FROM b | WHERE NOT (label == MEMBER(1,label)))
            """).collect(SequenceCondition.class).getFirst();
        assertEquals(List.of(0), condition.referencedStages());
        assertEquals(1, condition.condition().collect(SequenceMatchOutput.MemberReference.class).size());
        Sequence merged = sequence("""
            SEQUENCE WITHIN 30s PRIMARY 2 (FROM a),
            (FROM b | WHERE label != "skip" | WHERE label == MEMBER(1,label) | WHERE label != MEMBER(1,label) | KEEP label)
            """);
        assertEquals(2, merged.collect(SequenceCondition.class).size());
        assertEquals(1, merged.collect(Filter.class).size());
    }

    private Sequence sequence(String query) {
        LogicalPlan plan = parser.createStatement(query).plan();
        return plan.collect(Sequence.class).getFirst();
    }

    private void error(String query, String expected) {
        assertThat(expectThrows(ParsingException.class, () -> parser.createStatement(query)).getMessage(), containsString(expected));
    }
}
