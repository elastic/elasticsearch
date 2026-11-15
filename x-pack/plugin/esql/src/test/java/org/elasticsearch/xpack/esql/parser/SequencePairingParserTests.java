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

/** Pins Q31 selection and the narrow deferred-WHERE authoring boundary before analysis or execution. */
public class SequencePairingParserTests extends ESTestCase {
    private final EsqlParser parser = new EsqlParser(new EsqlConfig(true, TEST_FUNCTION_REGISTRY));

    public void testSelectionWithoutAStageCondition() {
        for (String options : List.of("", ",\"pairing\":\"today\"")) {
            for (String link : List.of("", " WITHIN 5s")) {
                Sequence sequence = sequence("""
                    SEQUENCE WITH {"maxspan":"30s"%s} PRIMARY 2 (FROM a), (FROM b)%s
                    | WHERE label != MEMBER(1, label)
                    """.formatted(options, link));
                assertEquals(Sequence.Pairing.TODAY, sequence.pairing());
                assertNull(sequence.maxCandidates());
                assertFalse(sequence.hasConditions());
            }
        }
        Sequence latest = sequence("SEQUENCE WITH {\"maxspan\":\"30s\",\"pairing\":\"latest_valid\"} PRIMARY 2 (FROM a),(FROM b)");
        assertEquals(Sequence.Pairing.LATEST_VALID, latest.pairing());
        assertFalse(latest.hasConditions());
        assertNull(latest.maxCandidates());
    }

    public void testInStageConditionSelectsLatestAndAllowsEarlierPrimary() {
        for (String member : List.of("1", "issued")) {
            for (String option : List.of("", ",\"pairing\":\"latest_valid\"")) {
                Sequence sequence = sequence("""
                    SEQUENCE WITH {"maxspan":"30s"%s} PRIMARY issued
                    (FROM a) AS issued, (FROM b | WHERE label != MEMBER(%s,label)) AS signed
                    """.formatted(option, member));
                assertEquals(Sequence.Pairing.LATEST_VALID, sequence.pairing());
                SequenceCondition condition = sequence.collect(SequenceCondition.class).getFirst();
                assertEquals(1, condition.ownStage());
                assertEquals(List.of(0), condition.referencedStages());
                assertEquals(1, condition.condition().collect(SequenceMatchOutput.MemberReference.class).size());
            }
        }
    }

    public void testRetainedMatchesAcceptEarlierPrimaryConditionsAndTrailingProjections() {
        for (String stage : List.of(
            "FROM b | WHERE label != MEMBER(issued,label)",
            "FROM b | EVAL own=label | WHERE own != MEMBER(issued,label) | KEEP label | RENAME label AS renamed | DROP renamed"
        )) {
            LogicalPlan plan = parser.createStatement("""
                SEQUENCE WITH {"maxspan":"30s"} PRIMARY issued
                (FROM a) AS issued, (%s) AS signed | RETURN MATCHES
                """.formatted(stage)).plan();
            Sequence sequence = plan.collect(Sequence.class).getFirst();
            assertEquals(Sequence.Pairing.LATEST_VALID, sequence.pairing());
            assertEquals(1, plan.collect(SequenceMatchOutput.Capture.class).size());
            assertEquals(2, plan.collect(SequenceMatchOutput.Originals.class).size());
            assertEquals(0, plan.collect(SequenceMatchOutput.Scope.class).getFirst().primary());
            SequenceCondition condition = sequence.collect(SequenceCondition.class).getFirst();
            assertEquals(1, condition.ownStage());
            assertEquals(List.of(0), condition.referencedStages());
        }
    }

    public void testCandidateBoundRequiresEffectiveLatestValid() {
        for (String selection : List.of(",\"pairing\":\"latest_valid\"", "")) {
            String condition = selection.isEmpty() ? " | WHERE label == MEMBER(1,label)" : "";
            Sequence sequence = sequence("""
                SEQUENCE WITH {"maxspan":"30s","max_candidates":16%s} PRIMARY 2 (FROM a),(FROM b%s)
                """.formatted(selection, condition));
            assertEquals(Integer.valueOf(16), sequence.maxCandidates());
        }
        error("SEQUENCE WITH {\"maxspan\":\"30s\",\"max_candidates\":16} PRIMARY 2 (FROM a),(FROM b)", "requires [pairing: latest_valid]");
        error("""
            SEQUENCE WITH {"maxspan":"30s","pairing":"today"} PRIMARY 2
            (FROM a),(FROM b | WHERE label == MEMBER(1,label))
            """, "[pairing: today] cannot evaluate an in-stage MEMBER condition");
        for (String value : List.of("0", "-1", "1.5", "null", "true", "\"16\"", "2147483648")) {
            error(
                "SEQUENCE WITH {\"maxspan\":\"30s\",\"pairing\":\"latest_valid\",\"max_candidates\":"
                    + value
                    + "} PRIMARY 2 (FROM a),(FROM b)",
                "[max_candidates] must be a positive integer"
            );
        }
    }

    public void testOptionDiagnosticsAndMandatoryPrimary() {
        for (String value : List.of("null", "true", "16", "\"other\"", "\"LATEST_VALID\"")) {
            error("SEQUENCE WITH {\"maxspan\":\"30s\",\"pairing\":" + value + "} PRIMARY 2 (FROM a),(FROM b)", "[pairing] must be");
        }
        error(
            "SEQUENCE WITH {\"maxspan\":\"30s\",\"pairing\":\"today\",\"pairing\":\"latest_valid\"} PRIMARY 2 (FROM a),(FROM b)",
            "Duplicated named parameters"
        );
        error("SEQUENCE WITH {\"maxspan\":\"30s\",\"candidate_limit\":16} PRIMARY 2 (FROM a),(FROM b)", "Unknown SEQUENCE option");
        error("SEQUENCE WITH {\"maxspan\":\"30s\",\"pairing\":\"latest_valid\"} (FROM a),(FROM b)", "requires an explicit PRIMARY");
    }

    public void testOriginalConjunctsStaySeparateAndMixedOrStaysDeferred() {
        Sequence sequence = sequence("""
            SEQUENCE WITH {"maxspan":"30s"} PRIMARY 3
            (FROM a) AS start_event, (FROM b) AS middle_event,
            (FROM c | WHERE label != "skip" AND label == MEMBER(start_event,label) AND label != MEMBER(middle_event,label))
            """);
        assertEquals(1, sequence.collect(Filter.class).size());
        List<SequenceCondition> conditions = sequence.collect(SequenceCondition.class);
        assertEquals(2, conditions.size());
        assertEquals(List.of(0), conditions.get(0).referencedStages());
        assertEquals(List.of(1), conditions.get(1).referencedStages());
        Sequence mixed = sequence("""
            SEQUENCE WITH {"maxspan":"30s"} PRIMARY 2
            (FROM a),(FROM b | WHERE label == "own" OR label == MEMBER(1,label))
            """);
        assertTrue(mixed.collect(Filter.class).isEmpty());
        assertEquals(1, mixed.collect(SequenceCondition.class).size());
    }

    public void testOnlyProjectionCommandsMayFollowTheMemberWhere() {
        sequence("""
            SEQUENCE WITH {"maxspan":"30s"} PRIMARY 2
            (FROM a),(FROM b | WHERE label == MEMBER(1,label) | KEEP label | RENAME label AS renamed | DROP renamed)
            """);
        for (String after : List.of("EVAL x = 1", "WHERE true", "MV_EXPAND label", "DISSECT label \"%{x}\"", "GROK label \"%{WORD:x}\"")) {
            error(
                "SEQUENCE WITH {\"maxspan\":\"30s\"} PRIMARY 2 (FROM a),(FROM b | WHERE label == MEMBER(1,label) | " + after + ")",
                "only KEEP, DROP or RENAME may follow"
            );
        }
        error("SEQUENCE WITH {\"maxspan\":\"30s\"} PRIMARY 2 (FROM a),(FROM b | EVAL x = MEMBER(1,label))", "final WHERE");
    }

    public void testMemberMustReadAnEarlierStageAndSubqueriesStayInOwnFilter() {
        for (String member : List.of("2", "later")) {
            error(
                "SEQUENCE WITH {\"maxspan\":\"30s\"} PRIMARY 2 (FROM a),(FROM b | WHERE label == MEMBER(" + member + ",label)) AS later",
                "must refer to an earlier stage"
            );
        }
        error("SEQUENCE WITH {\"maxspan\":\"30s\"} PRIMARY 2 (FROM a | WHERE label == MEMBER(2,label)),(FROM b)", "earlier stage");
        error("""
            SEQUENCE WITH {"maxspan":"30s"} PRIMARY 2 (FROM a),
            (FROM b | WHERE label == MEMBER(1,label) OR label IN (FROM c | KEEP label))
            """, "IN subqueries are not supported in a SEQUENCE MEMBER condition");
        sequence("""
            SEQUENCE WITH {"maxspan":"30s"} PRIMARY 2 (FROM a),
            (FROM b | WHERE label IN (FROM c | KEEP label) AND label == MEMBER(1,label))
            """);
    }

    private Sequence sequence(String query) {
        LogicalPlan plan = parser.createStatement(query).plan();
        return plan.collect(Sequence.class).getFirst();
    }

    private void error(String query, String expected) {
        assertThat(expectThrows(ParsingException.class, () -> parser.createStatement(query)).getMessage(), containsString(expected));
    }
}
