/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.parser;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.plan.logical.Sequence;
import org.elasticsearch.xpack.esql.plan.logical.SequenceStage;

import java.util.List;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_FUNCTION_REGISTRY;
import static org.hamcrest.Matchers.containsString;

/** Incoming limits follow each stage's optional name and keys, and keep the overall window independent. */
public class SequenceLinkTimeParserTests extends ESTestCase {
    private final EsqlParser parser = new EsqlParser(new EsqlConfig(true, TEST_FUNCTION_REGISTRY));

    /** Named/local-key stages lower the trailing constraint onto that member only. */
    public void testIncomingLimitAfterNamesAndLocalKeys() {
        Sequence sequence = parse("""
            SEQUENCE BY host.id WITHIN 1 hour PRIMARY conn
            (FROM a) AS proc BY process.entity_id,
            (FROM b) AS conn BY process.parent.entity_id WITHIN 10seconds
            | LIMIT 10
            """);
        assertEquals(3_600_000_000_000L, sequence.maxSpanNanos());
        assertEquals(0L, stage(sequence, 0).maxGapNanos());
        assertEquals(10_000_000_000L, stage(sequence, 1).maxGapNanos());
        assertEquals(List.of("host.id", "process.parent.entity_id"), stage(sequence, 1).keys().stream().map(Attribute::name).toList());
    }

    /** The existing map/numeric form can opt into only a later-stage bound, including a large long nanos quantity. */
    public void testLegacyHeaderAndNumericStagesAcceptIncomingNanos() {
        Sequence sequence = parse("""
            SEQUENCE BY key WITHIN 30s PRIMARY 2
            (FROM a), (FROM b) WITHIN 5000000000 nanos
            | LIMIT 10
            """);
        assertEquals(30_000_000_000L, sequence.maxSpanNanos());
        assertEquals(0L, stage(sequence, 0).maxGapNanos());
        assertEquals(5_000_000_000L, stage(sequence, 1).maxGapNanos());
    }

    /** The first member has no predecessor, and every explicit incoming bound must be a positive fixed duration. */
    public void testFirstMemberAndInvalidIncomingDurationsAreRejected() {
        rejected(
            "SEQUENCE WITHIN 1m PRIMARY 1 (FROM a) WITHIN 1s, (FROM b)",
            "The first SEQUENCE member cannot have an incoming WITHIN limit"
        );
        for (String value : List.of("0s", "-1s", "1.5s")) {
            rejected("SEQUENCE WITHIN 1m PRIMARY 1 (FROM a), (FROM b) WITHIN " + value, "positive integer fixed duration");
        }
        rejected("SEQUENCE WITHIN 1m PRIMARY 1 (FROM a), (FROM b) WITHIN 1 month", "fixed unit from nanoseconds through days");
        rejected("SEQUENCE WITHIN 1m PRIMARY 1 (FROM a), (FROM b) WITHIN 106752 days", "Invalid SEQUENCE WITHIN duration");
        rejected("SEQUENCE WITHIN 1m PRIMARY 1 (FROM a), (FROM b) NEAR 1s", "Expected WITHIN");
    }

    /** The otherwise invalid first-stage limit should name its declared stage, while positional diagnostics stay stable. */
    public void testNamedFirstMemberLimitIdentifiesStage() {
        rejected(
            "SEQUENCE WITHIN 1m PRIMARY execution (FROM a) AS download WITHIN 1s, (FROM b) AS execution",
            "The first SEQUENCE member [download] cannot have an incoming WITHIN limit"
        );
    }

    /** WITHIN remains a contextual identifier even beside the new bound; the suffix order has one unambiguous spelling. */
    public void testWithinIdentifiersAndSuffixOrder() {
        Sequence sequence = parse("""
            SEQUENCE WITHIN 1m PRIMARY within
            (FROM a) AS other BY within,
            (FROM b) AS within BY within WITHIN 1s
            | EVAL selected=MEMBER(other,within) | KEEP within,selected | LIMIT 10
            """);
        assertEquals(1_000_000_000L, stage(sequence, 1).maxGapNanos());
        assertEquals(List.of("within"), stage(sequence, 1).keys().stream().map(Attribute::name).toList());
        for (String invalid : List.of(
            "SEQUENCE WITHIN 1m PRIMARY 1 (FROM a) BY first, (FROM b) BY second AS later",
            "SEQUENCE BY key WITHIN 1m PRIMARY 1 (FROM a), (FROM b) WITHIN 1s AS later",
            "SEQUENCE WITHIN 1m PRIMARY 1 (FROM a) BY first, (FROM b) WITHIN 1s BY second",
            "SEQUENCE BY key WITHIN 1m PRIMARY 1 (FROM a), (FROM b) WITHIN 1s WITHIN 1s"
        )) {
            expectThrows(ParsingException.class, () -> parse(invalid));
        }
    }

    private Sequence parse(String query) {
        return parser.createStatement(query).plan().collect(Sequence.class).getFirst();
    }

    private static SequenceStage stage(Sequence sequence, int position) {
        return (SequenceStage) sequence.children().get(position);
    }

    private void rejected(String query, String expected) {
        assertThat(expectThrows(ParsingException.class, () -> parse(query)).getMessage(), containsString(expected));
    }
}
