/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.parser;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Sequence;
import org.elasticsearch.xpack.esql.plan.logical.SequenceStage;

import java.util.List;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_FUNCTION_REGISTRY;
import static org.hamcrest.Matchers.containsString;

/** Pins local key ordering without inventing or overwriting event fields. */
public class SequenceStageKeysParserTests extends ESTestCase {
    private final EsqlParser parser = new EsqlParser(new EsqlConfig(true, TEST_FUNCTION_REGISTRY));

    /** Shared keys precede local keys, whose names remain in their own stage scope. */
    public void testLocalKeysAppendToHeaderWithoutSyntheticEval() {
        Sequence sequence = parse("""
            SEQUENCE BY host.id WITH {"maxspan":"1m"} PRIMARY conn
            (FROM a) AS proc BY process.entity_id,
            (FROM b) AS conn BY process.parent.entity_id
            | LIMIT 10
            """);
        assertEquals(List.of("host.id", "process.entity_id"), keys(sequence, 0));
        assertEquals(List.of("host.id", "process.parent.entity_id"), keys(sequence, 1));
        assertTrue(sequence.collect(Eval.class).isEmpty());
    }

    /** Local tuples can be the entire key and can use multiple ordinary or quoted field identifiers. */
    public void testMultipleLocalKeysWithoutHeaderOrStageNames() {
        Sequence sequence = parse("""
            SEQUENCE WITH {"maxspan":"1m"} PRIMARY 2
            (FROM a) BY process.entity_id, `odd.name`,
            (FROM b) BY process.parent.entity_id, other
            | LIMIT 10
            """);
        assertEquals(List.of("process.entity_id", "odd.name"), keys(sequence, 0));
        assertEquals(List.of("process.parent.entity_id", "other"), keys(sequence, 1));
    }

    /** Every stage supplies the same number of local positions, including a stage that omits its local clause. */
    public void testLocalKeyCountsMustAgree() {
        for (String stages : List.of(
            "(FROM a) BY first, (FROM b)",
            "(FROM a), (FROM b) BY second",
            "(FROM a) BY first, extra, (FROM b) BY second"
        )) {
            rejected("SEQUENCE WITH {\"maxspan\":\"1m\"} PRIMARY 1 " + stages, "same number of local BY columns");
        }
    }

    /** Header/local duplicates are rejected by field name, while different member names remain legitimate. */
    public void testDuplicateNamesAcrossSharedAndLocalKeysAreRejected() {
        rejected(
            "SEQUENCE BY key WITH {\"maxspan\":\"1m\"} PRIMARY 1 (FROM a) BY key, (FROM b) BY other",
            "Duplicate SEQUENCE BY column [key] in member [1]"
        );
        rejected(
            "SEQUENCE WITH {\"maxspan\":\"1m\"} PRIMARY 1 (FROM a) BY key, key, (FROM b) BY other, extra",
            "Duplicate SEQUENCE BY column [key] in member [1]"
        );
        Sequence sequence = parse("SEQUENCE BY key WITH {\"maxspan\":\"1m\"} PRIMARY 1 (FROM a), (FROM b)");
        assertEquals(List.of("key"), keys(sequence, 0));
        assertEquals(List.of("key"), keys(sequence, 1));
    }

    private Sequence parse(String query) {
        return parser.createStatement(query).plan().collect(Sequence.class).getFirst();
    }

    private static List<String> keys(Sequence sequence, int member) {
        return ((SequenceStage) sequence.children().get(member)).keys().stream().map(Attribute::name).toList();
    }

    private void rejected(String query, String expected) {
        assertThat(expectThrows(ParsingException.class, () -> parser.createStatement(query)).getMessage(), containsString(expected));
    }
}
