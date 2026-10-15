/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.session;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.parser.EsqlConfig;
import org.elasticsearch.xpack.esql.parser.EsqlParser;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedRelation;

import java.time.Instant;
import java.util.List;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_FUNCTION_REGISTRY;

/** Fixed request clocks keep analysis, alias enumeration and PIT setup on the same date-based source names. */
public class SequenceSourceExpressionsTests extends ESTestCase {
    private final EsqlParser parser = new EsqlParser(new EsqlConfig(true, TEST_FUNCTION_REGISTRY));
    private static final long STARTED = Instant.parse("2026-09-25T23:59:59.999Z").toEpochMilli();

    public void testSourceNamesResolveBeforeAnalysisAgainstFixedClock() {
        LogicalPlan original = parse("""
            sequence by key with {"maxspan":"30s"}
            (from <events-{now/d}> metadata _id, _index),
            (from <events-{now-1d/d}>) | keep key, stage1._id
            """);
        LogicalPlan resolved = SequenceSourceExpressions.resolve(original, STARTED);
        assertEquals(List.of("events-2026.09.25", "events-2026.09.24"), names(resolved));
        assertEquals(List.of("events-2026.09.26", "events-2026.09.25"), names(SequenceSourceExpressions.resolve(original, STARTED + 1)));
        var before = original.collect(UnresolvedRelation.class).getFirst();
        var after = resolved.collect(UnresolvedRelation.class).getFirst();
        assertEquals(before.source(), after.source());
        assertEquals(before.metadataFields(), after.metadataFields());
        assertEquals(before.telemetryLabel(), after.telemetryLabel());
        assertEquals(before.unresolvedMessage(), after.unresolvedMessage());
        assertSame(resolved, SequenceSourceExpressions.resolve(resolved, STARTED + 1));
    }

    public void testExclusionsSelectorsTimezoneAndAliasExpressionsSurvive() {
        LogicalPlan resolved = SequenceSourceExpressions.resolve(parse("""
            sequence with {"maxspan":"30s"}
            (from events-*, -<events-{now-1d/d}>, routed-alias),
            (from "<events-{now/d{yyyy.MM.dd|+02:00}}>::data")
            """), STARTED);
        assertEquals(List.of("events-*,-events-2026.09.24,routed-alias", "events-2026.09.26::data"), names(resolved));
    }

    public void testOrdinarySiblingSourcesRemainUntouched() {
        LogicalPlan original = parse("""
            from (sequence with {"maxspan":"30s"} (from <a-{now/d}>), (from <b-{now/d}>)),
            (from <ordinary-{now/d}>)
            """);
        assertEquals(
            List.of("a-2026.09.25", "b-2026.09.25", "<ordinary-{now/d}>"),
            names(SequenceSourceExpressions.resolve(original, STARTED))
        );
        LogicalPlan ordinary = parse("from <ordinary-{now/d}>");
        assertSame(ordinary, SequenceSourceExpressions.resolve(ordinary, STARTED));
    }

    private LogicalPlan parse(String query) {
        return parser.createStatement(query).plan();
    }

    private static List<String> names(LogicalPlan plan) {
        return plan.collect(UnresolvedRelation.class).stream().map(relation -> relation.indexPattern().indexPattern()).toList();
    }
}
