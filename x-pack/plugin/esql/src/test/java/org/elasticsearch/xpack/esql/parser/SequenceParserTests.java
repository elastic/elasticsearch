/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.parser;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.expression.function.UnresolvedFunction;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.Sequence;
import org.elasticsearch.xpack.esql.plan.logical.SequenceMatchOutput;
import org.elasticsearch.xpack.esql.plan.logical.SequenceStage;
import org.elasticsearch.xpack.esql.plan.logical.UriParts;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_FUNCTION_REGISTRY;
import static org.hamcrest.Matchers.containsString;

/** Exercises command-mode transitions as well as the user-visible sequence contract. */
public class SequenceParserTests extends ESTestCase {
    private final EsqlParser parser = new EsqlParser(new EsqlConfig(true, TEST_FUNCTION_REGISTRY));

    public void testStageScopesAndDownstreamPipeline() {
        Limit limit = (Limit) parser.createStatement("""
            SeQuEnCe BY host.id, user.name WITHIN 30s PRIMARY 2
            (
              FROM network-* METADATA _id, _index
              | WHERE destination.port IN (5985, 5986) AND (source.ip IS NULL OR source.ip != "127.0.0.1")
              | EVAL key = TO_LOWER(host.id)
            ),
            (
              FROM process-*
              | WHERE TO_LOWER(process.parent.name) == "wsmprovhost.exe"
              | EVAL key = CASE(host.id IS NULL, "", host.id)
            )
            | WHERE @timestamp >= MEMBER(1, @timestamp)
            | SORT @timestamp DESC
            | LIMIT 5
            """).plan();
        Sequence sequence = limit.collect(Sequence.class).getFirst();
        assertEquals(30_000_000_000L, sequence.maxSpanNanos());
        assertEquals(2, sequence.children().size());
        for (var child : sequence.children()) {
            assertEquals(List.of("host.id", "user.name"), ((SequenceStage) child).keys().stream().map(a -> a.name()).toList());
        }
    }

    /** Names are command-local labels; the lowered children and primary remain positional. */
    public void testNamedStageScopesAndIdentifierNames() {
        for (String name : List.of("download", "Download", "_download", "@download", "primary")) {
            var plan = parser.createStatement("""
                SeQuEnCe BY host.id WITHIN 30s pRiMaRy execution
                (FROM a | KEEP host.id, label) aS %s,
                (FROM b | KEEP host.id, label) AS execution
                | EVAL evidence = MEMBER(%s, label) | KEEP host.id, evidence
                """.formatted(name, name)).plan();
            Sequence sequence = plan.collect(Sequence.class).getFirst();
            assertEquals(2, sequence.children().size());
            assertEquals(30_000_000_000L, sequence.maxSpanNanos());
            assertEquals(1, plan.collect(SequenceMatchOutput.Scope.class).getFirst().primary());
            assertTrue(plan.collect(SequenceMatchOutput.Declaration.class).isEmpty());
            for (var child : sequence.children()) {
                assertEquals(List.of("host.id"), ((SequenceStage) child).keys().stream().map(a -> a.name()).toList());
            }
        }
    }

    public void testNoKeysAndThreeStages() {
        Sequence sequence = parser.createStatement("""
            sequence WITHIN 1nanos primary 3
            (from a | where true), (from b | keep @timestamp), (from c)
            """).plan().collect(Sequence.class).getFirst();
        assertEquals(1, sequence.maxSpanNanos());
        assertEquals(3, sequence.children().size());
        assertTrue(((SequenceStage) sequence.children().getFirst()).keys().isEmpty());
    }

    public void testQuotedKeysAndComments() {
        Sequence sequence = parser.createStatement("""
            sequence /* header */ by `odd.name`, host.id WITHIN 2m primary 2
            // first
            (from a | keep `odd.name`, host.id),
            /* second */ (from b | mv_expand host.id | keep `odd.name`, host.id)
            """).plan().collect(Sequence.class).getFirst();
        assertEquals(
            List.of("odd.name", "host.id"),
            ((SequenceStage) sequence.children().getFirst()).keys().stream().map(a -> a.name()).toList()
        );
    }

    public void testSequenceDisabledInReleaseParser() {
        EsqlParser release = new EsqlParser(new EsqlConfig(false, TEST_FUNCTION_REGISTRY));
        assertEquals(
            "line 1:1: mismatched input 'sequence' expecting {, 'row', 'from', 'ts', 'promql', 'set', 'show'}",
            expectThrows(ParsingException.class, () -> release.createStatement("sequence WITHIN 30s primary 2 (from a), (from b)"))
                .getMessage()
        );
    }

    /** Release parsing must leave an unregistered function to ordinary analysis without exposing the DEV command. */
    public void testReleaseParserLeavesMemberForOrdinaryFunctionResolution() {
        EsqlParser release = new EsqlParser(new EsqlConfig(false, TEST_FUNCTION_REGISTRY));
        for (String name : List.of("MEMBER", "member")) {
            var plan = release.createStatement("ROW x = " + name + "(1, a)").plan();
            var functions = new ArrayList<UnresolvedFunction>();
            plan.forEachExpressionDown(UnresolvedFunction.class, functions::add);
            assertEquals(1, functions.size());
            assertTrue(name.equalsIgnoreCase(functions.getFirst().name()));
            assertEquals(2, functions.getFirst().children().size());
        }
    }

    /** Explicit DEV parsing must prepare output bindings even when the surrounding build is a release build. */
    public void testExplicitDevParserPreparesSequenceOutput() {
        var plan = parser.createStatement("""
            SEQUENCE BY key WITHIN 30s PRIMARY 2
            (FROM a), (FROM b) | EVAL supporting = MEMBER(1, payload)
            """).plan();
        assertTrue(plan.collect(SequenceMatchOutput.Declaration.class).isEmpty());
        assertEquals(1, plan.collect(SequenceMatchOutput.Scope.class).size());
        var members = new ArrayList<SequenceMatchOutput.MemberReference>();
        plan.forEachExpressionDown(SequenceMatchOutput.MemberReference.class, members::add);
        assertEquals(1, members.size());
    }

    /** DEV parsing retains the authoring diagnostic for MEMBER used without its sequence declaration. */
    public void testDevParserRetainsSequenceMemberDiagnostic() {
        assertThat(
            expectThrows(ParsingException.class, () -> parser.createStatement("ROW x = MEMBER(1, a)")).getMessage(),
            containsString("MEMBER requires SEQUENCE with an explicit PRIMARY")
        );
    }

    public void testDuplicateGlobalKeysAreRejected() {
        error("sequence by host.id, host.id WITHIN 1s primary 2 (from a), (from b)", "Duplicate SEQUENCE BY");
    }

    /** URI syntax must reach pragma-aware analysis instead of being rejected by the stage parser. */
    public void testUriPartsStageSyntaxReachesAnalysis() {
        var plan = parser.createStatement("""
            SEQUENCE BY id WITHIN 30s PRIMARY 1
            (FROM a | URI_PARTS parsed = url),
            (FROM b | EVAL alias = url | URI_PARTS other = alias | DROP alias, url)
            | KEEP id, parsed.domain | LIMIT 10
            """).plan();
        Sequence sequence = plan.collect(Sequence.class).getFirst();
        assertEquals(2, sequence.children().size());
        assertEquals(2, sequence.collect(UriParts.class).size());
        for (var stage : sequence.children()) {
            assertEquals(1, stage.collect(UriParts.class).size());
        }
    }

    public void testStageMustPreserveEvents() {
        error("sequence WITHIN 30s primary 2 (row x = 1), (from b)", "stages must start with FROM");
        error("sequence WITHIN 30s primary 2 (ts a), (from b)", "stages must start with FROM");
        error("sequence WITHIN 30s primary 2 (sequence WITHIN 30s primary 2 (from a), (from b)), (from b)", "stages must start with FROM");
        for (String command : List.of("stats count(*)", "inline stats count(*)", "sort @timestamp", "limit 3", "sample 0.5")) {
            error("sequence WITHIN 30s primary 2 (from a | " + command + "), (from b)", "not supported inside a SEQUENCE stage");
        }
        error("sequence WITHIN 30s primary 1 (from a)", "expecting");
    }

    public void testPredicateMembershipSubquery() {
        Sequence sequence = parser.createStatement("""
            sequence by host.id WITHIN 2m primary 2
            (from files | where file.path in (from targets | stats by process.executable | keep process.executable)),
            (from targets)
            """).plan().collect(Sequence.class).getFirst();
        assertEquals(2, sequence.children().size());
    }

    public void testEqlStageCapacity() {
        String stage = "(from events)";
        Sequence sequence = parser.createStatement(
            "sequence WITHIN 30s primary 1 " + String.join(", ", java.util.Collections.nCopies(Sequence.MAX_STAGES, stage))
        ).plan().collect(Sequence.class).getFirst();
        assertEquals(Sequence.MAX_STAGES, sequence.children().size());
        error(
            "sequence WITHIN 30s primary 1 " + String.join(", ", java.util.Collections.nCopies(Sequence.MAX_STAGES + 1, stage)),
            "cannot contain more than 256 stages"
        );
    }

    private void error(String query, String fragment) {
        assertThat(expectThrows(ParsingException.class, () -> parser.createStatement(query)).getMessage(), containsString(fragment));
    }
}
