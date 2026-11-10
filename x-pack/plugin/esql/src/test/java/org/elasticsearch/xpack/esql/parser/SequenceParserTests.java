/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.parser;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.Sequence;
import org.elasticsearch.xpack.esql.plan.logical.SequenceStage;

import java.util.List;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_FUNCTION_REGISTRY;
import static org.hamcrest.Matchers.containsString;

/** Exercises command-mode transitions as well as the user-visible sequence contract. */
public class SequenceParserTests extends ESTestCase {
    private final EsqlParser parser = new EsqlParser(new EsqlConfig(true, TEST_FUNCTION_REGISTRY));

    public void testStageScopesAndDownstreamPipeline() {
        Limit limit = (Limit) parser.createStatement("""
            SeQuEnCe BY host.id, user.name WITH {"maxspan": "30s"}
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
            | WHERE stage2.@timestamp >= stage1.@timestamp
            | SORT stage2.@timestamp DESC
            | LIMIT 5
            """).plan();
        Sequence sequence = limit.collect(Sequence.class).getFirst();
        assertEquals(30_000_000_000L, sequence.maxSpanNanos());
        assertEquals(2, sequence.children().size());
        for (var child : sequence.children()) {
            assertEquals(List.of("host.id", "user.name"), ((SequenceStage) child).keys().stream().map(a -> a.name()).toList());
        }
    }

    public void testNoKeysAndThreeStages() {
        Sequence sequence = (Sequence) parser.createStatement("""
            sequence with {"maxspan":"1nanos"}
            (from a | where true), (from b | keep @timestamp), (from c)
            """).plan();
        assertEquals(1, sequence.maxSpanNanos());
        assertEquals(3, sequence.children().size());
        assertTrue(((SequenceStage) sequence.children().getFirst()).keys().isEmpty());
    }

    public void testQuotedKeysAndComments() {
        Sequence sequence = (Sequence) parser.createStatement("""
            sequence /* header */ by `odd.name`, host.id with {"maxspan":"2m"}
            // first
            (from a | keep `odd.name`, host.id),
            /* second */ (from b | mv_expand host.id | keep `odd.name`, host.id)
            """).plan();
        assertEquals(
            List.of("odd.name", "host.id"),
            ((SequenceStage) sequence.children().getFirst()).keys().stream().map(a -> a.name()).toList()
        );
    }

    public void testSequenceDisabledInReleaseParser() {
        EsqlParser release = new EsqlParser(new EsqlConfig(false, TEST_FUNCTION_REGISTRY));
        assertThat(
            expectThrows(ParsingException.class, () -> release.createStatement("sequence with {\"maxspan\":\"30s\"} (from a), (from b)"))
                .getMessage(),
            containsString("sequence")
        );
    }

    public void testInvalidOptions() {
        error("sequence with {} (from a), (from b)", "requires a [maxspan]");
        error("sequence with {\"maxspan\":\"1s\", \"maxspan\":\"2s\"} (from a), (from b)", "Duplicated named parameters");
        error("sequence with {\"other\":\"1s\"} (from a), (from b)", "Unknown SEQUENCE option");
        for (String value : List.of("null", "false", "30", "\"0s\"", "\"-1s\"", "\"many\"", "\"9223372036854775807d\"", "{}")) {
            error("sequence with {\"maxspan\":" + value + "} (from a), (from b)", "SEQUENCE");
        }
        error("sequence by host.id, host.id with {\"maxspan\":\"1s\"} (from a), (from b)", "Duplicate SEQUENCE BY");
    }

    public void testStageMustPreserveEvents() {
        error("sequence with {\"maxspan\":\"30s\"} (row x = 1), (from b)", "stages must start with FROM");
        error("sequence with {\"maxspan\":\"30s\"} (ts a), (from b)", "stages must start with FROM");
        for (String command : List.of("stats count(*)", "inline stats count(*)", "sort @timestamp", "limit 3", "sample 0.5")) {
            error("sequence with {\"maxspan\":\"30s\"} (from a | " + command + "), (from b)", "not supported inside a SEQUENCE stage");
        }
        error("sequence with {\"maxspan\":\"30s\"} (from a)", "expecting");
    }

    public void testPredicateMembershipSubquery() {
        Sequence sequence = (Sequence) parser.createStatement("""
            sequence by host.id with {"maxspan":"2m"}
            (from files | where file.path in (from targets | stats by process.executable | keep process.executable)),
            (from targets)
            """).plan();
        assertEquals(2, sequence.children().size());
    }

    public void testSequenceInFromSubquery() {
        for (String pipeline : List.of("", " | keep host.id")) {
            var plan = parser.createStatement("""
                from (
                    sequence by host.id with {"maxspan":"30s"} (from a), (from b)
                """ + pipeline + "), (from targets) | limit 5").plan();
            assertEquals(1, plan.collect(Sequence.class).size());
        }
        error(
            "sequence with {\"maxspan\":\"30s\"} (sequence with {\"maxspan\":\"30s\"} (from a), (from b)), (from b)",
            "stages must start with FROM"
        );
    }

    public void testEqlStageCapacity() {
        String stage = "(from events)";
        Sequence sequence = (Sequence) parser.createStatement(
            "sequence with {\"maxspan\":\"30s\"} " + String.join(", ", java.util.Collections.nCopies(Sequence.MAX_STAGES, stage))
        ).plan();
        assertEquals(Sequence.MAX_STAGES, sequence.children().size());
        error(
            "sequence with {\"maxspan\":\"30s\"} " + String.join(", ", java.util.Collections.nCopies(Sequence.MAX_STAGES + 1, stage)),
            "cannot contain more than 256 stages"
        );
    }

    private void error(String query, String fragment) {
        assertThat(expectThrows(ParsingException.class, () -> parser.createStatement(query)).getMessage(), containsString(fragment));
    }
}
