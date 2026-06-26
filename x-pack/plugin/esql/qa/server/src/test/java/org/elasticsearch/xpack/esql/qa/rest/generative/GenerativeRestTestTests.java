/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.rest.generative;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.generator.Column;
import org.elasticsearch.xpack.esql.generator.command.CommandGenerator;
import org.elasticsearch.xpack.esql.generator.command.pipe.FillNullGenerator;

import java.util.List;
import java.util.Map;

/**
 * Tests the predicates that classify known generative-test failures as allowed failures,
 * and the schema-propagation helpers used by the generative pipeline.
 */
public class GenerativeRestTestTests extends ESTestCase {

    public void testFillNullAllFieldsClearsIndexMappedForEveryColumn() {
        List<Column> previous = List.of(
            new Column("message", "keyword", List.of("keyword"), true),
            new Column("age", "integer", List.of("integer"), true)
        );
        CommandGenerator.CommandDescription command = new CommandGenerator.CommandDescription(
            FillNullGenerator.FILL_NULL,
            FillNullGenerator.INSTANCE,
            " | fillnull with 0",
            Map.of(FillNullGenerator.ALL_FIELDS, Boolean.TRUE)
        );

        List<Column> updated = GenerativeRestTest.updateIndexMapped(previous, previous, command);

        assertFalse(updated.get(0).indexMapped());
        assertFalse(updated.get(1).indexMapped());
    }

    public void testFillNullTargetedClearsOnlyNamedTargets() {
        List<Column> previous = List.of(
            new Column("message", "keyword", List.of("keyword"), true),
            new Column("age", "integer", List.of("integer"), true)
        );
        CommandGenerator.CommandDescription command = new CommandGenerator.CommandDescription(
            FillNullGenerator.FILL_NULL,
            FillNullGenerator.INSTANCE,
            " | fillnull message",
            Map.of(FillNullGenerator.FILLED_FIELDS, List.of("message"))
        );

        List<Column> updated = GenerativeRestTest.updateIndexMapped(previous, previous, command);

        assertFalse(updated.get(0).indexMapped());
        assertTrue(updated.get(1).indexMapped());
    }

    public void testLimitByMvExpandBugMatchesDedup() {
        String query = "ROW x = 1 | MV_EXPAND x | DEDUP";
        String error = "illegal_state_exception: Found 1 problem\n"
            + "line 1:27: Plan [LimitBy[1[INTEGER],[x{r}#3594],false]] optimized incorrectly due to missing references [x{r}#3594]";

        assertTrue(GenerativeRestTest.isLimitByMvExpandBug(error, query));
    }

    public void testLimitByMvExpandBugMatchesLimitBy() {
        String query = "ROW x = 1 | MV_EXPAND x | LIMIT 1 BY x";
        String error = "illegal_state_exception: Found 1 problem\n"
            + "line 1:40: Plan [LimitBy[1[INTEGER],[x{r}#3594],false]] optimized incorrectly due to missing references [x{r}#3594]";

        assertTrue(GenerativeRestTest.isLimitByMvExpandBug(error, query));
    }

    public void testLimitByMvExpandBugRequiresMvExpand() {
        String query = "ROW x = 1 | DEDUP";
        String error = "illegal_state_exception: Found 1 problem\n"
            + "line 1:17: Plan [LimitBy[1[INTEGER],[x{r}#3594],false]] optimized incorrectly due to missing references [x{r}#3594]";

        assertFalse(GenerativeRestTest.isLimitByMvExpandBug(error, query));
    }

    public void testFullTextAfterSubqueryMatchesLimitInsideSubquery() {
        String query = "FROM books, (FROM books | LIMIT 1) | WHERE match(title, \"quick\")";
        String error = "verification_exception: line 1:13: [MATCH] function cannot be used after LIMIT";

        assertTrue(GenerativeRestTest.isFullTextAfterSubqueryInFromBug(error, query));
    }

    public void testFullTextAfterSubqueryMatchesMultiSourceSubqueryMessage() {
        String query = "FROM all_types, (FROM colors | MV_EXPAND hex_code) | WHERE match_phrase(hex_code, \"world search\")";
        String error = "verification_exception: line 1:973: [MatchPhrase] function cannot be used after "
            + "all_types,(from colors | mv_expand hex_code)";

        assertTrue(GenerativeRestTest.isFullTextAfterSubqueryInFromBug(error, query));
    }

    public void testFullTextAfterSubqueryMatchesSubqueryFirstMultiSourceMessage() {
        String query = "FROM (FROM message_types | KEEP type | DROP type),no_mapping_sample_data,service_owners "
            + "| WHERE match_phrase(service_id, \"fox world\")";
        String error = "verification_exception: line 1:91: [MatchPhrase] function cannot be used after "
            + "(from message_types | keep type | drop type),no_mapping_sample_data,service_owners";

        assertTrue(GenerativeRestTest.isFullTextAfterSubqueryInFromBug(error, query));
    }

    public void testFullTextAfterSubqueryMatchesTruncatedUnionAllSourceMessage() {
        String query = "from (from all_types_short_as_long | enrich languages_policy on wildcard "
            + "| dissect language_name \"%{a} %{b}\"),countries_bbox,(from dense_vector_arithmetic | keep id) "
            + "| where match_phrase(registered_domain, \"test data\")";
        // The UnionAll source text in the verifier message is truncated to Node.TO_STRING_MAX_WIDTH chars + "...",
        // so it can be cut off mid-branch, before the comma separating the union branches.
        String error = "verification_exception: line 1:1800: [MatchPhrase] function cannot be used after "
            + "(from all_types_short_as_long | enrich languages_policy on wildcard | dissect language_name \"%{HkOuTBPphONE} %...";

        assertTrue(GenerativeRestTest.isFullTextAfterSubqueryInFromBug(error, query));
    }

    public void testFullTextAfterSubqueryRequiresKnownErrorShape() {
        String query = "FROM all_types, (FROM colors | MV_EXPAND hex_code) | WHERE match_phrase(hex_code, \"world search\")";
        String error = "verification_exception: line 1:973: [MatchPhrase] function cannot be used after field "
            + "with details (from an unrelated diagnostic)";

        assertFalse(GenerativeRestTest.isFullTextAfterSubqueryInFromBug(error, query));
    }

    public void testFullTextAfterSubqueryMatchesLookupMessage() {
        String query = "FROM logs, (FROM messages | LOOKUP JOIN message_types_lookup ON message) | WHERE qstr(\"text:hello\")";
        String error = "verification_exception: line 1:34: [QSTR] function cannot be used after LOOKUP";

        assertTrue(GenerativeRestTest.isFullTextAfterSubqueryInFromBug(error, query));
    }

    public void testFullTextAfterSubqueryMatchesDedupMessage() {
        String query = "FROM employees, (FROM employees | DEDUP first_name) | WHERE first_name : \"world\"";
        String error = "verification_exception: line 1:18: [:] operator cannot be used after DEDUP";

        assertTrue(GenerativeRestTest.isFullTextAfterSubqueryInFromBug(error, query));
    }

    public void testFullTextAfterSubqueryRequiresSubqueryInQuery() {
        String query = "FROM logs | LOOKUP JOIN message_types_lookup ON message | WHERE qstr(\"text:hello\")";
        String error = "verification_exception: line 1:34: [QSTR] function cannot be used after LOOKUP";

        assertFalse(GenerativeRestTest.isFullTextAfterSubqueryInFromBug(error, query));
    }

}
