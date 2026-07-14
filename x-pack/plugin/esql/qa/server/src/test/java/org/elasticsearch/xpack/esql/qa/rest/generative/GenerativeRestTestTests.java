/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.rest.generative;

import org.elasticsearch.test.ESTestCase;

/**
 * Tests the predicates that classify known generative-test failures as allowed failures.
 */
public class GenerativeRestTestTests extends ESTestCase {

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

    /**
     * A subquery branch that ends with a {@code SORT} (pinned by a following {@code LIMIT}/{@code DEDUP}, which fuse the
     * {@code SORT} into a {@code TopN}/{@code TopNBy} carrying the {@code SORT}'s source text) makes the product reject a
     * full-text function in the outer {@code WHERE} with "cannot be used after SORT". Reproduced by
     * {@code GenerativeIT {feature:SUBQUERIES\}} on seed {@code 6DF16F9F17374414}.
     */
    public void testFullTextAfterSubqueryMatchesSortMessage() {
        String query = "FROM (FROM languages | SORT language_name | LIMIT 5), alerts | WHERE kql(\"language_name: English\")";
        String error = "verification_exception: line 1:36: [KQL] function cannot be used after SORT";

        assertTrue(GenerativeRestTest.isFullTextAfterSubqueryInFromBug(error, query));
    }

    public void testFullTextAfterSortRequiresSubqueryInQuery() {
        String query = "FROM languages | SORT language_name | LIMIT 5 | WHERE kql(\"language_name: English\")";
        String error = "verification_exception: line 1:36: [KQL] function cannot be used after SORT";

        assertFalse(GenerativeRestTest.isFullTextAfterSubqueryInFromBug(error, query));
    }

    public void testFullTextAfterSubqueryRequiresSubqueryInQuery() {
        String query = "FROM logs | LOOKUP JOIN message_types_lookup ON message | WHERE qstr(\"text:hello\")";
        String error = "verification_exception: line 1:34: [QSTR] function cannot be used after LOOKUP";

        assertFalse(GenerativeRestTest.isFullTextAfterSubqueryInFromBug(error, query));
    }

    /**
     * A field created inside a {@code FROM (...)} subquery branch (here via {@code EVAL}) is non-index-mapped, so a
     * {@code MATCH} carrying options against it is rejected with "Options are not supported for [MATCH] function call
     * on non-index-mapped field". Reproduced by {@code GenerativeIT {feature:SUBQUERIES\}} on seed
     * {@code 7033534A36E5A879}.
     */
    public void testMatchOptionsOnNonIndexMappedSubqueryFieldIsTolerated() {
        String query = "FROM books, (FROM books | EVAL title2 = concat(title, \"x\")) "
            + "| WHERE match(title2, \"search\", {\"lenient\": true})";
        String error = "verification_exception: Found 1 problem\n"
            + "line 1:88: Options are not supported for [MATCH] function call on non-index-mapped field [title2]";

        assertTrue(GenerativeRestTest.isMatchOptionsOnNonIndexMappedFieldInSubqueryBug(error, query));
    }

    public void testOptionsForNonMatchFunctionOnNonIndexMappedSubqueryFieldIsNotTolerated() {
        // The rule is pinned to [MATCH]; the same error for other full-text functions must not be tolerated yet.
        String query = "FROM books, (FROM books | EVAL title2 = concat(title, \"x\")) "
            + "| WHERE qstr(\"title2:search\", {\"lenient\": true})";
        String error = "verification_exception: Found 1 problem\n"
            + "line 1:88: Options are not supported for [QSTR] function call on non-index-mapped field [title2]";

        assertFalse(GenerativeRestTest.isMatchOptionsOnNonIndexMappedFieldInSubqueryBug(error, query));
    }
}
