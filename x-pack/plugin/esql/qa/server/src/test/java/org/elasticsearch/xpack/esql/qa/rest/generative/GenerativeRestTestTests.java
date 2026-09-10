/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.rest.generative;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.generator.Column;

import java.util.List;

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

    public void testFullTextAfterSubqueryRequiresSubqueryInQuery() {
        String query = "FROM logs | LOOKUP JOIN message_types_lookup ON message | WHERE qstr(\"text:hello\")";
        String error = "verification_exception: line 1:34: [QSTR] function cannot be used after LOOKUP";

        assertFalse(GenerativeRestTest.isFullTextAfterSubqueryInFromBug(error, query));
    }

    public void testMatchOptionsOnNonIndexMappedFieldIsAllowed() {
        String error = "Options are not supported for [MATCH] function call on non-index-mapped field [message]";
        List<Column> schema = List.of(new Column("message", "keyword", List.of(), false));

        assertTrue(GenerativeRestTest.isFieldFullTextError(error, schema));
    }

    public void testMatchOptionsOnIndexMappedFieldIsNotAllowed() {
        String error = "Options are not supported for [MATCH] function call on non-index-mapped field [message]";
        List<Column> schema = List.of(new Column("message", "keyword", List.of(), true));

        assertFalse(GenerativeRestTest.isFieldFullTextError(error, schema));
    }

    public void testMatchOptionsOnNonIndexMappedNonTextFieldIsAllowed() {
        String error = "Options are not supported for [MATCH] function call on non-index-mapped, non-TEXT field [message]";
        List<Column> schema = List.of(new Column("message", "keyword", List.of(), false));

        assertTrue(GenerativeRestTest.isFieldFullTextError(error, schema));
    }

    public void testMatchPhraseOptionsOnNonIndexMappedFieldIsAllowed() {
        String error = "Options are not supported for [MATCH_PHRASE] function call on non-index-mapped field [message]";
        List<Column> schema = List.of(new Column("message", "keyword", List.of(), false));

        assertTrue(GenerativeRestTest.isFieldFullTextError(error, schema));
    }

    public void testMatchPhraseOptionsOnNonIndexMappedNonTextFieldIsAllowed() {
        String error = "Options are not supported for [MATCH_PHRASE] function call on non-index-mapped, non-TEXT field [message]";
        List<Column> schema = List.of(new Column("message", "keyword", List.of(), false));

        assertTrue(GenerativeRestTest.isFieldFullTextError(error, schema));
    }

    public void testMatchPhraseOptionsOnIndexMappedFieldIsNotAllowed() {
        String error = "Options are not supported for [MATCH_PHRASE] function call on non-index-mapped field [message]";
        List<Column> schema = List.of(new Column("message", "keyword", List.of(), true));

        assertFalse(GenerativeRestTest.isFieldFullTextError(error, schema));
    }

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

    public void testQstrAfterSubqueryRenameIsTolerated() {
        // Real GenerativeIT {feature:SUBQUERIES} failure: the subquery ends with a RENAME, which the flattened plan
        // pushes ahead of the outer QSTR. RENAME is not one of FROM/WHERE/SORT, so QSTR is rejected.
        String query = "from (from employees_gender_text | limit 808 | rename `os.version` AS v) | where qstr(\"os.name:world\")";
        String error = "verification_exception: line 1:481: [QSTR] function cannot be used after RENAME";
        assertTrue(GenerativeRestTest.isFullTextAfterSubqueryInFromBug(error, query));
    }

    public void testKqlAfterSubqueryEvalIsTolerated() {
        String query = "FROM books, (FROM books | EVAL title2 = concat(title, \"x\")) | WHERE kql(\"title2: world\")";
        String error = "verification_exception: line 1:64: [KQL] function cannot be used after EVAL";
        assertTrue(GenerativeRestTest.isFullTextAfterSubqueryInFromBug(error, query));
    }

    public void testQstrAfterRenameRequiresSubqueryInQuery() {
        String query = "FROM employees | RENAME first_name AS fn | WHERE qstr(\"fn:world\")";
        String error = "verification_exception: line 1:44: [QSTR] function cannot be used after RENAME";
        assertFalse(GenerativeRestTest.isFullTextAfterSubqueryInFromBug(error, query));
    }

    public void testMatchAfterSubqueryRenameIsNotToleratedByQstrKqlBranch() {
        // The broadened branch is scoped to QSTR/KQL only: RENAME is legal before MATCH/MatchPhrase, so a MATCH error
        // naming RENAME is not a known bug and must not be tolerated here.
        String query = "FROM books, (FROM books | RENAME title AS title2) | WHERE match(title2, \"world\")";
        String error = "verification_exception: line 1:64: [MATCH] function cannot be used after RENAME";
        assertFalse(GenerativeRestTest.isFullTextAfterSubqueryInFromBug(error, query));
    }
}
