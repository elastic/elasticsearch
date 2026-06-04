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

    public void testWildcardLongRangeTopNConnectionBugMatchesFromStar() {
        String query = "SET unmapped_fields=\"nullify\";FROM * | CHANGE_POINT sv ON event_dates AS type, pvalue";
        String error = "Connection is closed";

        assertTrue(GenerativeRestTest.isWildcardLongRangeTopNConnectionBug(error, query));
    }

    public void testWildcardLongRangeTopNConnectionBugMatchesWildcardPattern() {
        String query = "FROM mv_* | SORT decade";
        String error = "Connection reset";

        assertTrue(GenerativeRestTest.isWildcardLongRangeTopNConnectionBug(error, query));
    }

    public void testWildcardLongRangeTopNConnectionBugMatchesConnectionRefused() {
        String query = "FROM flattened_many,languages_mi* | SORT decade";
        String error = "Connection refused";

        assertTrue(GenerativeRestTest.isWildcardLongRangeTopNConnectionBug(error, query));
    }

    public void testWildcardLongRangeTopNConnectionBugMatchesPartialNodeDisconnect() {
        String query = "FROM *,dense_vector,sample__data_ts_nanos_lookup | SORT decade";
        String error = "unexpected partial results: _clusters={details={(local)={status=partial, failures=[{reason={"
            + "type=node_disconnected_exception, reason=[test-cluster-1][internal:data/read/esql/exchange] disconnected"
            + "}}]}}}";

        assertTrue(GenerativeRestTest.isWildcardLongRangeTopNConnectionBug(error, query));
    }

    public void testWildcardLongRangeTopNConnectionBugMatchesPreviousQuery() {
        String previousQuery = "FROM m*,books | CHANGE_POINT sv_and_one_mv ON @timestamp AS type, pvalue BY lk";
        String query = "ROW x = 1";
        String error = "Connection refused: getsockopt";

        assertTrue(GenerativeRestTest.isWildcardLongRangeTopNConnectionBug(error, query, previousQuery));
    }

    public void testWildcardLongRangeTopNConnectionBugMatchesEarlierWildcardQuery() {
        String previousWildcardQuery = "FROM *,dense_vector,sample__data_ts_nanos_lookup | SORT decade";
        String previousQuery = "ROW y = 1";
        String query = "ROW x = 1";
        String error = "Connection refused: getsockopt";

        assertTrue(GenerativeRestTest.isWildcardLongRangeTopNConnectionBug(error, query, previousQuery, previousWildcardQuery));
    }

    public void testWildcardLongRangeTopNConnectionBugMatchesEarlierRangeTopNQuery() {
        String previousLongRangeTopNQuery = "FROM mv_decades | SORT decade";
        String previousWildcardQuery = null;
        String previousQuery = "ROW y = 1";
        String query = "FROM decades,languages_lookup_non_unique_key";
        String error = "Connection refused: getsockopt";

        assertTrue(
            GenerativeRestTest.isWildcardLongRangeTopNConnectionBug(
                error,
                query,
                previousQuery,
                previousWildcardQuery,
                previousLongRangeTopNQuery
            )
        );
    }

    public void testWildcardLongRangeTopNConnectionBugRequiresWildcardCurrentOrPreviousQuery() {
        String previousQuery = "FROM books | CHANGE_POINT sv_and_one_mv ON @timestamp AS type, pvalue BY lk";
        String query = "ROW x = 1";
        String error = "Connection refused: getsockopt";

        assertFalse(GenerativeRestTest.isWildcardLongRangeTopNConnectionBug(error, query, previousQuery));
    }

    public void testWildcardLongRangeTopNConnectionBugRequiresWildcardEarlierQuery() {
        String previousWildcardQuery = "FROM books | SORT decade";
        String previousQuery = "ROW y = 1";
        String query = "ROW x = 1";
        String error = "Connection refused: getsockopt";

        assertFalse(GenerativeRestTest.isWildcardLongRangeTopNConnectionBug(error, query, previousQuery, previousWildcardQuery));
    }

    public void testWildcardLongRangeTopNConnectionBugRequiresRangeSourceForEarlierQuery() {
        String previousLongRangeTopNQuery = "FROM books | SORT title";
        String previousWildcardQuery = null;
        String previousQuery = "ROW y = 1";
        String query = "ROW x = 1";
        String error = "Connection refused: getsockopt";

        assertFalse(
            GenerativeRestTest.isWildcardLongRangeTopNConnectionBug(
                error,
                query,
                previousQuery,
                previousWildcardQuery,
                previousLongRangeTopNQuery
            )
        );
    }

    public void testWildcardLongRangeTopNConnectionBugRequiresWildcardSource() {
        String query = "FROM mv_decades | SORT decade";
        String error = "Connection is closed";

        assertFalse(GenerativeRestTest.isWildcardLongRangeTopNConnectionBug(error, query));
    }

    public void testWildcardLongRangeTopNConnectionBugRequiresConnectionError() {
        String query = "FROM mv_* | SORT decade";
        String error = "verification_exception: line 1:1: unknown column [decade]";

        assertFalse(GenerativeRestTest.isWildcardLongRangeTopNConnectionBug(error, query));
    }

    public void testWildcardLongRangeTopNConnectionBugRequiresNodeDisconnectForPartialResults() {
        String query = "FROM mv_* | SORT decade";
        String error = "unexpected partial results: _clusters={details={(local)={status=partial, failures=[]}}}";

        assertFalse(GenerativeRestTest.isWildcardLongRangeTopNConnectionBug(error, query));
    }

    public void testLongRangeTopNNodeCrashCandidateMatchesWildcardSort() {
        String query = "FROM *,dense_vector,sample__data_ts_nanos_lookup | SORT decade";

        assertTrue(GenerativeRestTest.isLongRangeTopNNodeCrashCandidate(query));
    }

    public void testLongRangeTopNNodeCrashCandidateMatchesRangeSourceSort() {
        String query = "FROM mv_decades | SORT decade";

        assertTrue(GenerativeRestTest.isLongRangeTopNNodeCrashCandidate(query));
    }

    public void testLongRangeTopNNodeCrashCandidateMatchesRangeSourceDefaultLimit() {
        String query = "FROM decades";

        assertTrue(GenerativeRestTest.isLongRangeTopNNodeCrashCandidate(query));
    }

    public void testLongRangeTopNNodeCrashCandidateRequiresRangeSource() {
        String query = "FROM books | SORT title";

        assertFalse(GenerativeRestTest.isLongRangeTopNNodeCrashCandidate(query));
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

    public void testFullTextAfterSubqueryRequiresSubqueryInQuery() {
        String query = "FROM logs | LOOKUP JOIN message_types_lookup ON message | WHERE qstr(\"text:hello\")";
        String error = "verification_exception: line 1:34: [QSTR] function cannot be used after LOOKUP";

        assertFalse(GenerativeRestTest.isFullTextAfterSubqueryInFromBug(error, query));
    }

}
