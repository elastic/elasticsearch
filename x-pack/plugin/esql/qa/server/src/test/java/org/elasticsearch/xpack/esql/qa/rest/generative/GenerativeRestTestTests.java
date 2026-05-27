/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.rest.generative;

import org.elasticsearch.test.ESTestCase;

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

    public void testApproximationUnsupportedSubqueryMatchesLimitByBeforeStats() {
        String query = "SET approximation={};FROM (FROM colors | LIMIT 12 BY color | INLINE STATS c = COUNT(*)),logs";
        String error = "verification_exception: line 1:40: approximation not supported: query with [LIMIT 12 BY color] "
            + "before [STATS] cannot be approximated";

        assertTrue(GenerativeRestTest.isApproximationUnsupportedSubqueryBug(error, query));
    }

    public void testApproximationUnsupportedSubqueryRequiresApproximation() {
        String query = "FROM (FROM colors | LIMIT 12 BY color | INLINE STATS c = COUNT(*)),logs";
        String error = "verification_exception: line 1:40: approximation not supported: query with [LIMIT 12 BY color] "
            + "before [STATS] cannot be approximated";

        assertFalse(GenerativeRestTest.isApproximationUnsupportedSubqueryBug(error, query));
    }

    public void testApproximationUnsupportedSubqueryRequiresSubqueryInFrom() {
        String query = "SET approximation={};FROM colors | LIMIT 12 BY color | STATS c = COUNT(*)";
        String error = "verification_exception: line 1:40: approximation not supported: query with [LIMIT 12 BY color] "
            + "before [STATS] cannot be approximated";

        assertFalse(GenerativeRestTest.isApproximationUnsupportedSubqueryBug(error, query));
    }
}
