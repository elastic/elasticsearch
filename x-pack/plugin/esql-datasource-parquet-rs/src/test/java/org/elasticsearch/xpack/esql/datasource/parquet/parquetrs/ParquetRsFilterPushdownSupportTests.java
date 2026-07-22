/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet.parquetrs;

import org.elasticsearch.test.ESTestCase;

/**
 * Tests for the pure helpers in {@link ParquetRsFilterPushdownSupport} that don't touch the
 * native bridge.
 */
public class ParquetRsFilterPushdownSupportTests extends ESTestCase {

    // --- esqlWildcardToSqlLike ---

    public void testWildcardMappingStarToPercent() {
        assertEquals("foo%", ParquetRsFilterPushdownSupport.esqlWildcardToSqlLike("foo*"));
        assertEquals("%foo%", ParquetRsFilterPushdownSupport.esqlWildcardToSqlLike("*foo*"));
    }

    public void testWildcardMappingQuestionToUnderscore() {
        assertEquals("f_o", ParquetRsFilterPushdownSupport.esqlWildcardToSqlLike("f?o"));
        assertEquals("___", ParquetRsFilterPushdownSupport.esqlWildcardToSqlLike("???"));
    }

    public void testSqlSpecialCharactersAreEscaped() {
        // % and _ are literal in ESQL but wildcards in SQL LIKE — must be escaped.
        assertEquals("100\\%", ParquetRsFilterPushdownSupport.esqlWildcardToSqlLike("100%"));
        assertEquals("foo\\_bar", ParquetRsFilterPushdownSupport.esqlWildcardToSqlLike("foo_bar"));
    }

    public void testEscapedWildcardsAreLiterals() {
        // \* and \? are literals in ESQL; the loop preserves the backslash and the next char verbatim.
        assertEquals("\\*", ParquetRsFilterPushdownSupport.esqlWildcardToSqlLike("\\*"));
        assertEquals("\\?", ParquetRsFilterPushdownSupport.esqlWildcardToSqlLike("\\?"));
    }

    public void testEmptyAndPlainStringsPassThrough() {
        assertEquals("", ParquetRsFilterPushdownSupport.esqlWildcardToSqlLike(""));
        assertEquals("plain", ParquetRsFilterPushdownSupport.esqlWildcardToSqlLike("plain"));
    }

    public void testTrailingBackslashIsDoubled() {
        // Edge case called out in the method's Javadoc: a pattern ending in a single unmatched '\'
        // is emitted as '\\' so the SQL pattern doesn't end with an incomplete escape sequence.
        assertEquals("foo\\\\", ParquetRsFilterPushdownSupport.esqlWildcardToSqlLike("foo\\"));
        assertEquals("\\\\", ParquetRsFilterPushdownSupport.esqlWildcardToSqlLike("\\"));
    }

    public void testMixedPattern() {
        // ESQL "user_*" : "_" is a literal underscore (escape), "*" is a wildcard (becomes %).
        assertEquals("user\\_%", ParquetRsFilterPushdownSupport.esqlWildcardToSqlLike("user_*"));
    }
}
